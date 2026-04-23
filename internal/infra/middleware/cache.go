package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

const DefaultResponseCacheTTL = 30 * time.Second

type cachedResponse struct {
	Status      int             `json:"status"`
	ContentType string          `json:"content_type"`
	Body        json.RawMessage `json:"body"`
}

type ResponseCacheStore interface {
	Get(ctx context.Context, key string) (*cachedResponse, error)
	Set(ctx context.Context, key string, response *cachedResponse, ttl time.Duration) error
}

type RedisResponseCache struct {
	client *goredis.Client
	prefix string
}

func NewRedisResponseCache(client *goredis.Client) *RedisResponseCache {
	if client == nil {
		return nil
	}
	return &RedisResponseCache{
		client: client,
		prefix: "httpcache:",
	}
}

func (c *RedisResponseCache) Get(ctx context.Context, key string) (*cachedResponse, error) {
	raw, err := c.client.Get(ctx, c.prefix+key).Bytes()
	if err != nil {
		if err == goredis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var resp cachedResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *RedisResponseCache) Set(ctx context.Context, key string, response *cachedResponse, ttl time.Duration) error {
	if response == nil {
		return nil
	}
	payload, err := json.Marshal(response)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, c.prefix+key, payload, ttl).Err()
}

// cacheMissGroup deduplicates concurrent cache-miss handler runs across
// goroutines by the same cache key, so thundering herds after TTL expiry
// collapse to a single backend call.
var cacheMissGroup singleflight.Group

func writeCached(c *gin.Context, cached *cachedResponse) {
	contentType := cached.ContentType
	if contentType == "" {
		contentType = "application/json; charset=utf-8"
	}
	c.Header("Content-Type", contentType)
	c.Data(cached.Status, contentType, cached.Body)
	c.Abort()
}

func cacheable(method string) bool {
	return method == http.MethodGet || method == http.MethodPost
}

func CacheMiddleware(store ResponseCacheStore, ttl time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		if store == nil || !cacheable(c.Request.Method) {
			c.Next()
			return
		}

		key := cacheKey(c)
		ctx := c.Request.Context()

		if cached, err := store.Get(ctx, key); err == nil && cached != nil {
			writeCached(c, cached)
			return
		}

		// Cache miss: dedupe concurrent handler runs for the same key.
		isLeader := false
		result, _, _ := cacheMissGroup.Do(key, func() (any, error) {
			isLeader = true
			// Re-check the cache inside the leader — a sibling that arrived
			// between our miss and our Do() call may have populated it.
			if cached, gerr := store.Get(ctx, key); gerr == nil && cached != nil {
				return cached, nil
			}

			writer := &cacheResponseWriter{
				ResponseWriter: c.Writer,
				body:           bytes.NewBuffer(nil),
			}
			c.Writer = writer

			c.Next()

			if c.Writer.Status() != http.StatusOK || writer.body.Len() == 0 {
				return nil, nil
			}
			if c.Writer.Header().Get("Set-Cookie") != "" {
				return nil, nil
			}

			contentType := c.Writer.Header().Get("Content-Type")
			if contentType == "" || !strings.Contains(strings.ToLower(contentType), "application/json") {
				return nil, nil
			}

			response := &cachedResponse{
				Status:      c.Writer.Status(),
				ContentType: contentType,
				Body:        append([]byte(nil), writer.body.Bytes()...),
			}
			if serr := store.Set(ctx, key, response, ttl); serr != nil {
				slog.Debug("cache: store response failed", slog.Any("error", serr))
			}
			return response, nil
		})

		if isLeader {
			// Leader's response already written via the capture writer.
			return
		}

		// Follower: replay leader's cached response, or run our own handler
		// if the leader produced nothing cacheable.
		if cached, ok := result.(*cachedResponse); ok && cached != nil {
			writeCached(c, cached)
			return
		}
		c.Next()
	}
}

type cacheResponseWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (w *cacheResponseWriter) Write(data []byte) (int, error) {
	if len(data) > 0 {
		_, _ = w.body.Write(data) //nolint:errcheck // bytes.Buffer.Write never returns an error
	}
	return w.ResponseWriter.Write(data)
}

func (w *cacheResponseWriter) WriteString(s string) (int, error) {
	if s != "" {
		_, _ = w.body.WriteString(s) //nolint:errcheck // bytes.Buffer.WriteString never returns an error
	}
	return w.ResponseWriter.WriteString(s)
}

func cacheKey(c *gin.Context) string {
	tenant := GetTenant(c)
	raw := strings.Join([]string{
		c.Request.Method,
		c.Request.URL.Path,
		normalizeTimeParams(c.Request.URL.RawQuery),
		c.GetHeader("Accept"),
		c.GetHeader("X-Team-Id"),
		strings.TrimSpace(tenant.UserRole),
		strconvInt64(tenant.TeamID),
		strconvInt64(tenant.UserID),
		postBodyHash(c),
	}, "|")

	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// postBodyHash reads the POST body, computes its SHA256, re-attaches the body
// so downstream handlers can still read it, and returns the hex digest.
// Returns "" for non-POST requests.
func postBodyHash(c *gin.Context) string {
	if c.Request.Method != http.MethodPost || c.Request.Body == nil {
		return ""
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return ""
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(body))
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// cacheKeyTimeBucketMs is the bucket width applied to epoch-ms time params in
// the cache key. LIVE mode (5s refresh) would otherwise shift `to=<now>` on
// every tick, giving a unique key per request and bypassing the 30s TTL. 60s
// aligns with the coarsest-tier rollup step (1m) used for ≤3h windows, so
// rounding introduces no accuracy loss for the queries served from cache.
const cacheKeyTimeBucketMs int64 = 60_000

// timeParamNames are query-string keys that carry epoch-ms time bounds. They
// get rounded to cacheKeyTimeBucketMs before hashing the cache key so adjacent
// LIVE ticks collapse to the same key.
var timeParamNames = map[string]struct{}{
	"from":    {},
	"to":      {},
	"startMs": {},
	"endMs":   {},
	"start":   {},
	"end":     {},
}

// normalizeTimeParams parses rawQuery and rewrites any epoch-ms values under
// timeParamNames to their floor-rounded cacheKeyTimeBucketMs bucket. Non-time
// params are left exactly as-is (order preserved) so unrelated query params
// still differentiate the cache key.
func normalizeTimeParams(rawQuery string) string {
	if rawQuery == "" {
		return rawQuery
	}
	pairs := strings.Split(rawQuery, "&")
	for i, pair := range pairs {
		eq := strings.IndexByte(pair, '=')
		if eq <= 0 {
			continue
		}
		name := pair[:eq]
		if _, ok := timeParamNames[name]; !ok {
			continue
		}
		value := pair[eq+1:]
		ms, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			// Non-numeric (e.g. "now-30m") — leave untouched.
			continue
		}
		bucketed := ms - (ms % cacheKeyTimeBucketMs)
		pairs[i] = name + "=" + strconv.FormatInt(bucketed, 10)
	}
	return strings.Join(pairs, "&")
}

func strconvInt64(v int64) string {
	return strconv.FormatInt(v, 10)
}
