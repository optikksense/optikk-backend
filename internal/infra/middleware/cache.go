package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
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

func CacheMiddleware(store ResponseCacheStore, ttl time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		if store == nil || c.Request.Method != http.MethodGet {
			c.Next()
			return
		}

		key := cacheKey(c)
		if cached, err := store.Get(c.Request.Context(), key); err == nil && cached != nil {
			contentType := cached.ContentType
			if contentType == "" {
				contentType = "application/json; charset=utf-8"
			}
			c.Header("Content-Type", contentType)
			c.Data(cached.Status, contentType, cached.Body)
			c.Abort()
			return
		}

		writer := &cacheResponseWriter{
			ResponseWriter: c.Writer,
			body:           bytes.NewBuffer(nil),
		}
		c.Writer = writer

		c.Next()

		if c.Writer.Status() != http.StatusOK {
			return
		}
		if writer.body.Len() == 0 {
			return
		}
		if c.Writer.Header().Get("Set-Cookie") != "" {
			return
		}

		contentType := c.Writer.Header().Get("Content-Type")
		if contentType == "" || !strings.Contains(strings.ToLower(contentType), "application/json") {
			return
		}

		response := &cachedResponse{
			Status:      c.Writer.Status(),
			ContentType: contentType,
			Body:        append([]byte(nil), writer.body.Bytes()...),
		}
		if err := store.Set(c.Request.Context(), key, response, ttl); err != nil {
			slog.Debug("cache: store response failed", slog.Any("error", err))
		}
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
		c.Request.URL.RawQuery,
		c.GetHeader("Accept"),
		c.GetHeader("X-Team-Id"),
		strings.TrimSpace(tenant.UserRole),
		strconvInt64(tenant.TeamID),
		strconvInt64(tenant.UserID),
	}, "|")

	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func strconvInt64(v int64) string {
	return strconv.FormatInt(v, 10)
}
