package cache

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func CacheResponse(qc *QueryCache, ttl time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !qc.Enabled() || c.Request.Method != http.MethodGet {
			c.Next()
			return
		}

		key := cacheKey(c)

		if val, ok := qc.Get(c.Request.Context(), key); ok {
			c.Data(http.StatusOK, "application/json; charset=utf-8", []byte(val))
			c.Abort()
			return
		}

		// Capture the response body.
		w := &responseCapture{ResponseWriter: c.Writer}
		c.Writer = w

		c.Next()

		if c.Writer.Status() == http.StatusOK && len(w.body) > 0 {
			qc.Set(c.Request.Context(), key, string(w.body), ttl)
		}
	}
}

func cacheKey(c *gin.Context) string {
	// Include teamID from context so different teams don't share cache.
	teamID := c.GetInt64("team_id")
	raw := fmt.Sprintf("qc:%d:%s?%s", teamID, c.Request.URL.Path, c.Request.URL.RawQuery)
	h := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("qc:%x", h[:16])
}

type responseCapture struct {
	gin.ResponseWriter
	body []byte
}

func (w *responseCapture) Write(b []byte) (int, error) {
	w.body = append(w.body, b...)
	return w.ResponseWriter.Write(b)
}
