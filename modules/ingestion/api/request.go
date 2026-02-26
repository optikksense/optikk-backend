package api

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
)

// protoDecoder converts raw protobuf bytes into a typed payload.
type protoDecoder[T any] func([]byte) (T, error)

// decodeRequest handles the common auth → read-body → unmarshal sequence
// shared by all three OTLP ingestion handlers. On failure it writes the
// appropriate JSON error response and returns ok=false.
func decodeRequest[T any](h *Handler, c *gin.Context, protoDec protoDecoder[T]) (string, T, bool) {
	var zero T

	teamUUID, ok := h.resolveAPIKey(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or missing api_key"})
		return "", zero, false
	}

	body, err := readBody(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return "", zero, false
	}

	var payload T
	if isProtobuf(c) {
		payload, err = protoDec(body)
	} else {
		err = json.Unmarshal(body, &payload)
	}
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid OTLP payload"})
		return "", zero, false
	}

	return teamUUID, payload, true
}
