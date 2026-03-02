package telemetry

import (
	"fmt"
	"io"
	"strings"

	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// DecodeProto reads the request body and unmarshals it into a proto message.
// Supports both application/x-protobuf (wire format) and JSON (protojson).
func DecodeProto[T proto.Message](c *gin.Context, newMsg func() T) (T, error) {
	var zero T
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, 100<<20))
	if err != nil {
		return zero, fmt.Errorf("failed to read request body")
	}

	msg := newMsg()
	ct := c.GetHeader("Content-Type")
	if strings.Contains(ct, "application/x-protobuf") || strings.Contains(ct, "application/protobuf") {
		if err := proto.Unmarshal(body, msg); err != nil {
			return zero, fmt.Errorf("invalid protobuf payload: %w", err)
		}
		return msg, nil
	}

	// JSON path — use protojson for spec-compliant OTLP/JSON.
	if err := protojson.Unmarshal(body, msg); err != nil {
		return zero, fmt.Errorf("invalid OTLP JSON payload: %w", err)
	}
	return msg, nil
}
