package decode

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/gin-gonic/gin"
)

// ProtoDecoder converts raw protobuf bytes into a typed payload.
type ProtoDecoder[T any] func([]byte) (T, error)

// DecodePayload reads the request body and unmarshals it as protobuf or JSON.
func DecodePayload[T any](c *gin.Context, protoDec ProtoDecoder[T]) (T, error) {
	var zero T

	body, err := readBody(c)
	if err != nil {
		return zero, fmt.Errorf("failed to read request body")
	}

	if isProtobuf(c) {
		return protoDec(body)
	}

	var payload T
	if err := json.Unmarshal(body, &payload); err != nil {
		return zero, fmt.Errorf("invalid OTLP payload")
	}
	return payload, nil
}

func isProtobuf(c *gin.Context) bool {
	ct := c.GetHeader("Content-Type")
	return strings.Contains(ct, "application/x-protobuf") || strings.Contains(ct, "application/protobuf")
}

func readBody(c *gin.Context) ([]byte, error) {
	return io.ReadAll(io.LimitReader(c.Request.Body, 100<<20))
}
