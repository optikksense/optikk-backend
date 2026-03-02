package main

import (
"context"
"fmt"
"net/http"
"strings"
"google.golang.org/grpc/metadata"
)

func main() {
	req, _ := http.NewRequest("POST", "/", nil)
	// Setting the header normally in HTTP
	req.Header.Set("x-api-key", "f52c40c58c6db3545bc3c9cc4859c158afd29c82a74c1d08930cedc27ee1ebb4")

	md := make(metadata.MD)
	for k, v := range req.Header {
		// This is the EXACT code from grpc-go that converts HTTP/2 headers to gRPC metadata
		md.Append(strings.ToLower(k), v...)
	}

	fmt.Printf("Metadata map: %v\n", md)

	// Simulate gRPC interceptor
	ctx := metadata.NewIncomingContext(context.Background(), md)
	
	// My extract logic
	mdFromCtx, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		fmt.Println("No metadata")
		return
	}
	
	var found string
	if vals := mdFromCtx.Get("x-api-key"); len(vals) > 0 {
		found = vals[0]
	}
	
	fmt.Printf("Extracted API Key: %s\n", found)
}
