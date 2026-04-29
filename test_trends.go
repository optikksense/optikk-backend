package main

import (
	"bytes"
	"fmt"
	"net/http"
	"time"
)

func main() {
	time.Sleep(2 * time.Second) // wait for server to start if running in background
	body := []byte(`{"startTime":1700000000000,"endTime":1700005000000,"step":"1h"}`)
	resp, err := http.Post("http://localhost:8080/api/v1/logs/trends", "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer resp.Body.Close()
	fmt.Println("Status:", resp.StatusCode)
}
