package main

import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Return.Errors = true

	client, err := sarama.NewConsumer([]string{"localhost:9092"}, saramaCfg)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer client.Close()

	pc, err := client.ConsumePartition("otlp-metrics", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("failed to consume partition: %v", err)
	}
	defer pc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 0
	for {
		select {
		case msg := <-pc.Messages():
			fmt.Printf("Message: %s\n", string(msg.Value))
			count++
			if count >= 1 {
				return
			}
		case err := <-pc.Errors():
			log.Printf("error: %v", err)
			return
		case <-ctx.Done():
			return
		}
	}
}
