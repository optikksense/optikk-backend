package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/config"
	"github.com/Optikk-Org/optikk-backend/internal/app/server"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/explorer"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	app, err := server.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	service := explorer.NewService(app.Infra.CH, nil, cfg, app.Infra)

	now := time.Now().UnixMilli()
	req := explorer.Request{
		StartTime: now - 1800000,
		EndTime:   now,
		Limit:     20,
	}

	resp, err := service.Query(context.Background(), req, 1)
	if err != nil {
		log.Fatal(err)
	}

	b, _ := json.MarshalIndent(resp, "", "  ")
	fmt.Println(string(b))
}
