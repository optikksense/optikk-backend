package main

import (
	"context"
	"fmt"
	"time"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/log_analytics"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

func main() {
	opts, err := clickhouse.ParseDSN("clickhouse://default:clickhouse123@127.0.0.1:19000/observability")
	if err != nil { panic(err) }
	conn, err := clickhouse.Open(opts)
	if err != nil { panic(err) }

	repo := log_analytics.NewRepository(conn)
	
	now := time.Now()
	
	req := log_analytics.Request{
		StartTime:    now.Add(-24 * time.Hour).UnixMilli(),
		EndTime:      now.Add(24 * time.Hour).UnixMilli(),
		Filters:      []querycompiler.StructuredFilter{},
		GroupBy:      []string{"service"},
		Aggregations: []models.Aggregation{{Fn: "count"}},
		VizMode:      "table",
		Step:         "auto",
		Limit:        100,
	}

	filters, err := querycompiler.FromStructured(req.Filters, 1, req.StartTime, req.EndTime)
	if err != nil { panic(err) }

	rows, _, dropped, err := repo.Analytics(context.Background(), filters, req)
	if err != nil { panic(err) }

	fmt.Printf("Dropped: %v\n", dropped)
	fmt.Printf("Returned %d rows\n", len(rows))
}
