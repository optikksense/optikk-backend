package main

import (
	"encoding/json"
	"fmt"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

func main() {
	row := models.AnalyticsRow{
		TimeBucket: "2026-04-26 20:03:00",
		Group:      map[string]string{"service": "kafka"},
		Values:     map[string]float64{"count": 2558},
	}
	b, _ := json.MarshalIndent(row, "", "  ")
	fmt.Println(string(b))
}
