package dashboardcfg

import (
	"encoding/json"

	queryvalue "github.com/Optikk-Org/optikk-backend/internal/shared/contracts/queryvalue"
)

func FloatPtr(value float64) *float64 {
	return &value
}

func IntPtr(value int) *int {
	return &value
}

func MustQueryParams(raw map[string]any) map[string]queryvalue.Value {
	if len(raw) == 0 {
		return nil
	}
	params := make(map[string]queryvalue.Value, len(raw))
	for key, value := range raw {
		params[key] = MustQueryValue(value)
	}
	return params
}

func MustQueryValue(value any) queryvalue.Value {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var parsed queryvalue.Value
	if err := json.Unmarshal(payload, &parsed); err != nil {
		panic(err)
	}
	return parsed
}
