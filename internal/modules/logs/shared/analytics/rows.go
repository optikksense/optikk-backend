package analytics

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

// ScanAnyRow scans a CH row into a `[]any` slice for dynamic-shape results.
func ScanAnyRow(rows chdriver.Rows, n int) ([]any, error) {
	values := make([]any, n)
	ptrs := make([]any, n)
	
	cts := rows.ColumnTypes()
	for i, ct := range cts {
		var ptr any
		if ct != nil {
			st := ct.ScanType()
			if st != nil && st.Kind() != reflect.Interface {
				ptr = reflect.New(st).Interface()
			} else {
				dbType := strings.ToLower(ct.DatabaseTypeName())
				if strings.Contains(dbType, "string") {
					ptr = new(string)
				} else if strings.Contains(dbType, "datetime") || strings.Contains(dbType, "date") {
					ptr = new(time.Time)
				} else if strings.Contains(dbType, "float") || strings.Contains(dbType, "avg") {
					ptr = new(float64)
				} else if strings.Contains(dbType, "int") {
					ptr = new(int64)
				} else if strings.Contains(dbType, "bool") {
					ptr = new(bool)
				} else {
					ptr = &values[i]
				}
			}
		} else {
			ptr = &values[i]
		}
		ptrs[i] = ptr
	}
	
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	
	for i := range ptrs {
		if val := reflect.ValueOf(ptrs[i]); val.Kind() == reflect.Ptr && val.Elem().CanInterface() {
			values[i] = val.Elem().Interface()
		} else {
			values[i] = ptrs[i]
		}
	}
	
	return values, nil
}

// MapRow converts the raw scanned values into a typed AnalyticsRow.
// time_bucket → TimeBucket; agg aliases → Values; everything else → Group.
func MapRow(cols []string, values []any, aggSet map[string]bool) models.AnalyticsRow {
	row := models.AnalyticsRow{
		Group:  make(map[string]string),
		Values: make(map[string]float64),
	}
	for i, col := range cols {
		if i >= len(values) {
			break
		}
		v := values[i]
		if col == "time_bucket" {
			row.TimeBucket = ToString(v)
			continue
		}
		if aggSet[col] {
			row.Values[col] = ToFloat(v)
			continue
		}
		row.Group[col] = ToString(v)
	}
	return row
}

func ToString(v any) string {
	if v == nil {
		return ""
	}
	if valuer, ok := v.(driver.Valuer); ok {
		val, err := valuer.Value()
		if err != nil {
			return ""
		}
		return ToString(val)
	}
	
	val := reflect.ValueOf(v)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return ""
		}
		val = val.Elem()
	}
	
	switch x := val.Interface().(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", x)
	}
}

func ToFloat(v any) float64 {
	if v == nil {
		return 0
	}
	
	val := reflect.ValueOf(v)
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return 0
		}
		val = val.Elem()
	}
	
	switch x := val.Interface().(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case uint:
		return float64(x)
	case uint32:
		return float64(x)
	case uint64:
		return float64(x)
	case uint8:
		return float64(x)
	case string:
		return 0
	}
	return 0
}
