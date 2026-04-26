package deployments

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestParseServiceNameSupportsServiceFallback(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name     string
		rawQuery string
		want     string
		ok       bool
	}{
		{name: "serviceName", rawQuery: "serviceName=checkout", want: "checkout", ok: true},
		{name: "service fallback", rawQuery: "service=payments", want: "payments", ok: true},
		{name: "missing", rawQuery: "", want: "", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			req := httptest.NewRequest("GET", "/api/v1/deployments/list?"+tt.rawQuery, nil)
			c.Request = req

			got, ok := parseServiceName(c)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("parseServiceName() = (%q, %v), want (%q, %v)", got, ok, tt.want, tt.ok)
			}
		})
	}
}
