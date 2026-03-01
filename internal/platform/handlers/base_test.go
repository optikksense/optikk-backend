package handlers

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestParseListParamSupportsBracketArrays(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest("GET", "/?services[]=svc-a&services[]=svc-b", nil)
	ctx.Request = req

	got := ParseListParam(ctx, "services")
	if len(got) != 2 || got[0] != "svc-a" || got[1] != "svc-b" {
		t.Fatalf("unexpected parsed values: %#v", got)
	}
}
