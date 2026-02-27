package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/platform/auth"
)

func TestTenantMiddleware_JWTTeamDefault(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jwtManager := auth.JWTManager{Secret: []byte("test-secret"), Expiration: time.Hour}
	token, err := jwtManager.Generate(42, "user@example.com", "User", "member", 94)
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}

	rec := httptest.NewRecorder()
	_, r := gin.CreateTestContext(rec)
	r.Use(TenantMiddleware(jwtManager))
	r.GET("/test", func(ctx *gin.Context) {
		tenant := GetTenant(ctx)
		if tenant.TeamID != 94 {
			t.Fatalf("expected team 94, got %d", tenant.TeamID)
		}
		if tenant.UserID != 42 {
			t.Fatalf("expected user 42, got %d", tenant.UserID)
		}
		ctx.Status(http.StatusNoContent)
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status 204, got %d", rec.Code)
	}
}

func TestTenantMiddleware_JWTTeamOverrideFromHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jwtManager := auth.JWTManager{Secret: []byte("test-secret"), Expiration: time.Hour}
	token, err := jwtManager.Generate(42, "user@example.com", "User", "member", 94)
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}

	rec := httptest.NewRecorder()
	_, r := gin.CreateTestContext(rec)
	r.Use(TenantMiddleware(jwtManager))
	r.GET("/test", func(ctx *gin.Context) {
		tenant := GetTenant(ctx)
		if tenant.TeamID != 12 {
			t.Fatalf("expected team 12 from header override, got %d", tenant.TeamID)
		}
		if tenant.UserID != 42 {
			t.Fatalf("expected user 42, got %d", tenant.UserID)
		}
		ctx.Status(http.StatusNoContent)
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Team-Id", "12")
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status 204, got %d", rec.Code)
	}
}

func TestTenantMiddleware_JWTFallbackToTeam1WhenMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jwtManager := auth.JWTManager{Secret: []byte("test-secret"), Expiration: time.Hour}
	token, err := jwtManager.Generate(42, "user@example.com", "User", "member", 0)
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}

	rec := httptest.NewRecorder()
	_, r := gin.CreateTestContext(rec)
	r.Use(TenantMiddleware(jwtManager))
	r.GET("/test", func(ctx *gin.Context) {
		tenant := GetTenant(ctx)
		if tenant.TeamID != 1 {
			t.Fatalf("expected default team 1, got %d", tenant.TeamID)
		}
		ctx.Status(http.StatusNoContent)
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status 204, got %d", rec.Code)
	}
}
