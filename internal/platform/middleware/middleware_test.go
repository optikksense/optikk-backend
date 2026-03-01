package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/platform/auth"
)

// helper: generate a JWT with teamID as primary team and extra teams in the Teams list.
func makeToken(t *testing.T, jm auth.JWTManager, userID int64, teamID int64, extraTeams ...int64) string {
	t.Helper()
	teams := append([]int64{teamID}, extraTeams...)
	tok, err := jm.Generate(userID, "user@example.com", "User", "member", teamID, teams...)
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}
	return tok
}

// TestTenantMiddleware_JWTTeamDefault verifies that the primary team from the
// JWT is used when no X-Team-Id header is provided.
func TestTenantMiddleware_JWTTeamDefault(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jwtManager := auth.JWTManager{Secret: []byte("test-secret"), Expiration: time.Hour}
	token := makeToken(t, jwtManager, 42, 94)

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

// TestTenantMiddleware_JWTTeamOverrideFromHeader verifies that the X-Team-Id header
// overrides the primary team ONLY if the user is a member of the requested team.
func TestTenantMiddleware_JWTTeamOverrideFromHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jwtManager := auth.JWTManager{Secret: []byte("test-secret"), Expiration: time.Hour}
	// User is member of teams 94 AND 12.
	token := makeToken(t, jwtManager, 42, 94, 12)

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

// TestTenantMiddleware_XTeamIdForbidden verifies that switching to a team the
// user is NOT a member of returns HTTP 403 (Fix 2: cross-tenant header exploit).
func TestTenantMiddleware_XTeamIdForbidden(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jwtManager := auth.JWTManager{Secret: []byte("test-secret"), Expiration: time.Hour}
	// User is a member of team 94 only.
	token := makeToken(t, jwtManager, 42, 94)

	rec := httptest.NewRecorder()
	_, r := gin.CreateTestContext(rec)
	r.Use(TenantMiddleware(jwtManager))
	r.GET("/test", func(ctx *gin.Context) {
		t.Fatal("handler should not be reached — should have 403'd")
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Team-Id", "999") // team 999 not in the user's Teams list
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403 for unauthorized team override, got %d", rec.Code)
	}
}

// TestTenantMiddleware_CookieFallback verifies that the JWT is accepted from the
// httpOnly cookie when no Authorization header is present (Fix 1).
func TestTenantMiddleware_CookieFallback(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jwtManager := auth.JWTManager{Secret: []byte("test-secret"), Expiration: time.Hour}
	token := makeToken(t, jwtManager, 42, 94)

	rec := httptest.NewRecorder()
	_, r := gin.CreateTestContext(rec)
	r.Use(TenantMiddleware(jwtManager))
	r.GET("/test", func(ctx *gin.Context) {
		tenant := GetTenant(ctx)
		if tenant.TeamID != 94 {
			t.Fatalf("expected team 94 from cookie, got %d", tenant.TeamID)
		}
		ctx.Status(http.StatusNoContent)
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	// No Authorization header — only the cookie.
	req.AddCookie(&http.Cookie{Name: "token", Value: token})
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected status 204 when auth via cookie, got %d", rec.Code)
	}
}

// TestTenantMiddleware_JWTFallbackToTeam1WhenMissing verifies that a JWT with
// teamID=0 results in a 403 (no valid team context).
func TestTenantMiddleware_JWTFallbackToTeam1WhenMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	jwtManager := auth.JWTManager{Secret: []byte("test-secret"), Expiration: time.Hour}
	token := makeToken(t, jwtManager, 42, 0) // teamID=0 → no team

	rec := httptest.NewRecorder()
	_, r := gin.CreateTestContext(rec)
	r.Use(TenantMiddleware(jwtManager))
	r.GET("/test", func(ctx *gin.Context) {
		t.Fatal("handler should not be reached — should have 403'd")
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status 403 when no valid team in JWT, got %d", rec.Code)
	}
}
