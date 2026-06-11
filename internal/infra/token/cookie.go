package token

import (
	"net/http"
	"strings"
)

// RefreshCookiePath limits the refresh cookie to the refresh endpoint.
const RefreshCookiePath = "/api/v1/auth/refresh"

type cookieOpts struct {
	name     string
	domain   string
	secure   bool
	sameSite http.SameSite
}

// RefreshCookieName returns the configured refresh cookie name.
func (s *Service) RefreshCookieName() string {
	return s.cookie.name
}

// SetRefreshCookie writes the refresh token as an httpOnly cookie.
func (s *Service) SetRefreshCookie(w http.ResponseWriter, token string) {
	http.SetCookie(w, &http.Cookie{
		Name:     s.cookie.name,
		Value:    token,
		Path:     RefreshCookiePath,
		Domain:   s.cookie.domain,
		MaxAge:   int(s.refreshTTL.Seconds()),
		HttpOnly: true,
		Secure:   s.cookie.secure,
		SameSite: s.cookie.sameSite,
	})
}

// ClearRefreshCookie expires the refresh cookie.
func (s *Service) ClearRefreshCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     s.cookie.name,
		Value:    "",
		Path:     RefreshCookiePath,
		Domain:   s.cookie.domain,
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   s.cookie.secure,
		SameSite: s.cookie.sameSite,
	})
}

func parseSameSite(raw string) http.SameSite {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "strict":
		return http.SameSiteStrictMode
	case "none":
		return http.SameSiteNoneMode
	case "default":
		return http.SameSiteDefaultMode
	default:
		return http.SameSiteLaxMode
	}
}
