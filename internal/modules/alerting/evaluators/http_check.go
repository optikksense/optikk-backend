package evaluators

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HTTPCheck performs outbound HTTP(S) probes. TargetRef JSON:
//
//	{
//	  "url": "https://example.com/health",
//	  "method": "GET",
//	  "expect_status": 200,
//	  "timeout_ms": 10000,
//	  "follow_redirects": false,
//	  "expect_body_substring": ""
//	}
//
// Thresholds use the standard state machine: operator "gt" with critical_threshold 0.5
// treats probe failure as value 1.0 on window "short" (alert) and success as 0.0.
type HTTPCheck struct{}

func (e *HTTPCheck) Kind() string { return "http_check" }

type httpCheckTarget struct {
	URL                 string `json:"url"`
	Method              string `json:"method"`
	ExpectStatus        int    `json:"expect_status"`
	TimeoutMS           int    `json:"timeout_ms"`
	FollowRedirects     bool   `json:"follow_redirects"`
	ExpectBodySubstring string `json:"expect_body_substring"`
}

func (e *HTTPCheck) Evaluate(ctx context.Context, rule Rule) ([]InstanceResult, error) {
	return e.run(ctx, rule)
}

func (e *HTTPCheck) EvaluateAt(ctx context.Context, rule Rule, _, _ int64) ([]InstanceResult, error) {
	return e.run(ctx, rule)
}

func (e *HTTPCheck) run(ctx context.Context, rule Rule) ([]InstanceResult, error) {
	var cfg httpCheckTarget
	if len(rule.TargetRef) == 0 {
		return httpCheckNoData(), nil
	}
	if err := json.Unmarshal(rule.TargetRef, &cfg); err != nil {
		return httpCheckNoData(), nil
	}
	if strings.TrimSpace(cfg.URL) == "" {
		return httpCheckNoData(), nil
	}
	if cfg.ExpectStatus == 0 {
		cfg.ExpectStatus = http.StatusOK
	}
	if cfg.TimeoutMS <= 0 {
		cfg.TimeoutMS = 10_000
	}
	if cfg.TimeoutMS > 60_000 {
		cfg.TimeoutMS = 60_000
	}
	method := strings.ToUpper(strings.TrimSpace(cfg.Method))
	if method == "" {
		method = http.MethodGet
	}
	if method != http.MethodGet && method != http.MethodHead {
		method = http.MethodGet
	}

	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return httpCheckNoData(), nil
	}
	host := u.Hostname()
	if host == "" || strings.EqualFold(host, "localhost") {
		return httpCheckNoData(), nil
	}
	if err := validateHTTPCheckHost(host); err != nil {
		return httpCheckNoData(), nil
	}

	timeout := time.Duration(cfg.TimeoutMS) * time.Millisecond
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, cfg.URL, nil)
	if err != nil {
		return httpCheckFailure(), nil
	}

	client := &http.Client{
		Timeout: timeout,
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if !cfg.FollowRedirects {
				return http.ErrUseLastResponse
			}
			if len(via) >= 5 {
				return http.ErrUseLastResponse
			}
			last := via[len(via)-1].URL
			if last != nil {
				h := last.Hostname()
				if h != "" && validateHTTPCheckHost(h) != nil {
					return http.ErrUseLastResponse
				}
			}
			return nil
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return httpCheckFailure(), nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))

	failed := resp.StatusCode != cfg.ExpectStatus
	if !failed && cfg.ExpectBodySubstring != "" {
		if !strings.Contains(string(body), cfg.ExpectBodySubstring) {
			failed = true
		}
	}
	if failed {
		return httpCheckFailure(), nil
	}
	return httpCheckSuccess(), nil
}

func httpCheckSuccess() []InstanceResult {
	return []InstanceResult{{
		InstanceKey: "*",
		Windows:     map[string]float64{"short": 0},
		NoData:      false,
	}}
}

func httpCheckFailure() []InstanceResult {
	return []InstanceResult{{
		InstanceKey: "*",
		Windows:     map[string]float64{"short": 1},
		NoData:      false,
	}}
}

func httpCheckNoData() []InstanceResult {
	return []InstanceResult{{
		InstanceKey: "*",
		Windows:     map[string]float64{"short": 0},
		NoData:      true,
	}}
}

// validateHTTPCheckHost blocks SSRF-prone destinations after DNS resolution.
func validateHTTPCheckHost(host string) error {
	if host == "" {
		return errors.New("empty host")
	}
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return errors.New("resolve")
	}
	for _, ip := range ips {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
			return errors.New("private")
		}
		if ip4 := ip.To4(); ip4 != nil {
			if ip4[0] == 0 {
				return errors.New("unspecified")
			}
			if ip4[0] == 169 && ip4[1] == 254 {
				return errors.New("link-local")
			}
		}
	}
	return nil
}
