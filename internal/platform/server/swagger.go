package server

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

type openAPIDoc struct {
	OpenAPI    string           `json:"openapi"`
	Info       map[string]any   `json:"info"`
	Servers    []map[string]any `json:"servers,omitempty"`
	Paths      map[string]any   `json:"paths"`
	Components map[string]any   `json:"components,omitempty"`
}

func (a *App) registerSwagger(r *gin.Engine) {
	r.GET("/openapi.json", func(c *gin.Context) {
		c.JSON(200, a.buildOpenAPI(r))
	})

	r.GET("/swagger/*any", ginSwagger.WrapHandler(
		swaggerFiles.Handler,
		ginSwagger.URL("/openapi.json"),
		ginSwagger.DefaultModelsExpandDepth(-1),
	))
}

func (a *App) buildOpenAPI(r *gin.Engine) openAPIDoc {
	paths := map[string]any{}

	routes := r.Routes()
	sort.Slice(routes, func(i, j int) bool {
		if routes[i].Path == routes[j].Path {
			return routes[i].Method < routes[j].Method
		}
		return routes[i].Path < routes[j].Path
	})

	for _, route := range routes {
		if strings.HasPrefix(route.Path, "/swagger") {
			continue
		}

		path := toOpenAPIPath(route.Path)
		method := strings.ToLower(route.Method)
		tags := []string{getTag(route.Path)}

		operation := map[string]any{
			"tags":        tags,
			"operationId": operationID(route.Method, route.Path),
			"summary":     fmt.Sprintf("%s %s", route.Method, route.Path),
			"responses": map[string]any{
				"200": map[string]any{"description": "OK"},
				"400": map[string]any{"description": "Bad Request"},
				"500": map[string]any{"description": "Internal Server Error"},
			},
			"security": []map[string][]string{{"bearerAuth": []string{}}},
		}

		// Add request body for POST, PUT, PATCH methods
		if method == "post" || method == "put" || method == "patch" {
			schemaName := getSchemaNameForPath(path, method)
			if schemaName != "" {
				operation["requestBody"] = map[string]any{
					"required": true,
					"content": map[string]any{
						"application/json": map[string]any{
							"schema": map[string]any{
								"$ref": "#/components/schemas/" + schemaName,
							},
						},
					},
				}
			}
		}

		entry, ok := paths[path]
		if !ok {
			entry = map[string]any{}
			paths[path] = entry
		}
		pathMap := entry.(map[string]any)
		pathMap[method] = operation
	}

	schemas := getSchemas()
	schemas["Error"] = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"code":    map[string]any{"type": "string"},
			"message": map[string]any{"type": "string"},
			"path":    map[string]any{"type": "string"},
		},
	}
	schemas["Success"] = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"data": map[string]any{"type": "object"},
		},
	}

	return openAPIDoc{
		OpenAPI: "3.0.3",
		Info: map[string]any{
			"title":       "Observability Backend Go API",
			"version":     "1.0.0",
			"description": "Auto-generated OpenAPI from registered Gin routes.",
		},
		Servers: []map[string]any{{
			"url": fmt.Sprintf("http://localhost:%s", a.Config.Port),
		}},
		Paths: paths,
		Components: map[string]any{
			"securitySchemes": map[string]any{
				"bearerAuth": map[string]any{
					"type":         "http",
					"scheme":       "bearer",
					"bearerFormat": "JWT",
				},
			},
			"schemas": schemas,
		},
	}
}

func toOpenAPIPath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if strings.HasPrefix(p, ":") {
			parts[i] = "{" + strings.TrimPrefix(p, ":") + "}"
			continue
		}
		if strings.HasPrefix(p, "*") {
			parts[i] = "{" + strings.TrimPrefix(p, "*") + "}"
		}
	}
	return strings.Join(parts, "/")
}

func operationID(method, path string) string {
	sanitized := strings.ReplaceAll(path, "/", "_")
	sanitized = strings.ReplaceAll(sanitized, ":", "")
	sanitized = strings.ReplaceAll(sanitized, "*", "")
	sanitized = strings.Trim(sanitized, "_")
	if sanitized == "" {
		sanitized = "root"
	}
	return strings.ToLower(method) + "_" + sanitized
}

func getTag(path string) string {
	path, _ = strings.CutPrefix(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 {
		return "default"
	}

	// Skip "api" prefix if present
	idx := 0
	if parts[0] == "api" {
		idx = 1
	}

	// Skip version prefix (v1, v2, etc.) if present
	if idx < len(parts) {
		if _, ok := strings.CutPrefix(parts[idx], "v"); ok && len(parts[idx]) > 1 {
			idx++
		}
	}

	// Get the resource name (logs, metrics, traces, etc.)
	if idx < len(parts) && parts[idx] != "" {
		tag := parts[idx]
		// Capitalize first letter
		return strings.ToUpper(tag[:1]) + tag[1:]
	}
	return "default"
}

func getSchemaNameForPath(path string, method string) string {
	// Map specific endpoints to their request schemas
	schemaMap := map[string]string{
		// Auth & Users
		"/api/signup|post":                        "SignupRequest",
		"/api/auth/login|post":                    "LoginRequest",
		"/api/auth/logout|post":                   "EmptyRequest",
		"/api/users|post":                         "UserRequest",
		"/api/users/{userId}/teams/{teamId}|post": "EmptyRequest",
		"/api/teams|post":                         "CreateTeamRequest",
		"/api/settings/profile|put":               "SettingsRequest",

		// Alerts
		"/api/alerts|post":                       "AlertRequest",
		"/api/alerts/{id}/acknowledge|post":      "EmptyRequest",
		"/api/alerts/{id}/resolve|post":          "EmptyRequest",
		"/api/alerts/{id}/mute|post":             "EmptyRequest",
		"/api/alerts/{id}/mute-with-reason|post": "MuteAlertRequest",
		"/api/alerts/bulk/mute|post":             "BulkAlertActionRequest",
		"/api/alerts/bulk/resolve|post":          "BulkAlertActionRequest",

		// Deployments
		"/api/v1/deployments|post": "DeploymentCreateRequest",

		// OTLP
		"/otlp/v1/metrics|post": "OTLPMetricsRequest",
		"/otlp/v1/logs|post":    "OTLPLogsRequest",
		"/otlp/v1/traces|post":  "OTLPTracesRequest",
	}

	key := path + "|" + method
	if schema, ok := schemaMap[key]; ok {
		return schema
	}
	return ""
}

func getSchemas() map[string]any {
	return map[string]any{
		"EmptyRequest": map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
		"SignupRequest": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"email":    map[string]any{"type": "string", "example": "user@example.com"},
				"name":     map[string]any{"type": "string", "example": "John Doe"},
				"password": map[string]any{"type": "string", "example": "securePassword123"},
				"teamName": map[string]any{"type": "string", "example": "My Team"},
				"orgId":    map[string]any{"type": "integer", "example": 1},
				"teamId":   map[string]any{"type": "integer", "example": 5},
			},
			"required": []string{"email", "password", "teamName"},
		},
		"LoginRequest": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"email":    map[string]any{"type": "string", "example": "user@example.com"},
				"password": map[string]any{"type": "string", "example": "securePassword123"},
			},
			"required": []string{"email", "password"},
		},
		"UserRequest": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"email":    map[string]any{"type": "string"},
				"name":     map[string]any{"type": "string"},
				"role":     map[string]any{"type": "string"},
				"password": map[string]any{"type": "string"},
			},
		},
		"CreateTeamRequest": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name":        map[string]any{"type": "string", "example": "Engineering"},
				"slug":        map[string]any{"type": "string", "example": "engineering"},
				"description": map[string]any{"type": "string"},
				"color":       map[string]any{"type": "string", "example": "#3B82F6"},
			},
			"required": []string{"name"},
		},
		"AlertRequest": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name":            map[string]any{"type": "string"},
				"description":     map[string]any{"type": "string"},
				"type":            map[string]any{"type": "string"},
				"severity":        map[string]any{"type": "string"},
				"serviceName":     map[string]any{"type": "string"},
				"condition":       map[string]any{"type": "string"},
				"metric":          map[string]any{"type": "string"},
				"operator":        map[string]any{"type": "string"},
				"threshold":       map[string]any{"type": "number"},
				"durationMinutes": map[string]any{"type": "integer"},
				"runbookUrl":      map[string]any{"type": "string"},
			},
			"required": []string{"name", "type", "severity"},
		},
		"MuteAlertRequest": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"reason": map[string]any{"type": "string", "example": "Investigating the issue"},
			},
		},
		"BulkAlertActionRequest": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"alertIds": map[string]any{
					"type":    "array",
					"items":   map[string]any{"type": "integer"},
					"example": []int{1, 2, 3},
				},
			},
			"required": []string{"alertIds"},
		},

		"DeploymentCreateRequest": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"service_name":     map[string]any{"type": "string"},
				"version":          map[string]any{"type": "string"},
				"environment":      map[string]any{"type": "string"},
				"deployed_by":      map[string]any{"type": "string"},
				"status":           map[string]any{"type": "string"},
				"commit_sha":       map[string]any{"type": "string"},
				"duration_seconds": map[string]any{"type": "integer"},
				"attributes":       map[string]any{"type": "object"},
			},
		},
		"SettingsRequest": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name":        map[string]any{"type": "string"},
				"avatarUrl":   map[string]any{"type": "string"},
				"preferences": map[string]any{"type": "object"},
			},
		},
		"OTLPMetricsRequest": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"resourceMetrics": map[string]any{"type": "array"},
			},
		},
		"OTLPLogsRequest": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"resourceLogs": map[string]any{"type": "array"},
			},
		},
		"OTLPTracesRequest": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"resourceSpans": map[string]any{"type": "array"},
			},
		},
	}
}
