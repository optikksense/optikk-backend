package server

import (
	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/telemetry"
)

func (a *App) registerRoutes(r *gin.Engine) {
	api := r.Group("/api")
	{
		api.POST("/signup", a.Users.Signup)

		auth := api.Group("/auth")
		{
			auth.POST("/login", a.Auth.Login)
			auth.POST("/logout", a.Auth.Logout)
			auth.GET("/me", a.Auth.AuthMe)
			auth.GET("/context", a.Auth.AuthContext)
			auth.GET("/validate", a.Auth.ValidateToken)
		}

		users := api.Group("/users")
		{
			users.GET("/me", a.Users.GetCurrentUser)
			users.GET("", a.Users.GetUsers)
			users.GET("/:id", a.Users.GetUserByID)
			users.POST("", a.Users.CreateUser)
			users.POST("/:userId/teams/:teamId", a.Users.AddUserToTeam)
			users.DELETE("/:userId/teams/:teamId", a.Users.RemoveUserFromTeam)
		}

		teams := api.Group("/teams")
		{
			teams.GET("", a.Users.GetTeams)
			teams.GET("/my-teams", a.Users.GetMyTeams)
			teams.GET("/:id", a.Users.GetTeamByID)
			teams.GET("/slug/:slug", a.Users.GetTeamBySlug)
			teams.POST("", a.Users.CreateTeam)
		}

		settings := api.Group("/settings")
		{
			settings.GET("/profile", a.Users.GetProfile)
			settings.PUT("/profile", a.Users.UpdateProfile)
		}

		alerts := api.Group("/alerts")
		{
			alerts.GET("", a.Alerts.GetAlerts)
			alerts.GET("/paged", a.Alerts.GetAlertsPaged)
			alerts.GET("/:id", a.Alerts.GetAlertByID)
			alerts.POST("", a.Alerts.CreateAlert)
			alerts.POST("/:id/acknowledge", a.Alerts.AcknowledgeAlert)
			alerts.POST("/:id/resolve", a.Alerts.ResolveAlert)
			alerts.POST("/:id/mute", a.Alerts.MuteAlert)
			alerts.POST("/:id/mute-with-reason", a.Alerts.MuteAlertWithReason)
			alerts.POST("/bulk/mute", a.Alerts.BulkMuteAlerts)
			alerts.POST("/bulk/resolve", a.Alerts.BulkResolveAlerts)
			alerts.GET("/for-incident/:policyId", a.Alerts.GetAlertsForIncident)
			alerts.GET("/count/active", a.Alerts.CountActiveAlerts)
		}

		dashboard := api.Group("/dashboard")
		{
			dashboard.GET("/overview", a.Metrics.GetDashboardOverview)
			dashboard.GET("/services", a.Metrics.GetDashboardServices)
			dashboard.GET("/services/:serviceName", a.Metrics.GetDashboardServiceDetail)
		}
	}

	v1 := r.Group("/api/v1")
	{
		v1.GET("/status", a.Metrics.GetSystemStatus)
		v1.GET("/incidents", a.Alerts.GetIncidents)

		v1.GET("/services/topology", a.Metrics.GetServiceTopology)
		v1.GET("/services/dependencies", a.Traces.GetServiceDependencies)
		v1.GET("/services/:serviceName/errors", a.Traces.GetServiceErrors)
		v1.GET("/services/metrics", a.Metrics.GetServiceMetrics)
		v1.GET("/services/timeseries", a.Metrics.GetServiceTimeSeries)
		v1.GET("/services/:serviceName/endpoints", a.Metrics.GetServiceEndpointBreakdown)
		v1.GET("/endpoints/metrics", a.Metrics.GetEndpointMetrics)
		v1.GET("/endpoints/timeseries", a.Metrics.GetEndpointTimeSeries)

		v1.GET("/logs", a.Logs.GetLogs)
		v1.GET("/logs/histogram", a.Logs.GetLogHistogram)
		v1.GET("/logs/volume", a.Logs.GetLogVolume)
		v1.GET("/logs/stats", a.Logs.GetLogStats)
		v1.GET("/logs/fields", a.Logs.GetLogFields)
		v1.GET("/logs/surrounding", a.Logs.GetLogSurrounding)
		v1.GET("/logs/detail", a.Logs.GetLogDetail)
		v1.GET("/traces/:traceId/logs", a.Logs.GetTraceLogs)

		v1.GET("/traces", a.Traces.GetTraces)
		v1.GET("/traces/:traceId/spans", a.Traces.GetTraceSpans)

		v1.GET("/latency/histogram", a.Traces.GetLatencyHistogram)
		v1.GET("/latency/heatmap", a.Traces.GetLatencyHeatmap)
		v1.GET("/errors/groups", a.Traces.GetErrorGroups)
		v1.GET("/errors/timeseries", a.Traces.GetErrorTimeSeries)
		v1.GET("/saturation/metrics", a.Traces.GetSaturationMetrics)
		v1.GET("/saturation/timeseries", a.Traces.GetSaturationTimeSeries)

		v1.GET("/metrics/timeseries", a.Metrics.GetMetricsTimeSeries)
		v1.GET("/metrics/summary", a.Metrics.GetMetricsSummary)

		v1.GET("/infrastructure", a.Metrics.GetInfrastructure)
		v1.GET("/infrastructure/nodes", a.Metrics.GetInfrastructureNodes)
		v1.GET("/infrastructure/nodes/:host/services", a.Metrics.GetInfrastructureNodeServices)

		v1.GET("/health-checks", a.Health.GetHealthChecks)
		v1.POST("/health-checks", a.Health.CreateHealthCheck)
		v1.PUT("/health-checks/:id", a.Health.UpdateHealthCheck)
		v1.DELETE("/health-checks/:id", a.Health.DeleteHealthCheck)
		v1.PATCH("/health-checks/:id/toggle", a.Health.ToggleHealthCheck)
		v1.GET("/health-checks/status", a.Health.GetHealthCheckStatus)
		v1.GET("/health-checks/:checkId/results", a.Health.GetHealthCheckResults)
		v1.GET("/health-checks/:checkId/trend", a.Health.GetHealthCheckTrend)

		v1.GET("/deployments", a.Deployments.GetDeployments)
		v1.GET("/deployments/events", a.Deployments.GetDeploymentEvents)
		v1.GET("/deployments/:deployId/diff", a.Deployments.GetDeploymentDiff)
		v1.POST("/deployments", a.Deployments.CreateDeployment)

		v1.GET("/insights/resource-utilization", a.Insights.GetInsightResourceUtilization)
		v1.GET("/insights/slo-sli", a.Insights.GetInsightSloSli)
		v1.GET("/insights/logs-stream", a.Insights.GetInsightLogsStream)
		v1.GET("/insights/database-cache", a.Insights.GetInsightDatabaseCache)
		v1.GET("/insights/messaging-queue", a.Insights.GetInsightMessagingQueue)

		v1.GET("/ai/summary", a.AI.GetAISummary)
		v1.GET("/ai/models", a.AI.GetAIModels)
		v1.GET("/ai/performance/metrics", a.AI.GetAIPerformanceMetrics)
		v1.GET("/ai/performance/timeseries", a.AI.GetAIPerformanceTimeSeries)
		v1.GET("/ai/performance/latency-histogram", a.AI.GetAILatencyHistogram)
		v1.GET("/ai/cost/metrics", a.AI.GetAICostMetrics)
		v1.GET("/ai/cost/timeseries", a.AI.GetAICostTimeSeries)
		v1.GET("/ai/cost/token-breakdown", a.AI.GetAITokenBreakdown)
		v1.GET("/ai/security/metrics", a.AI.GetAISecurityMetrics)
		v1.GET("/ai/security/timeseries", a.AI.GetAISecurityTimeSeries)
		v1.GET("/ai/security/pii-categories", a.AI.GetAIPiiCategories)

		v1.GET("/dashboard-config/pages", a.DashboardConfig.ListPages)
		v1.GET("/dashboard-config/:pageId", a.DashboardConfig.GetDashboardConfig)
		v1.PUT("/dashboard-config/:pageId", a.DashboardConfig.SaveDashboardConfig)

		v1.GET("/explore/saved-queries", a.Explore.ListSavedQueries)
		v1.POST("/explore/saved-queries", a.Explore.CreateSavedQuery)
		v1.PUT("/explore/saved-queries/:id", a.Explore.UpdateSavedQuery)
		v1.DELETE("/explore/saved-queries/:id", a.Explore.DeleteSavedQuery)
	}

	// OTLP ingestion endpoint — authenticated via api_key (not JWT).
	otlp := r.Group("/otlp")
	{
		repo := telemetry.NewRepository(database.NewMySQLWrapper(a.CH))
		otlpHandler := telemetry.NewHandler(repo, a.DB)
		otlp.POST("/v1/metrics", otlpHandler.HandleMetrics)
		otlp.POST("/v1/logs", otlpHandler.HandleLogs)
		otlp.POST("/v1/traces", otlpHandler.HandleTraces)
	}
}
