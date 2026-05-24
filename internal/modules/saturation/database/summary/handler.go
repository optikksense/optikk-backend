package summary

import (
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

