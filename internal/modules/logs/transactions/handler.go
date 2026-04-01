package transactions

import (
	"net/http"

	logshared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/queryparser"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type TransactionsRequest struct {
	StartTime    int64  `json:"startTime"`
	EndTime      int64  `json:"endTime"`
	Query        string `json:"query"`
	GroupByField string `json:"groupByField"`
	Limit        int    `json:"limit"`
}

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func NewHandler(getTenant modulecommon.GetTenantFunc, service *Service) *Handler {
	return &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  service,
	}
}

func (h *Handler) GetTransactions(c *gin.Context) {
	var req TransactionsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if req.StartTime <= 0 || req.EndTime <= 0 || req.StartTime >= req.EndTime {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}
	if req.GroupByField == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "groupByField is required")
		return
	}

	filters := logshared.LogFilters{
		TeamID:  h.GetTenant(c).TeamID,
		StartMs: req.StartTime,
		EndMs:   req.EndTime,
	}

	if req.Query != "" {
		node, err := queryparser.Parse(req.Query)
		if err != nil {
			modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid query syntax")
			return
		}
		if node != nil {
			applyQueryToFilters(node, &filters)
		}
	}

	resp, err := h.Service.GetTransactions(c.Request.Context(), filters, req.GroupByField, req.Limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get log transactions", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func applyQueryToFilters(node queryparser.Node, f *logshared.LogFilters) {
	switch n := node.(type) {
	case *queryparser.AndNode:
		for _, child := range n.Children {
			applyQueryToFilters(child, f)
		}
	case *queryparser.FieldMatch:
		switch n.Field {
		case "service":
			f.Services = append(f.Services, n.Value)
		case "status", "level", "severity":
			f.Severities = append(f.Severities, n.Value)
		case "host":
			f.Hosts = append(f.Hosts, n.Value)
		case "pod":
			f.Pods = append(f.Pods, n.Value)
		case "container":
			f.Containers = append(f.Containers, n.Value)
		case "environment":
			f.Environments = append(f.Environments, n.Value)
		}
	case *queryparser.FreeText:
		if f.Search == "" {
			f.Search = n.Text
		} else {
			f.Search += " " + n.Text
		}
	}
}
