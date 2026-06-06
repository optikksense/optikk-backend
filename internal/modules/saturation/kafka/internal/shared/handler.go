package shared

import (
	"context"
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// HandleRangeQuery parses tenant and range, executes query, and responds.
func HandleRangeQuery(
	c *gin.Context,
	getTenant registry.GetTenantFunc,
	errMessage string,
	query func(ctx context.Context, teamID, startMs, endMs int64) (any, error),
) {
	teamID := getTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := query(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, errMessage, err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
