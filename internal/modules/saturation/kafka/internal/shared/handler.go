package shared

import (
	"context"
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/app/registry"
	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// HandleRangeQuery is the common shape every kafka panel handler reaches for:
// pull tenant + required range, fan to a teamID-scoped query, and respond.
// Free function so each submodule's handler can reuse it without inheritance.
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
