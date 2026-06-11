package httputil

import (
	"context"
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	"github.com/gin-gonic/gin"
)

// HandleRangeQuery parses tenant and required range, executes query, responds.
func HandleRangeQuery(
	c *gin.Context,
	getTenant GetTenantFunc,
	errMessage string,
	query func(ctx context.Context, teamID, startMs, endMs int64) (any, error),
) {
	teamID := getTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := query(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, errMessage, err)
		return
	}
	RespondOK(c, resp)
}
