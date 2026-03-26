package shared

import (
	"errors"
	"net/http"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

func RespondServiceError(c *gin.Context, err error, fallbackMessage string) {
	var serviceErr *ServiceError
	if errors.As(err, &serviceErr) {
		status := http.StatusInternalServerError
		switch serviceErr.Code {
		case ServiceErrorValidation:
			status = http.StatusBadRequest
		case ServiceErrorUnauthorized:
			status = http.StatusUnauthorized
		case ServiceErrorNotFound:
			status = http.StatusNotFound
		case ServiceErrorInternal:
			status = http.StatusInternalServerError
		}

		message := serviceErr.Message
		if message == "" {
			message = fallbackMessage
		}
		if message == "" {
			message = "Internal error"
		}

		code := string(serviceErr.Code)
		if code == "" {
			code = string(ServiceErrorInternal)
		}
		modulecommon.RespondError(c, status, code, message)
		return
	}

	if fallbackMessage == "" {
		fallbackMessage = "Internal error"
	}
	modulecommon.RespondError(c, http.StatusInternalServerError, string(ServiceErrorInternal), fallbackMessage)
}
