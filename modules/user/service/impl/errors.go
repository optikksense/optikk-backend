package impl

import serviceinterfaces "github.com/observability/observability-backend-go/modules/user/service/interfaces"

func newValidationError(message string, cause error) error {
	return &serviceinterfaces.ServiceError{Code: serviceinterfaces.ServiceErrorValidation, Message: message, Cause: cause}
}

func newUnauthorizedError(message string, cause error) error {
	return &serviceinterfaces.ServiceError{Code: serviceinterfaces.ServiceErrorUnauthorized, Message: message, Cause: cause}
}

func newNotFoundError(message string, cause error) error {
	return &serviceinterfaces.ServiceError{Code: serviceinterfaces.ServiceErrorNotFound, Message: message, Cause: cause}
}

func newInternalError(message string, cause error) error {
	return &serviceinterfaces.ServiceError{Code: serviceinterfaces.ServiceErrorInternal, Message: message, Cause: cause}
}
