package identity

import "fmt"

type CreateUserInput struct {
	TeamIDs  []int64
	Email    string
	Name     string
	Role     string
	Password string
}

type CreateTeamInput struct {
	OrgName     string
	Name        string
	Slug        string
	Description string
	Color       string
}

type UpdateProfileInput struct {
	UserID    int64
	Name      string
	AvatarURL string
}

type ServiceErrorCode string

const (
	ServiceErrorValidation   ServiceErrorCode = "VALIDATION_ERROR"
	ServiceErrorUnauthorized ServiceErrorCode = "UNAUTHORIZED"
	ServiceErrorNotFound     ServiceErrorCode = "RESOURCE_NOT_FOUND"
	ServiceErrorInternal     ServiceErrorCode = "INTERNAL_ERROR"
)

type ServiceError struct {
	Code    ServiceErrorCode
	Message string
	Cause   error
}

func (e *ServiceError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *ServiceError) Unwrap() error {
	return e.Cause
}

func newValidationError(message string, cause error) error {
	return &ServiceError{Code: ServiceErrorValidation, Message: message, Cause: cause}
}

func newUnauthorizedError(message string, cause error) error {
	return &ServiceError{Code: ServiceErrorUnauthorized, Message: message, Cause: cause}
}

func newNotFoundError(message string, cause error) error {
	return &ServiceError{Code: ServiceErrorNotFound, Message: message, Cause: cause}
}

func newInternalError(message string, cause error) error {
	return &ServiceError{Code: ServiceErrorInternal, Message: message, Cause: cause}
}

type TeamMembership struct {
	TeamID int64  `json:"team_id"`
	Role   string `json:"role"`
}
