package shared

import (
	"fmt"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"
)

type TeamMembership struct {
	TeamID int64  `json:"team_id"`
	Role   string `json:"role"`
}

type AuthUser struct {
	ID           int64
	Email        string
	PasswordHash string
	Name         string
	AvatarURL    string
	TeamsJSON    string
}

type UserRecord struct {
	ID          int64
	Email       string
	Name        string
	AvatarURL   string
	TeamsJSON   string
	Active      bool
	LastLoginAt any
	CreatedAt   any
}

type TeamRecord struct {
	ID          int64
	OrgName     string
	Name        string
	Slug        string
	Description *string
	Active      bool
	Color       string
	Icon        string
	APIKey      string
	CreatedAt   any
}

type ServiceErrorCode string

const (
	ServiceErrorValidation   ServiceErrorCode = errorcode.Validation
	ServiceErrorUnauthorized ServiceErrorCode = errorcode.Unauthorized
	ServiceErrorNotFound     ServiceErrorCode = errorcode.NotFound
	ServiceErrorInternal     ServiceErrorCode = errorcode.Internal
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

func NewValidationError(message string, cause error) error {
	return &ServiceError{Code: ServiceErrorValidation, Message: message, Cause: cause}
}

func NewUnauthorizedError(message string, cause error) error {
	return &ServiceError{Code: ServiceErrorUnauthorized, Message: message, Cause: cause}
}

func NewNotFoundError(message string, cause error) error {
	return &ServiceError{Code: ServiceErrorNotFound, Message: message, Cause: cause}
}

func NewInternalError(message string, cause error) error {
	return &ServiceError{Code: ServiceErrorInternal, Message: message, Cause: cause}
}
