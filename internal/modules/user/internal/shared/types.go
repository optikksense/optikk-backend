package shared

import (
	"fmt"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
)

type TeamMembership struct {
	TeamID int64  `json:"team_id"`
	Role   string `json:"role"`
}

// AuthUser is scanned directly by sqlx from the users table.
type AuthUser struct {
	ID           int64   `db:"id"`
	Email        string  `db:"email"`
	PasswordHash *string `db:"password_hash"`
	Name         string  `db:"name"`
	AvatarURL    *string `db:"avatar_url"`
	TeamsJSON    *string `db:"teams"`
}

// UserRecord is scanned directly by sqlx from the users table.
type UserRecord struct {
	ID          int64      `db:"id"`
	Email       string     `db:"email"`
	Name        string     `db:"name"`
	AvatarURL   *string    `db:"avatar_url"`
	TeamsJSON   *string    `db:"teams"`
	Active      bool       `db:"active"`
	LastLoginAt *time.Time `db:"last_login_at"`
	CreatedAt   time.Time  `db:"created_at"`
}

// TeamRecord is scanned directly by sqlx from the teams table.
type TeamRecord struct {
	ID          int64      `db:"id"`
	OrgName     string     `db:"org_name"`
	Name        string     `db:"name"`
	Slug        string     `db:"slug"`
	Description *string    `db:"description"`
	Active      bool       `db:"active"`
	Color       string     `db:"color"`
	Icon        *string    `db:"icon"`
	APIKey      string     `db:"api_key"`
	CreatedAt   time.Time  `db:"created_at"`
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
