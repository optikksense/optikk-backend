package interfaces

import "fmt"

// AuthService encapsulates authentication business rules.
type AuthService interface {
	Login(email, password string) (map[string]any, error)
	BuildAuthContext(userID int64) (map[string]any, error)
}

// UserService encapsulates user/team/profile business rules.
type UserService interface {
	GetCurrentUser(userID int64) (map[string]any, error)
	GetUsers(organizationID int64, limit, offset int) ([]map[string]any, error)
	GetUserByID(userID int64) (map[string]any, error)
	CreateUser(input CreateUserInput) (map[string]any, error)
	Signup(input SignupInput) (map[string]any, error)
	AddUserToTeam(userID, teamID int64, role string) error
	RemoveUserFromTeam(userID, teamID int64) error
	GetTeams(organizationID int64) ([]map[string]any, error)
	GetMyTeams(userID int64) ([]map[string]any, error)
	GetTeamByID(teamID int64) (map[string]any, error)
	GetTeamBySlug(organizationID int64, slug string) (map[string]any, error)
	CreateTeam(input CreateTeamInput) (map[string]any, error)
	GetProfile(userID int64) (map[string]any, error)
	UpdateProfile(input UpdateProfileInput) (map[string]any, error)
}

type CreateUserInput struct {
	OrganizationID int64
	Email          string
	Name           string
	Role           string
	Password       string
}

type SignupInput struct {
	Email              string
	Name               string
	Password           string
	TeamName           string
	OrganizationID     *int64
	TenantOrganization int64
}

type CreateTeamInput struct {
	OrganizationID int64
	Name           string
	Slug           string
	Description    string
	Color          string
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
