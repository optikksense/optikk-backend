package service

import serviceinterfaces "github.com/observability/observability-backend-go/modules/user/service/interfaces"

type AuthService = serviceinterfaces.AuthService
type UserService = serviceinterfaces.UserService

type CreateUserInput = serviceinterfaces.CreateUserInput
type SignupInput = serviceinterfaces.SignupInput
type CreateTeamInput = serviceinterfaces.CreateTeamInput
type UpdateProfileInput = serviceinterfaces.UpdateProfileInput

type ServiceErrorCode = serviceinterfaces.ServiceErrorCode
type ServiceError = serviceinterfaces.ServiceError

const (
	ServiceErrorValidation   = serviceinterfaces.ServiceErrorValidation
	ServiceErrorUnauthorized = serviceinterfaces.ServiceErrorUnauthorized
	ServiceErrorNotFound     = serviceinterfaces.ServiceErrorNotFound
	ServiceErrorInternal     = serviceinterfaces.ServiceErrorInternal
)
