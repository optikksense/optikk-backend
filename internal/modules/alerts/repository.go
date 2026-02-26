package alerts

import repositoryimpl "github.com/observability/observability-backend-go/internal/modules/alerts/repository/impl"

type Repository = repositoryimpl.Repository

var NewRepository = repositoryimpl.NewRepository
