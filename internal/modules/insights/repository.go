package insights

import repositoryimpl "github.com/observability/observability-backend-go/internal/modules/insights/repository/impl"

type Repository = repositoryimpl.Repository

var NewRepository = repositoryimpl.NewRepository
