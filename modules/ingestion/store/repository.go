package store

import storeimpl "github.com/observability/observability-backend-go/modules/ingestion/store/impl"

type ClickHouseRepository = storeimpl.ClickHouseRepository

var NewRepository = storeimpl.NewRepository
