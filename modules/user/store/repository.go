package store

import storeimpl "github.com/observability/observability-backend-go/modules/user/store/impl"

type MySQLProvider = storeimpl.MySQLProvider
type MySQLUserTable = storeimpl.MySQLUserTable
type MySQLTeamTable = storeimpl.MySQLTeamTable

var NewMySQLProvider = storeimpl.NewMySQLProvider
