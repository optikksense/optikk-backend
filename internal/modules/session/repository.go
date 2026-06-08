package session

import (
	"database/sql"

	"github.com/alexedwards/scs/mysqlstore"
)

// Repository wraps the MySQL session store.
type Repository struct {
	Store *mysqlstore.MySQLStore
}

// NewRepository creates a new MySQL session repository.
func NewRepository(db *sql.DB) *Repository {
	return &Repository{
		Store: mysqlstore.New(db),
	}
}
