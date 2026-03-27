package contracts

type TenantContext struct {
	TeamID    int64
	UserID    int64
	UserEmail string
	UserRole  string
}
