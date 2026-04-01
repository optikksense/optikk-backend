package auth

type LoginRequest struct {
	Email    string `json:"email" binding:"required" validate:"required,email" example:"user@example.com"`
	Password string `json:"password" binding:"required" validate:"required" example:"securePassword123"`
}
