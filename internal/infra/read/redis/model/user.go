package model

// UserCache is the Redis projection model for the user read model.
type UserCache struct {
	ID        string `json:"user_id"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	AvatarID  string `json:"avatar_id"`
	IsActive  bool   `json:"is_active"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// UserDetailedCache is the Redis projection model for the detailed public user
// read model.
type UserDetailedCache struct {
	ID          string `json:"user_id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	AvatarID    string `json:"avatar_id"`
	AvatarURL   string `json:"avatar_url"`
	About       string `json:"about"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}
