package model

import "time"

// UserRow is the Yugabyte persistence model for the user write model.
type UserRow struct {
	ID        string    `db:"user_id"`
	Username  string    `db:"username"`
	FirstName string    `db:"first_name"`
	LastName  string    `db:"last_name"`
	AvatarID  string    `db:"avatar_id"`
	IsActive  bool      `db:"is_active"`
	Status    string    `db:"status"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}
