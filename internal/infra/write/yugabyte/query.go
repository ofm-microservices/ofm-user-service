package repository

const (
	createUserQuery = `
		INSERT INTO users (user_id, username, first_name, last_name)
		VALUES ($1, $2, $3, $4)
		RETURNING user_id, username, first_name, last_name, created_at, updated_at
	`

	getUserByID = `
		SELECT user_id, username, first_name, last_name, created_at, updated_at
		FROM users
		WHERE user_id = $1
	`

	existsByUsernameQuery = `
		SELECT EXISTS (
			SELECT 1
			FROM users
			WHERE username = $1
		)
	`

	deleteUserByID = `
		DELETE FROM users
		WHERE user_id = $1
	`
)
