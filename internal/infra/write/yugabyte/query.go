package repository

const (
	createUserQuery = `
		INSERT INTO users (user_id, username, first_name, last_name, avatar_id, status)
		VALUES ($1, $2, $3, $4, $5, 'pending_registration')
		RETURNING user_id, username, first_name, last_name, avatar_id, is_active, status, created_at, updated_at
	`

	getUserByID = `
		SELECT user_id, username, first_name, last_name, avatar_id, is_active, status, created_at, updated_at
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

	activateUserByID = `
		UPDATE users
		SET is_active = TRUE,
			status = 'active',
			updated_at = NOW()
		WHERE user_id = $1
		RETURNING user_id, username, first_name, last_name, avatar_id, is_active, status, created_at, updated_at
	`

	deactivateUserByID = `
		UPDATE users
		SET is_active = FALSE,
			status = 'registration_failed',
			updated_at = NOW()
		WHERE user_id = $1
	`
)
