package nats

// createUserCommand is the payload consumed from the registration saga create
// user subject.
type createUserCommand struct {
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	SagaID    string `json:"saga_id,omitempty"`
}

// deleteUserCommand is the payload consumed from the registration saga delete
// user subject.
type deleteUserCommand struct {
	SessionID string `json:"session_id,omitempty"`
	UserID    string `json:"user_id"`
	SagaID    string `json:"saga_id,omitempty"`
}
