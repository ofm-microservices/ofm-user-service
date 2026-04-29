package nats

import (
	"encoding/json"
	"time"
)

type registrationSagaMessageMapper struct{}

func newRegistrationSagaMessageMapper() RegistrationSagaMessageMapper {
	return &registrationSagaMessageMapper{}
}

func (m *registrationSagaMessageMapper) ToCreateFailureResultPayload(cmd createUserCommand, reason string) ([]byte, error) {
	payload, err := json.Marshal(createUserResult{
		SessionID: cmd.SessionID,
		UserID:    cmd.UserID,
		SagaID:    cmd.SagaID,
		Status:    "failed",
		Error:     reason,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, WrapMarshalCreateUserResultError(err)
	}

	return payload, nil
}

func (m *registrationSagaMessageMapper) ToCreateSuccessResultPayload(cmd createUserCommand) ([]byte, error) {
	payload, err := json.Marshal(createUserResult{
		SessionID: cmd.SessionID,
		UserID:    cmd.UserID,
		SagaID:    cmd.SagaID,
		Status:    "success",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, WrapMarshalCreateUserResultError(err)
	}

	return payload, nil
}

func (m *registrationSagaMessageMapper) ToDeleteFailureResultPayload(cmd deleteUserCommand, reason string) ([]byte, error) {
	payload, err := json.Marshal(deleteUserResult{
		SessionID: cmd.SessionID,
		UserID:    cmd.UserID,
		SagaID:    cmd.SagaID,
		Status:    "failed",
		Error:     reason,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, WrapMarshalDeleteUserResultError(err)
	}

	return payload, nil
}

func (m *registrationSagaMessageMapper) ToDeleteSuccessResultPayload(cmd deleteUserCommand) ([]byte, error) {
	payload, err := json.Marshal(deleteUserResult{
		SessionID: cmd.SessionID,
		UserID:    cmd.UserID,
		SagaID:    cmd.SagaID,
		Status:    "success",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, WrapMarshalDeleteUserResultError(err)
	}

	return payload, nil
}
