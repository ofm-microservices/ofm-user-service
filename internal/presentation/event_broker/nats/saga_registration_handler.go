package nats

import (
	"context"
	"encoding/json"
)

func (s *registrationSagaSubscriber) handleCreateUserCommand(ctx context.Context, _ string, payload []byte) error {
	var cmd createUserCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return WrapUnmarshalCreateUserCommandError(err)
	}

	_, err := s.service.CreateUser(ctx, cmd.UserID, cmd.Username, cmd.FirstName, cmd.LastName)
	if err != nil {
		return s.publishCreateFailureResult(ctx, cmd, err)
	}

	return s.publishCreateSuccessResult(ctx, cmd)
}

func (s *registrationSagaSubscriber) handleDeleteUserCommand(ctx context.Context, _ string, payload []byte) error {
	var cmd deleteUserCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return WrapUnmarshalDeleteUserCommandError(err)
	}

	if err := s.service.DeleteUser(ctx, cmd.UserID); err != nil {
		return s.publishDeleteFailureResult(ctx, cmd, err)
	}

	return s.publishDeleteSuccessResult(ctx, cmd)
}

func (s *registrationSagaSubscriber) publishCreateFailureResult(ctx context.Context, cmd createUserCommand, err error) error {
	payload, mapErr := s.mapr.ToCreateFailureResultPayload(cmd, s.resolver.CreateUserFailureReason(err))
	if mapErr != nil {
		return mapErr
	}

	return s.broker.Publish(ctx, s.cfg.SagaCreateUserResultSubject, payload)
}

func (s *registrationSagaSubscriber) publishCreateSuccessResult(ctx context.Context, cmd createUserCommand) error {
	resultPayload, err := s.mapr.ToCreateSuccessResultPayload(cmd)
	if err != nil {
		return err
	}

	return s.broker.Publish(ctx, s.cfg.SagaCreateUserResultSubject, resultPayload)
}

func (s *registrationSagaSubscriber) publishDeleteFailureResult(ctx context.Context, cmd deleteUserCommand, err error) error {
	payload, mapErr := s.mapr.ToDeleteFailureResultPayload(cmd, s.resolver.DeleteUserFailureReason(err))
	if mapErr != nil {
		return mapErr
	}

	return s.broker.Publish(ctx, s.cfg.SagaDeleteUserResultSubject, payload)
}

func (s *registrationSagaSubscriber) publishDeleteSuccessResult(ctx context.Context, cmd deleteUserCommand) error {
	resultPayload, err := s.mapr.ToDeleteSuccessResultPayload(cmd)
	if err != nil {
		return err
	}

	return s.broker.Publish(ctx, s.cfg.SagaDeleteUserResultSubject, resultPayload)
}
