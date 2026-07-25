package config

import "time"

// NATSConfig defines NATS streams, subjects, and pull-consumer settings used
// by user-service.
type NATSConfig struct {
	URL                              string        `env:"NATS_URL,required"`
	User                             string        `env:"NATS_USER"`
	Password                         string        `env:"NATS_PASSWORD"`
	UserEventsStream                 string        `env:"NATS_STREAM_USER_EVENTS" envDefault:"USER_EVENTS"`
	UserCreatedSubject               string        `env:"NATS_SUBJECT_USER_CREATED" envDefault:"user.created"`
	UserDetailedStream               string        `env:"NATS_STREAM_USER_DETAILED" envDefault:"USER_DETAILED"`
	UserDetailedRequestedSubject     string        `env:"NATS_SUBJECT_USER_DETAILED_REQUESTED" envDefault:"user.detailed.requested"`
	UserDetailedProjectionSubject    string        `env:"NATS_SUBJECT_USER_DETAILED_PROJECTION_REQUESTED" envDefault:"user.detailed.projection.requested"`
	UserDetailedProjectionDurable    string        `env:"NATS_DURABLE_USER_DETAILED_PROJECTION" envDefault:"user_service_user_detailed_projection"`
	UserDetailedProjectionBatchSize  int           `env:"NATS_USER_DETAILED_PROJECTION_BATCH_SIZE" envDefault:"50"`
	UserDetailedProjectionMaxWait    time.Duration `env:"NATS_USER_DETAILED_PROJECTION_MAX_WAIT" envDefault:"500ms"`
	UserDetailedProjectionWorkers    int           `env:"NATS_USER_DETAILED_PROJECTION_WORKERS" envDefault:"4"`
	UserDetailedProjectionQueueSize  int           `env:"NATS_USER_DETAILED_PROJECTION_QUEUE_SIZE" envDefault:"200"`
	UserDetailedProjectionAckWait    time.Duration `env:"NATS_USER_DETAILED_PROJECTION_ACK_WAIT" envDefault:"30s"`
	UserDetailedProjectionMaxDeliver int           `env:"NATS_USER_DETAILED_PROJECTION_MAX_DELIVER" envDefault:"5"`
	SagaCommandsStream               string        `env:"NATS_STREAM_SAGA_COMMANDS" envDefault:"SAGA_USER_COMMANDS"`
	SagaCreateUserSubject            string        `env:"NATS_SUBJECT_SAGA_CREATE_USER" envDefault:"saga.user.create"`
	SagaDeleteUserSubject            string        `env:"NATS_SUBJECT_SAGA_DELETE_USER" envDefault:"saga.user.delete"`
	SagaCreateUserResultSubject      string        `env:"NATS_SUBJECT_SAGA_CREATE_USER_RESULT" envDefault:"saga.user.create.result"`
	SagaDeleteUserResultSubject      string        `env:"NATS_SUBJECT_SAGA_DELETE_USER_RESULT" envDefault:"saga.user.delete.result"`
	SagaCreateUserDurable            string        `env:"NATS_DURABLE_SAGA_CREATE_USER" envDefault:"user_service_saga_create"`
	SagaDeleteUserDurable            string        `env:"NATS_DURABLE_SAGA_DELETE_USER" envDefault:"user_service_saga_delete"`
	SagaBatchSize                    int           `env:"NATS_SAGA_BATCH_SIZE" envDefault:"100"`
	SagaMaxWait                      time.Duration `env:"NATS_SAGA_MAX_WAIT" envDefault:"500ms"`
	SagaWorkers                      int           `env:"NATS_SAGA_WORKERS" envDefault:"8"`
	SagaQueueSize                    int           `env:"NATS_SAGA_QUEUE_SIZE" envDefault:"500"`
	SagaAckWait                      time.Duration `env:"NATS_SAGA_ACK_WAIT" envDefault:"30s"`
	SagaMaxDeliver                   int           `env:"NATS_SAGA_MAX_DELIVER" envDefault:"5"`
	SagaAdaptiveEnabled              bool          `env:"NATS_SAGA_ADAPTIVE_ENABLED" envDefault:"false"`
	SagaAdaptiveCheckInterval        time.Duration `env:"NATS_SAGA_ADAPTIVE_CHECK_INTERVAL" envDefault:"2s"`
	SagaAdaptiveMediumPending        int           `env:"NATS_SAGA_ADAPTIVE_MEDIUM_PENDING" envDefault:"200"`
	SagaAdaptiveHighPending          int           `env:"NATS_SAGA_ADAPTIVE_HIGH_PENDING" envDefault:"1000"`
	SagaAdaptiveLowBatchSize         int           `env:"NATS_SAGA_ADAPTIVE_LOW_BATCH_SIZE" envDefault:"25"`
	SagaAdaptiveLowMaxWait           time.Duration `env:"NATS_SAGA_ADAPTIVE_LOW_MAX_WAIT" envDefault:"1s"`
	SagaAdaptiveMediumBatchSize      int           `env:"NATS_SAGA_ADAPTIVE_MEDIUM_BATCH_SIZE" envDefault:"100"`
	SagaAdaptiveMediumMaxWait        time.Duration `env:"NATS_SAGA_ADAPTIVE_MEDIUM_MAX_WAIT" envDefault:"500ms"`
	SagaAdaptiveHighBatchSize        int           `env:"NATS_SAGA_ADAPTIVE_HIGH_BATCH_SIZE" envDefault:"300"`
	SagaAdaptiveHighMaxWait          time.Duration `env:"NATS_SAGA_ADAPTIVE_HIGH_MAX_WAIT" envDefault:"100ms"`
}
