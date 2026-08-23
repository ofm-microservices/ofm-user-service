package config

// KafkaConfig defines the Kafka broker and consumer group used by user-service.
type KafkaConfig struct {
	Brokers                     []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:"127.0.0.1:9092"`
	GroupID                     string   `env:"KAFKA_USER_GROUP_ID" envDefault:"user-service"`
	DetailedUserRequestedTopic  string   `env:"KAFKA_USER_DETAILED_REQUESTED_TOPIC" envDefault:"user.detailed.requested"`
	DetailedUserProjectionTopic string   `env:"KAFKA_USER_DETAILED_PROJECTION_TOPIC" envDefault:"user.detailed.projection.requested"`
	SagaCreateUserTopic         string   `env:"KAFKA_USER_SAGA_CREATE_TOPIC" envDefault:"saga.user.create"`
	SagaDeleteUserTopic         string   `env:"KAFKA_USER_SAGA_DELETE_TOPIC" envDefault:"saga.user.delete"`
	SagaCreateResultTopic       string   `env:"KAFKA_USER_SAGA_CREATE_RESULT_TOPIC" envDefault:"saga.user.create.result"`
	SagaDeleteResultTopic       string   `env:"KAFKA_USER_SAGA_DELETE_RESULT_TOPIC" envDefault:"saga.user.delete.result"`
}
