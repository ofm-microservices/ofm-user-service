package config

// GRPCConfig defines the internal gRPC listener exposed by user-service.
type GRPCConfig struct {
	Host string `env:"GRPC_HOST" envDefault:"0.0.0.0"`
	Port int    `env:"GRPC_PORT" envDefault:"9092"`
}
