package config

// FileServiceConfig defines the upstream file-service endpoint used by
// user-service for avatar URL enrichment.
type FileServiceConfig struct {
	Address string `env:"FILE_SERVICE_ADDRESS" envDefault:"127.0.0.1:9503"`
}
