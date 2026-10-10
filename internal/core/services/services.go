package services

// Config holds the repository dependencies required to initialise all services.
type Config struct{}

// Services aggregates all domain services.
type Services struct{}

// New wires all services using the provided repository config.
func New(config *Config) *Services {
	return &Services{}
}
