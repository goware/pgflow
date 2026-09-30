package workflow

type startConfig struct {
	idempotencyKey string
}

func resolveStart(opts []StartOption) startConfig {
	var cfg startConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// StartOption configures Start and StartTx.
type StartOption func(*startConfig)

// WithIdempotencyKey returns the existing run for a duplicate key untouched,
// so a parked run stays parked.
func WithIdempotencyKey(key string) StartOption {
	return func(c *startConfig) { c.idempotencyKey = key }
}
