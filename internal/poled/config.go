package poled

type Config struct {
	IndexPath string
}

func DefaultConfig() *Config {
	return &Config{IndexPath: "/tmp/pole"}
}
