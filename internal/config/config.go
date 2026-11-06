package config

import (
	"os"
)

// Config holds runtime configuration loaded from environment variables.
type Config struct {
	Port        string
	DatabaseURL string
	LogLevel    string
	Env         string
	AllowLocal  bool
}

// Load loads configuration from environment variables with sensible defaults.
func Load() *Config {
	env := getEnv("ENV", "development")
	// Allow local addresses by default in development mode; block in production
	allowLocalDefault := env == "development"
	if val := os.Getenv("HOOKRELAY_ALLOW_LOCAL"); val != "" {
		allowLocalDefault = val == "true" || val == "1"
	}

	return &Config{
		Port:        getEnv("PORT", "8080"),
		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres@localhost:5432/hookrelay?sslmode=disable"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
		Env:         env,
		AllowLocal:  allowLocalDefault,
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
