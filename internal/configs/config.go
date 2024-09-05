package configs

import (
	"os"
)

type Config struct {
	tokenKey string
}

func (c *Config) TokenKey() string {
	return c.tokenKey
}

func NewFromEnv() (*Config, error) {
	return &Config{
		tokenKey: os.Getenv("SECRET_KEY"),
	}, nil
}
