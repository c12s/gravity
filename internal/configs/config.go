package configs

import (
	"os"
)

type Config struct {
	tokenKey        string
	magnetarAddress string
}

func (c *Config) TokenKey() string {
	return c.tokenKey
}

func (c *Config) MagnetarAddress() string {
	return c.magnetarAddress
}

func NewFromEnv() (*Config, error) {
	return &Config{
		tokenKey:        os.Getenv("SECRET_KEY"),
		magnetarAddress: os.Getenv("MAGNETAR_ADDRESS"),
	}, nil
}
