package config

import (
	"os"
)

type Config struct {
	DNS  string
	Host string
	Port string
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		DNS:  os.Getenv("DNS"),
		Host: os.Getenv("HOST"),
		Port: os.Getenv("PORT"),
	}

	return cfg, nil
}
