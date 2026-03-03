package config

import "time"

type WalConfig struct {
	Path string
	MaxBatchBytes int
	MaxBatchDelay time.Duration
}

type HttpConfig struct {
	Port string
}

type Config struct {
	HttpConfig HttpConfig
	WalConfig WalConfig
}

func NewConfig() *Config {
	// TODO dotenv
	return &Config{
		HttpConfig: HttpConfig{
			Port: "8080",
		},
		WalConfig: WalConfig{
			Path: "./wal.log",
			MaxBatchBytes: 1024 * 1024, // 1MB
			MaxBatchDelay: 100 * time.Millisecond, // 100ms
		},
	}
}