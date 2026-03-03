package database

import (
	"github.com/rassulmurat/lsm-kv-db/internal/config"
	"github.com/rassulmurat/lsm-kv-db/internal/wal"
)

type Engine struct {
	wal *wal.WAL
}

func NewEngine(config *config.Config) *Engine {
	wal, err := wal.New(config.WalConfig)
	if err != nil {
		panic(err)
	}
	return &Engine{
		wal: wal,
	}
}

func (e *Engine) Put(key string, value string) error {
	e.wal.Put(key, value)
	return nil
}
