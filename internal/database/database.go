package database

import (
	"sync/atomic"

	"github.com/rassulmurat/lsm-kv-db/internal/config"
	"github.com/rassulmurat/lsm-kv-db/internal/wal"
)

type Engine struct {
	walWriter *wal.WALWriter
	lastSeq uint64
}

func NewEngine(config *config.Config) *Engine {
	walWriter, err := wal.NewWALWriter(
		config.WalConfig.DirPath, 1,
		wal.WithMaxBatchBytes(config.WalConfig.MaxBatchBytes),
		wal.WithMaxBatchDelay(config.WalConfig.MaxBatchDelay),
	)
	if err != nil {
		panic(err)
	}
	return &Engine{
		walWriter: walWriter,
		// TODO: get seq from wal
		lastSeq: 0,
	}
}

func (e *Engine) Put(key string, value string) error {
	seq := atomic.AddUint64(&e.lastSeq, 1)

	if err := e.walWriter.Put(seq, key, value); err != nil {
		return err
	}
	return nil
}

func (e *Engine) Get(key string) (string, error) {
	// rec, err := e.wal.Get(key)
	// if err != nil {
	// 	return "", err
	// }
	// return string(rec.val), nil
	return "", nil
}
