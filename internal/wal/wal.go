package wal

import (
	"fmt"
	"os"

	"github.com/rassulmurat/lsm-kv-db/internal/config"
)

type WAL struct {
	ch chan walReq
	stopped chan struct{}
}

func New(cfg config.WalConfig) (*WAL, error) {
	fd, err := os.OpenFile(cfg.Path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	ch := make(chan walReq, 2048)
	stopped := make(chan struct{})

	go writerLoop(fd, ch, stopped, cfg)

	return &WAL{
		ch: ch,
		stopped: stopped,
	}, nil
}

func (w *WAL) Put(key string, value string, opts ...Option) error {
	options := Options{
		sync: true,
	}
	for _, opt := range opts {
		opt(&options)
	}

	w.ch <- walReq{
		rec:  []byte(fmt.Sprintf("%s:%s", key, value)),
		sync: options.sync,
		done: make(chan error),
	}

	return nil
}

func (w *WAL) Close() error {
	close(w.ch)
	<-w.stopped
	return nil
}

type walReq struct {
    rec  []byte
    sync bool
    done chan error
}

type Option func(*Options)

type Options struct {
	sync bool
}

func WithSync(sync bool) Option {
	return func(o *Options) {
		o.sync = sync
	}
}