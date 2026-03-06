package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func NewWALWriter(dirPath string, seq uint64, opts ...WriteOption) (*WALWriter, error) {
	options := WriteOptions{
		maxBatchBytes: 1024 * 1024, // 1MB
		maxBatchDelay: 100 * time.Millisecond, // 100ms
	}
	for _, opt := range opts {
		opt(&options)
	}
	w := WALWriter{
		dirPath: dirPath,
		options: options,
		ch: make(chan WALWriteEvent, 1024),
		stopped: make(chan struct{}),
	}

	filePath := filepath.Join(w.dirPath, fmt.Sprintf(walFileFormat, seq))
    fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    if err != nil {
        return &WALWriter{},err
    }
	go w.writerLoop(fd)

	return &w, nil
}

const walFileFormat = "%016d.wal"

type Operations uint8

const (
	PUT Operations = iota + 1
	DEL
)

func (o Operations) String() string {
	names := [...]string{"", "PUT", "DEL"}
	if int(o) < len(names) {
		return names[o]
	}
	return "UNKNOWN"
}