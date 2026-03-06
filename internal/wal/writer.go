package wal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type WALWriter struct {
    dirPath string
    options WriteOptions
	ch chan WALWriteEvent
	stopped chan struct{}
}

func (w *WALWriter) Put(seq uint64, key string, value string, opts ...WriteReqOption) error {
	options := WriteReqOptions{
		sync: true,
	}
	for _, opt := range opts {
		opt(&options)
	}

	rec, err := newWalRecord(seq, PUT, []byte(key), []byte(value))
	if err != nil {
		return err
	}
	recBytes, err := rec.encode()
	if err != nil {
		return err
	}
    req := walReq{
		rec:        recBytes,
		reqOpts:    options,
		done:       make(chan error, 1),
	}
	w.ch<-req
	return <-req.done
}

func (w *WALWriter) writerLoop(fd *os.File) {
    bufWriter := bufio.NewWriterSize(fd, 64*1024) // 64KB buffer
	defer bufWriter.Flush()
	defer close(w.stopped)

    var (
        batchReqs       []walReq
        batchBytes      int
        needSync        bool
        needRotate      bool
        rotateOrdr      rotateOrder
    )
    timer := time.NewTimer(w.options.maxBatchDelay)
    defer timer.Stop()

    for {
        req, ok := <- w.ch
        if !ok {
            return
        }

        batchReqs = batchReqs[:0]
        batchBytes = 0
        needSync = false
        if !timer.Stop() {
            <-timer.C
        }
        timer.Reset(w.options.maxBatchDelay)

        switch req := req.(type) {
        case walReq:
            batchReqs = append(batchReqs, req)
            batchBytes += len(req.rec)
            needSync = needSync || req.reqOpts.sync
        case rotateOrder:
            needRotate = true
            rotateOrdr = req
            continue
        default:
            panic(fmt.Sprintf("unexpected event type: %T", req))
        }

    gather:
        for batchBytes < w.options.maxBatchBytes {
            select {
            case req, ok := <-w.ch:
                if !ok {
                    if !timer.Stop() {
                        <-timer.C
                    }
                    break gather
                }

                switch req := req.(type) {
                case walReq:
                    batchReqs = append(batchReqs, req)
                    batchBytes += len(req.rec)
                    needSync = needSync || req.reqOpts.sync
                case rotateOrder:
                    needRotate = true
                    rotateOrdr = req
                    break gather
                default:
                    panic(fmt.Sprintf("unexpected event type: %T", req))
                }

            case <-timer.C:
                break gather
            }
        }
        if !timer.Stop() {
            <-timer.C
        }

        err := writeBatch(bufWriter, batchReqs)

        if err == nil {
            err = bufWriter.Flush()
        }

        if err == nil && needSync {
            err = fd.Sync()
        }

        for _, r := range batchReqs {
            if r.done != nil {
                r.done <- err
                close(r.done)
            }
        }

        if needRotate {
            if fd != nil {
                fd.Sync()
                fd.Close()
            }
            fd = rotateOrdr.newFd
            bufWriter.Reset(fd)
            rotateOrdr.done <- nil
            needRotate = false
            rotateOrdr = rotateOrder{}
        }
    }
}

func writeBatch(w *bufio.Writer, reqs []walReq) error {
    for _, r := range reqs {
        if _, err := w.Write(r.rec); err != nil {
            return err
        }
    }
    return nil
}

func (w *WALWriter) Rotate (newSeq uint64) error {
    filePath := filepath.Join(w.dirPath, fmt.Sprintf(walFileFormat, newSeq))
    fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    if err != nil {
        return err
    }
    rtOrdr := rotateOrder{
        newFd: fd,
        done: make(chan error, 1),
    }
    w.ch <- rtOrdr
    timer := time.NewTimer(500*time.Millisecond)
    select {
    case err := <-rtOrdr.done:
        return err
    case <-timer.C:
        return fmt.Errorf("Timed out while waiting for WAL rotation")
    }
}

func (w *WALWriter) Close() error {
    close(w.ch)
    <-w.stopped
    return nil
}

type WALWriteEvent interface {
    isWALWriteEvent()
}

type walReq struct {
    rec         []byte
    reqOpts     WriteReqOptions
    done        chan error
}
func (r walReq) isWALWriteEvent() {}
type rotateOrder struct {
    newFd *os.File
    done  chan error
}
func (r rotateOrder) isWALWriteEvent() {}

func WithMaxBatchBytes(maxBatchBytes int) WriteOption {
	return func(o *WriteOptions) {
		o.maxBatchBytes = maxBatchBytes
	}
}

func WithMaxBatchDelay(maxBatchDelay time.Duration) WriteOption {
	return func(o *WriteOptions) {
		o.maxBatchDelay = maxBatchDelay
	}
}

type WriteOption func(*WriteOptions)
type WriteOptions struct {
	maxBatchBytes int
	maxBatchDelay time.Duration
}

type WriteReqOptions struct {
    sync bool
}
type WriteReqOption func(*WriteReqOptions)
func WithSync(sync bool) WriteReqOption {
	return func(o *WriteReqOptions) {
		o.sync = sync
	}
}

