package wal

import (
	"os"
	"time"

	"github.com/rassulmurat/lsm-kv-db/internal/config"
)

func writerLoop(fd *os.File, ch chan walReq, stopped chan struct{}, cfg config.WalConfig) {
	defer close(stopped)

    var (
        batchReqs   []walReq
        batchBytes  int
        needSync    bool
    )

    for {
        req, ok := <-ch
        if !ok {
            return
        }

        batchReqs = batchReqs[:0]
        batchBytes = 0
        needSync = false

        batchReqs = append(batchReqs, req)
        batchBytes += len(req.rec)
        needSync = needSync || req.sync

        timer := time.NewTimer(cfg.MaxBatchDelay)

    gather:
        for batchBytes < cfg.MaxBatchBytes {
            select {
            case req2, ok := <-ch:
                if !ok {
                    timer.Stop()
                    break gather
                }

                batchReqs = append(batchReqs, req2)
                batchBytes += len(req2.rec)
                needSync = needSync || req2.sync

            case <-timer.C:
                break gather
            }
        }
        timer.Stop()

        err := writeBatch(fd, batchReqs)

        if err == nil && needSync {
            err = fd.Sync()
        }

        for _, r := range batchReqs {
            if r.done != nil {
                r.done <- err
                close(r.done)
            }
        }

        // 7) если канал закрыли во время gather — тут можно либо:
        // - продолжить цикл и он на следующем чтении выйдет,
        // - или если ok==false поймали — выйти сразу после ack.
        // (См. выше: мы выходим через return, когда ok==false на первом чтении)
    }
}

func writeBatch(fd *os.File, reqs []walReq) error {
    for _, r := range reqs {
        if _, err := fd.Write(r.rec); err != nil {
            return err
        }
    }
    return nil
}