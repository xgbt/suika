package server

import (
	"context"
	"time"

	"suika/internal/biz"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/go-kratos/kratos/v3/transport"
)

// stopWaitTimeout bounds how long Stop waits for the recorder main loop
// to drain after cancelling it. It covers the biz finishGracePeriod (the
// detached FinishSession/remux work triggered by cancellation) plus slack.
const stopWaitTimeout = 45 * time.Second

var _ transport.Server = (*Daemon)(nil)

type Daemon struct {
	recorder *biz.RecorderUsecase

	cancel context.CancelFunc
	done   chan struct{}
}

func NewDaemon(recorder *biz.RecorderUsecase) *Daemon {
	return &Daemon{recorder: recorder}
}

func (d *Daemon) Start(ctx context.Context) error {
	rctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	d.cancel = cancel
	d.done = done

	go func() {
		defer close(done)
		if err := d.recorder.Run(rctx); err != nil {
			log.Error("recorder ended with error", "err", err)
			return
		}
		log.Info("recorder exited")
	}()

	log.Info("daemon started")
	return nil
}

func (d *Daemon) Stop(ctx context.Context) error {
	if d.cancel == nil {
		return nil
	}
	d.cancel()
	select {
	case <-d.done:
		log.Info("daemon stopped")
	case <-ctx.Done():
		log.Warn("daemon stop interrupted, continuing shutdown", "err", ctx.Err())
	case <-time.After(stopWaitTimeout):
		log.Warn("daemon stop timed out, continuing shutdown", "timeout", stopWaitTimeout)
	}
	return nil
}
