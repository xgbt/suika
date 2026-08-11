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

var _ transport.Server = (*RecorderJob)(nil)

// RecorderJob wraps the recorder daemon (biz.RecorderUsecase.Run) as a
// kratos transport.Server so the app lifecycle starts and stops it
// alongside the HTTP/gRPC servers.
type RecorderJob struct {
	uc *biz.RecorderUsecase

	cancel context.CancelFunc
	done   chan struct{}
}

// NewRecorderJob new a RecorderJob.
func NewRecorderJob(uc *biz.RecorderUsecase) *RecorderJob {
	return &RecorderJob{uc: uc}
}

// Start launches the recorder main loop in a goroutine and returns
// immediately. The loop runs on a context derived from context.Background:
// kratos only requires Start to return once the server is accepting work,
// and the daemon must keep running afterwards, so the loop must not be
// tied to a context that may be cancelled when Start returns.
func (j *RecorderJob) Start(ctx context.Context) error {
	rctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	j.cancel = cancel
	j.done = done
	go func() {
		defer close(done)
		if err := j.uc.Run(rctx); err != nil {
			log.Error("recorder job ended with error", "err", err)
			return
		}
		log.Info("recorder job exited")
	}()
	log.Info("recorder job started")
	return nil
}

// Stop cancels the recorder main loop and waits for it to exit. The wait
// is bounded by both the given context (kratos may attach a stop timeout)
// and a fallback timeout so shutdown can never wedge.
func (j *RecorderJob) Stop(ctx context.Context) error {
	if j.cancel == nil {
		return nil
	}
	j.cancel()
	select {
	case <-j.done:
		log.Info("recorder job stopped")
	case <-ctx.Done():
		log.Warn("recorder job stop interrupted, continuing shutdown", "err", ctx.Err())
	case <-time.After(stopWaitTimeout):
		log.Warn("recorder job stop timed out, continuing shutdown", "timeout", stopWaitTimeout)
	}
	return nil
}
