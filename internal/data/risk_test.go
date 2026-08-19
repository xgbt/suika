package data

import (
	"context"
	stderrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"suika/internal/biz"
)

// scriptStep 是 scripted 的一步：返回的业务码与错误。
type scriptStep struct {
	code int
	err  error
}

// scripted 按脚本逐步返回结果；走完脚本后重复最后一步。
type scripted struct {
	calls int
	steps []scriptStep
}

func (s *scripted) run(ctx context.Context) (int, error) {
	step := s.steps[min(s.calls, len(s.steps)-1)]
	s.calls++
	return step.code, step.err
}

func newTestGuard() (*riskGuard, *atomic.Int64) {
	var refreshes atomic.Int64
	return newRiskGuard(func() { refreshes.Add(1) }), &refreshes
}

func httpRiskErr(status int) error {
	return fmt.Errorf("%w: status=%d", errHTTPRiskControl, status)
}

func TestRiskGuardCallSuccess(t *testing.T) {
	g, refreshes := newTestGuard()
	attempt := &scripted{steps: []scriptStep{{code: 0}}}

	code, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run})
	if err != nil || code != 0 {
		t.Fatalf("call = (%d, %v), want (0, nil)", code, err)
	}
	if attempt.calls != 1 || refreshes.Load() != 0 {
		t.Fatalf("calls=%d refreshes=%d, want 1/0", attempt.calls, refreshes.Load())
	}
	if err := g.checkCooldown(1); err != nil {
		t.Fatalf("no cooldown expected, got %v", err)
	}
}

func TestRiskGuardSuccessClearsCooldown(t *testing.T) {
	g, _ := newTestGuard()
	g.noteFailure(1)
	g.cooldowns[1].until = time.Now().Add(-time.Second) // 模拟冷却已过期

	if _, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: (&scripted{steps: []scriptStep{{code: 0}}}).run}); err != nil {
		t.Fatalf("call: %v", err)
	}
	if _, ok := g.cooldowns[1]; ok {
		t.Fatal("cooldown entry should be deleted after success")
	}
	if err := g.checkCooldown(1); err != nil {
		t.Fatalf("cooldown should be cleared after success, got %v", err)
	}
}

func TestRiskGuardCooldownGateBlocksAttempts(t *testing.T) {
	g, refreshes := newTestGuard()
	g.noteFailure(1)
	attempt := &scripted{steps: []scriptStep{{code: 0}}}

	_, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run})
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
	if attempt.calls != 0 || refreshes.Load() != 0 {
		t.Fatalf("calls=%d refreshes=%d, want 0/0 (gated)", attempt.calls, refreshes.Load())
	}
}

func TestRiskGuardPlainErrorPassesThrough(t *testing.T) {
	g, refreshes := newTestGuard()
	plain := stderrors.New("network unreachable")
	attempt := &scripted{steps: []scriptStep{{err: plain}}}

	_, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run})
	if !stderrors.Is(err, plain) {
		t.Fatalf("err = %v, want pass-through of %v", err, plain)
	}
	if refreshes.Load() != 0 {
		t.Fatalf("refreshes=%d, want 0", refreshes.Load())
	}
	if err := g.checkCooldown(1); err != nil {
		t.Fatalf("plain error must not start cooldown, got %v", err)
	}
}

func TestRiskGuardHTTPRiskRetriesOnce(t *testing.T) {
	g, refreshes := newTestGuard()
	attempt := &scripted{steps: []scriptStep{{err: httpRiskErr(412)}, {code: 0}}}

	code, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run})
	if err != nil || code != 0 {
		t.Fatalf("call = (%d, %v), want success after retry", code, err)
	}
	if attempt.calls != 2 || refreshes.Load() != 1 {
		t.Fatalf("calls=%d refreshes=%d, want 2/1", attempt.calls, refreshes.Load())
	}
}

func TestRiskGuardHTTPRiskRetryExhausted(t *testing.T) {
	g, refreshes := newTestGuard()
	attempt := &scripted{steps: []scriptStep{{err: httpRiskErr(412)}}}

	_, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run})
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
	if attempt.calls != 2 || refreshes.Load() != 1 {
		t.Fatalf("calls=%d refreshes=%d, want 2/1", attempt.calls, refreshes.Load())
	}
	if err := g.checkCooldown(1); err == nil {
		t.Fatal("cooldown expected after exhausted risk retry")
	}
}

func TestRiskGuardRiskCode352RetriesOnce(t *testing.T) {
	g, refreshes := newTestGuard()
	attempt := &scripted{steps: []scriptStep{{code: riskCode352}, {code: 0}}}

	code, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run})
	if err != nil || code != 0 {
		t.Fatalf("call = (%d, %v), want success after -352 retry", code, err)
	}
	if attempt.calls != 2 || refreshes.Load() != 1 {
		t.Fatalf("calls=%d refreshes=%d, want 2/1", attempt.calls, refreshes.Load())
	}
}

func TestRiskGuardRiskCode352Exhausted(t *testing.T) {
	g, _ := newTestGuard()
	attempt := &scripted{steps: []scriptStep{{code: riskCode352}}}

	_, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run})
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
	if attempt.calls != 2 {
		t.Fatalf("calls=%d, want 2 (initial + one retry)", attempt.calls)
	}
	if err := g.checkCooldown(1); err == nil {
		t.Fatal("cooldown expected after exhausted -352")
	}
}

func TestRiskGuardRiskCode352RetryErrorClassified(t *testing.T) {
	g, _ := newTestGuard()
	attempt := &scripted{steps: []scriptStep{{code: riskCode352}, {err: httpRiskErr(429)}}}

	_, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run})
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
	if err := g.checkCooldown(1); err == nil {
		t.Fatal("cooldown expected when the -352 retry itself hits risk control")
	}
}

func TestRiskGuardRiskCode352RetryPlainError(t *testing.T) {
	g, _ := newTestGuard()
	plain := stderrors.New("timeout")
	attempt := &scripted{steps: []scriptStep{{code: riskCode352}, {err: plain}}}

	_, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run})
	if !stderrors.Is(err, plain) {
		t.Fatalf("err = %v, want pass-through of %v", err, plain)
	}
	if err := g.checkCooldown(1); err != nil {
		t.Fatalf("plain retry error must not start cooldown, got %v", err)
	}
}

func TestRiskGuardFallbackSuccess(t *testing.T) {
	g, _ := newTestGuard()
	g.noteFailure(1) // 预置已过期的冷却，验证成功后条目被删除
	g.cooldowns[1].until = time.Now().Add(-time.Second)
	attempt := &scripted{steps: []scriptStep{{code: riskCode352}}}
	fallback := &scripted{steps: []scriptStep{{code: 0}}}

	code, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run, fallback: fallback.run})
	if err != nil || code != 0 {
		t.Fatalf("call = (%d, %v), want fallback success", code, err)
	}
	if attempt.calls != 2 || fallback.calls != 1 {
		t.Fatalf("attempts=%d fallbacks=%d, want 2/1", attempt.calls, fallback.calls)
	}
	if _, ok := g.cooldowns[1]; ok {
		t.Fatal("cooldown entry should be deleted after fallback success")
	}
}

func TestRiskGuardFallbackFailure(t *testing.T) {
	g, _ := newTestGuard()
	attempt := &scripted{steps: []scriptStep{{code: riskCode352}}}
	fallback := &scripted{steps: []scriptStep{{err: stderrors.New("getConf code=-352")}}}

	_, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run, fallback: fallback.run})
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
	if err := g.checkCooldown(1); err == nil {
		t.Fatal("cooldown expected after fallback failure")
	}
}

func TestRiskGuardFallbackNonZeroCode(t *testing.T) {
	g, _ := newTestGuard()
	attempt := &scripted{steps: []scriptStep{{code: riskCode352}}}
	fallback := &scripted{steps: []scriptStep{{code: 5}}}

	_, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run, fallback: fallback.run})
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
}

func TestRiskGuardNonZeroCodeNoBookkeeping(t *testing.T) {
	g, refreshes := newTestGuard()
	g.noteFailure(1) // 预置已过期的冷却条目
	g.cooldowns[1].until = time.Now().Add(-time.Second)
	attempt := &scripted{steps: []scriptStep{{code: -400}}}

	code, err := g.call(context.Background(), 1, riskCall{op: "t", attempt: attempt.run})
	if err != nil || code != -400 {
		t.Fatalf("call = (%d, %v), want (-400, nil) for endpoint translation", code, err)
	}
	if refreshes.Load() != 0 {
		t.Fatalf("refreshes=%d, want 0", refreshes.Load())
	}
	if _, ok := g.cooldowns[1]; !ok {
		t.Fatal("non-risk business code must not clear an existing cooldown entry")
	}
}

func TestRiskGuardCooldownLadderEscalates(t *testing.T) {
	g, _ := newTestGuard()

	for i, want := range []time.Duration{
		riskCooldownLadder[0], riskCooldownLadder[1], riskCooldownLadder[2], riskCooldownLadder[2],
	} {
		before := time.Now()
		g.noteFailure(1)
		cd := g.cooldowns[1]
		if cd.failures != i+1 {
			t.Fatalf("failures=%d, want %d", cd.failures, i+1)
		}
		elapsed := cd.until.Sub(before)
		if elapsed < want-time.Second || elapsed > want+time.Second {
			t.Fatalf("failure %d: cooldown %v, want ~%v", i+1, elapsed, want)
		}
	}
}

func TestRiskGuardConcurrentUse(t *testing.T) {
	g, _ := newTestGuard()
	var wg sync.WaitGroup
	for room := int64(1); room <= 8; room++ {
		wg.Add(1)
		go func(room int64) {
			defer wg.Done()
			attempt := &scripted{steps: []scriptStep{{err: httpRiskErr(412)}, {code: 0}}}
			for i := 0; i < 50; i++ {
				attempt.calls = 0
				_, _ = g.call(context.Background(), room, riskCall{op: "t", attempt: attempt.run})
			}
		}(room)
	}
	wg.Wait()
}
