package bili

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"suika/internal/biz"
)

// scriptStep 是 scripted 的一步：返回的业务码与错误。
type scriptStep struct {
	code int
	err  error
}

// testResponse 是 riskGuard 测试用的最小响应体，同时记录 done 钩子是否
// 被调用过。
type testResponse struct {
	code int
	done bool
}

func (r *testResponse) bizCode() int { return r.code }

// testCall 构造一次最小调用，响应落在一个新的 testResponse 上。
func testCall(resp *testResponse) riskCall {
	return riskCall{attempt: riskRequest{
		op:   "t",
		path: "/t",
		out:  resp,
		done: func() error { resp.done = true; return nil },
	}}
}

// scriptedTransport 实现 riskTransport：按脚本逐步回答 fetchEndpointJSON，走完脚本
// 后重复最后一步。它同时是调用次数与刷新次数的探针，并记录最近一次请求的
// 形状，供请求构造的断言使用。
type scriptedTransport struct {
	mu        sync.Mutex
	calls     int
	steps     []scriptStep
	refreshes int
	signCalls int

	lastEndpoint string
	lastRoomID   int64
	lastCookie   string
}

func (t *scriptedTransport) antiRiskCookie(context.Context) string {
	return "SESSDATA=stub"
}

func (t *scriptedTransport) signEndpoint(_ context.Context, endpoint string) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.signCalls++
	return endpoint + "&w_rid=stub"
}

func (t *scriptedTransport) refreshRiskState(_ context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refreshes++
}

func (t *scriptedTransport) fetchEndpointJSON(_ context.Context, endpoint string, roomID int64, cookie string, out any) error {
	t.mu.Lock()
	if len(t.steps) == 0 {
		t.mu.Unlock()
		return stderrors.New("scriptedTransport: no steps")
	}
	step := t.steps[min(t.calls, len(t.steps)-1)]
	t.calls++
	t.lastEndpoint, t.lastRoomID, t.lastCookie = endpoint, roomID, cookie
	t.mu.Unlock()

	if step.err != nil {
		return step.err
	}
	resp, ok := out.(*testResponse)
	if !ok {
		return fmt.Errorf("scriptedTransport: unexpected response type %T", out)
	}
	resp.code = step.code
	return nil
}

func newTestGuard(steps []scriptStep) (*riskGuard, *scriptedTransport) {
	tr := &scriptedTransport{steps: steps}
	return newRiskGuard(tr), tr
}

func httpRiskErr(status int) error {
	return fmt.Errorf("%w: status=%d", errHTTPRiskControl, status)
}

func TestRiskGuardCallSuccess(t *testing.T) {
	g, tr := newTestGuard([]scriptStep{{code: 0}})
	resp := &testResponse{}

	code, err := g.call(context.Background(), 1, testCall(resp))
	if err != nil || code != 0 {
		t.Fatalf("call = (%d, %v), want (0, nil)", code, err)
	}
	if tr.calls != 1 || tr.refreshes != 0 {
		t.Fatalf("calls=%d refreshes=%d, want 1/0", tr.calls, tr.refreshes)
	}
	if !resp.done {
		t.Fatal("done hook should run when the business code is 0")
	}
	if err := g.checkCooldown(1); err != nil {
		t.Fatalf("no cooldown expected, got %v", err)
	}
}

func TestRiskGuardSuccessClearsCooldown(t *testing.T) {
	g, _ := newTestGuard([]scriptStep{{code: 0}})
	g.noteFailure(1)
	g.cooldowns[1].until = time.Now().Add(-time.Second) // 模拟冷却已过期

	if _, err := g.call(context.Background(), 1, testCall(&testResponse{})); err != nil {
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
	g, tr := newTestGuard([]scriptStep{{code: 0}})
	g.noteFailure(1)

	_, err := g.call(context.Background(), 1, testCall(&testResponse{}))
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
	if tr.calls != 0 || tr.refreshes != 0 {
		t.Fatalf("calls=%d refreshes=%d, want 0/0 (gated)", tr.calls, tr.refreshes)
	}
}

func TestRiskGuardPlainErrorPassesThrough(t *testing.T) {
	plain := stderrors.New("network unreachable")
	g, tr := newTestGuard([]scriptStep{{err: plain}})

	_, err := g.call(context.Background(), 1, testCall(&testResponse{}))
	if !stderrors.Is(err, plain) {
		t.Fatalf("err = %v, want pass-through of %v", err, plain)
	}
	if tr.refreshes != 0 {
		t.Fatalf("refreshes=%d, want 0", tr.refreshes)
	}
	if err := g.checkCooldown(1); err != nil {
		t.Fatalf("plain error must not start cooldown, got %v", err)
	}
}

func TestRiskGuardHTTPRiskRetriesOnce(t *testing.T) {
	g, tr := newTestGuard([]scriptStep{{err: httpRiskErr(412)}, {code: 0}})

	code, err := g.call(context.Background(), 1, testCall(&testResponse{}))
	if err != nil || code != 0 {
		t.Fatalf("call = (%d, %v), want success after retry", code, err)
	}
	if tr.calls != 2 || tr.refreshes != 1 {
		t.Fatalf("calls=%d refreshes=%d, want 2/1", tr.calls, tr.refreshes)
	}
}

func TestRiskGuardHTTPRiskRetryExhausted(t *testing.T) {
	g, tr := newTestGuard([]scriptStep{{err: httpRiskErr(412)}})

	_, err := g.call(context.Background(), 1, testCall(&testResponse{}))
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
	if tr.calls != 2 || tr.refreshes != 1 {
		t.Fatalf("calls=%d refreshes=%d, want 2/1", tr.calls, tr.refreshes)
	}
	if err := g.checkCooldown(1); err == nil {
		t.Fatal("cooldown expected after exhausted risk retry")
	}
}

func TestRiskGuardRiskCode352RetriesOnce(t *testing.T) {
	g, tr := newTestGuard([]scriptStep{{code: riskCode352}, {code: 0}})

	code, err := g.call(context.Background(), 1, testCall(&testResponse{}))
	if err != nil || code != 0 {
		t.Fatalf("call = (%d, %v), want success after -352 retry", code, err)
	}
	if tr.calls != 2 || tr.refreshes != 1 {
		t.Fatalf("calls=%d refreshes=%d, want 2/1", tr.calls, tr.refreshes)
	}
}

func TestRiskGuardRiskCode352Exhausted(t *testing.T) {
	g, tr := newTestGuard([]scriptStep{{code: riskCode352}})

	_, err := g.call(context.Background(), 1, testCall(&testResponse{}))
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
	if tr.calls != 2 {
		t.Fatalf("calls=%d, want 2 (initial + one retry)", tr.calls)
	}
	if err := g.checkCooldown(1); err == nil {
		t.Fatal("cooldown expected after exhausted -352")
	}
}

func TestRiskGuardRiskCode352RetryErrorClassified(t *testing.T) {
	g, _ := newTestGuard([]scriptStep{{code: riskCode352}, {err: httpRiskErr(429)}})

	_, err := g.call(context.Background(), 1, testCall(&testResponse{}))
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
	if err := g.checkCooldown(1); err == nil {
		t.Fatal("cooldown expected when the -352 retry itself hits risk control")
	}
}

func TestRiskGuardRiskCode352RetryPlainError(t *testing.T) {
	plain := stderrors.New("timeout")
	g, _ := newTestGuard([]scriptStep{{code: riskCode352}, {err: plain}})

	_, err := g.call(context.Background(), 1, testCall(&testResponse{}))
	if !stderrors.Is(err, plain) {
		t.Fatalf("err = %v, want pass-through of %v", err, plain)
	}
	if err := g.checkCooldown(1); err != nil {
		t.Fatalf("plain retry error must not start cooldown, got %v", err)
	}
}

func TestRiskGuardFallbackSuccess(t *testing.T) {
	g, tr := newTestGuard([]scriptStep{{code: riskCode352}, {code: riskCode352}, {code: 0}})
	g.noteFailure(1) // 预置已过期的冷却，验证成功后条目被删除
	g.cooldowns[1].until = time.Now().Add(-time.Second)
	fallback := &riskRequest{op: "fallback", path: "/fallback", out: &testResponse{}}

	rc := testCall(&testResponse{})
	rc.fallback = fallback

	code, err := g.call(context.Background(), 1, rc)
	if err != nil || code != 0 {
		t.Fatalf("call = (%d, %v), want fallback success", code, err)
	}
	if tr.calls != 3 {
		t.Fatalf("calls=%d, want 3 (two attempts + one fallback)", tr.calls)
	}
	if _, ok := g.cooldowns[1]; ok {
		t.Fatal("cooldown entry should be deleted after fallback success")
	}
}

func TestRiskGuardFallbackFailure(t *testing.T) {
	g, _ := newTestGuard([]scriptStep{
		{code: riskCode352}, {code: riskCode352}, {err: stderrors.New("getConf unreachable")},
	})

	rc := testCall(&testResponse{})
	rc.fallback = &riskRequest{op: "fallback", path: "/fallback", out: &testResponse{}}

	_, err := g.call(context.Background(), 1, rc)
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
	if err := g.checkCooldown(1); err == nil {
		t.Fatal("cooldown expected after fallback failure")
	}
}

func TestRiskGuardFallbackNonZeroCode(t *testing.T) {
	g, _ := newTestGuard([]scriptStep{{code: riskCode352}, {code: riskCode352}, {code: 5}})

	rc := testCall(&testResponse{})
	rc.fallback = &riskRequest{op: "fallback", path: "/fallback", out: &testResponse{}}

	_, err := g.call(context.Background(), 1, rc)
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
}

// 兜底端点的 done 钩子报错（如旧版 getConf 返回空 token）与「兜底本身失败」
// 走同一条路：统一归为 -352 风控并记入冷却。这不完全准确（空 token 未必是
// 风控），但沿用既有语义；若日后要区分，从这里改起。
func TestRiskGuardFallbackDoneErrorTreatedAsRisk(t *testing.T) {
	g, tr := newTestGuard([]scriptStep{{code: riskCode352}, {code: riskCode352}, {code: 0}})

	rc := testCall(&testResponse{})
	rc.fallback = &riskRequest{
		op: "fallback", path: "/fallback", out: &testResponse{},
		done: func() error { return stderrors.New("legacy getConf returned empty token") },
	}

	_, err := g.call(context.Background(), 1, rc)
	if !stderrors.Is(err, biz.ErrRiskControl) {
		t.Fatalf("err = %v, want biz.ErrRiskControl", err)
	}
	if tr.calls != 3 {
		t.Fatalf("calls=%d, want 3", tr.calls)
	}
}

func TestRiskGuardNonZeroCodeNoBookkeeping(t *testing.T) {
	g, tr := newTestGuard([]scriptStep{{code: -400}})
	g.noteFailure(1) // 预置已过期的冷却条目
	g.cooldowns[1].until = time.Now().Add(-time.Second)
	resp := &testResponse{}

	code, err := g.call(context.Background(), 1, testCall(resp))
	if err != nil || code != -400 {
		t.Fatalf("call = (%d, %v), want (-400, nil) for endpoint translation", code, err)
	}
	if tr.refreshes != 0 {
		t.Fatalf("refreshes=%d, want 0", tr.refreshes)
	}
	if resp.done {
		t.Fatal("done hook must not run for a non-zero business code")
	}
	if _, ok := g.cooldowns[1]; !ok {
		t.Fatal("non-risk business code must not clear an existing cooldown entry")
	}
}

func TestRiskGuardCooldownLadderEscalates(t *testing.T) {
	g, _ := newTestGuard(nil)

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

// callOnce 负责把声明的端点形状变成合规请求：拼上 liveAPIBase 与查询参数、
// 按 sign 决定是否签名、注入指纹快照。这些步骤原先散在各端点里手工重复。
func TestRiskGuardFetchBuildsRequest(t *testing.T) {
	g, tr := newTestGuard([]scriptStep{{code: 0}})

	_, err := g.call(context.Background(), 42, riskCall{attempt: riskRequest{
		op:    "probe",
		path:  "/x/y",
		query: url.Values{"room_id": {"42"}, "format": {"0,1,2"}},
		sign:  true,
		out:   &testResponse{},
	}})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if !strings.HasPrefix(tr.lastEndpoint, liveAPIBase+"/x/y?") {
		t.Fatalf("endpoint = %q, want prefix %q", tr.lastEndpoint, liveAPIBase+"/x/y?")
	}
	if !strings.Contains(tr.lastEndpoint, "room_id=42") {
		t.Fatalf("endpoint = %q, want the query parameters", tr.lastEndpoint)
	}
	if tr.signCalls != 1 || !strings.Contains(tr.lastEndpoint, "w_rid=") {
		t.Fatalf("signCalls=%d endpoint=%q, want a signed request", tr.signCalls, tr.lastEndpoint)
	}
	if tr.lastRoomID != 42 {
		t.Fatalf("roomID = %d, want 42", tr.lastRoomID)
	}
	if tr.lastCookie != "SESSDATA=stub" {
		t.Fatalf("cookie = %q, want the injected fingerprint snapshot", tr.lastCookie)
	}
}

// sign: false 的端点（旧版 getConf）既不签名也不带查询参数。
func TestRiskGuardFetchSkipsSignature(t *testing.T) {
	g, tr := newTestGuard([]scriptStep{{code: 0}})

	if _, err := g.call(context.Background(), 1, riskCall{attempt: riskRequest{
		op: "legacy", path: "/legacy", sign: false, out: &testResponse{},
	}}); err != nil {
		t.Fatalf("call: %v", err)
	}
	if tr.signCalls != 0 {
		t.Fatalf("signCalls=%d, want 0 for an unsigned endpoint", tr.signCalls)
	}
	if tr.lastEndpoint != liveAPIBase+"/legacy" {
		t.Fatalf("endpoint = %q, want %q", tr.lastEndpoint, liveAPIBase+"/legacy")
	}
}

// 主端点 done 钩子的错误原样上抛，且不记冷却：它不是风控。
func TestRiskGuardDoneErrorSurfaces(t *testing.T) {
	boom := stderrors.New("empty token")
	g, _ := newTestGuard([]scriptStep{{code: 0}})

	_, err := g.call(context.Background(), 1, riskCall{attempt: riskRequest{
		op: "t", path: "/t", out: &testResponse{},
		done: func() error { return boom },
	}})
	if !stderrors.Is(err, boom) {
		t.Fatalf("err = %v, want pass-through of %v", err, boom)
	}
	if err := g.checkCooldown(1); err != nil {
		t.Fatalf("done error must not start cooldown, got %v", err)
	}
}

func TestRiskGuardConcurrentUse(t *testing.T) {
	g, _ := newTestGuard([]scriptStep{{err: httpRiskErr(412)}, {code: 0}})
	var wg sync.WaitGroup
	for room := int64(1); room <= 8; room++ {
		wg.Add(1)
		go func(room int64) {
			defer wg.Done()
			for range 50 {
				_, _ = g.call(context.Background(), room, testCall(&testResponse{}))
			}
		}(room)
	}
	wg.Wait()
}
