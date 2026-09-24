// risk.go 是全部 B 站 API 流量的风控编排模块：合规请求的构造、冷却闸门、
// 412/403/429 与 -352 的刷新重试、旧接口兜底、错误分类，以及每房间递增
// 的冷却阶梯。
package bili

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"suika/internal/biz"

	"github.com/go-kratos/kratos/v3/log"
)

// riskCooldownLadder 风控被拒后，递增的冷却时长
var riskCooldownLadder = []time.Duration{5 * time.Minute, 10 * time.Minute, 20 * time.Minute}

type riskCooldown struct {
	failures int
	until    time.Time
}

// codedResponse 是 B 站 API 响应体的共同形状：顶层带一个业务码字段。
// riskGuard 靠它读业务码做风控判定，端点只关心其余字段。
type codedResponse interface {
	// bizCode 返回响应体顶层的业务码。
	bizCode() int
}

// riskRequest 描述一次受风控保护的平台 API 调用：端点形状与响应落点。
// 调用方只声明形状，注入指纹、WBI 签名、发送请求全部由 riskGuard 完成，
// 端点没有机会漏掉其中任何一步。
type riskRequest struct {
	// op 是端点名称，仅用于日志定位。
	op string
	// path 是端点路径，拼在 liveAPIBase 之后。
	path string
	// query 是查询参数；nil 表示无参数。
	query url.Values
	// sign 为 true 时对 URL 做 WBI 签名。旧版接口不需要签名，显式置
	// false —— 让"有意不签名"成为一个看得见的参数，而不是漏掉的步骤。
	sign bool
	// out 是 JSON 响应体的解码落点。
	out codedResponse
	// done 在响应解码成功且业务码为 0 后调用，端点在此把响应转成领域
	// 对象或做额外校验；返回错误表示本次尝试不成立。可为 nil。
	done func() error
}

// riskCall 描述一次受风控保护的整体调用：一次尝试，外加可选的兜底端点。
type riskCall struct {
	// attempt 是主端点。
	attempt riskRequest
	// fallback 在 -352 重试后仍被风控时调用；成功（业务码为 0 且无错误）
	// 视为整体成功。可选，目前仅弹幕取 token 一路使用（旧版 getConf，
	// 无需 WBI 签名）。
	fallback *riskRequest
}

// riskGuard 是所有 B 站 API 流量的风控编排模块：合规请求构造、冷却闸门、
// 412/-352 刷新重试、兜底调用、错误分类与每房间冷却阶梯，全部收在这里。
// 端点代码只负责声明端点形状与翻译业务码。
type riskGuard struct {
	mu        sync.Mutex
	cooldowns map[int64]*riskCooldown
	transport riskTransport
}

func newRiskGuard(transport riskTransport) *riskGuard {
	return &riskGuard{
		cooldowns: make(map[int64]*riskCooldown),
		transport: transport,
	}
}

// call 执行一次受风控保护的 API 调用：冷却检查 → 尝试 → 风控重试 →
// 兜底 → 分类。HTTP 层风控（412/403/429）与 -352 一律刷新并重试一次；
// 风控类失败记入冷却并包装为 biz.ErrRiskControl；业务码为 0 时清除冷却。
// 业务码非零且非风控时原样返回，由端点翻译。
func (g *riskGuard) call(ctx context.Context, roomID int64, rc riskCall) (int, error) {
	if err := g.checkCooldown(roomID); err != nil {
		return 0, err
	}

	code, err := g.callOnce(ctx, roomID, &rc.attempt)
	if err != nil && stderrors.Is(err, errHTTPRiskControl) {
		log.Warn("http-layer risk control, refreshing and retrying once", "op", rc.attempt.op, "room", roomID)
		g.transport.refreshRiskState(ctx)
		code, err = g.callOnce(ctx, roomID, &rc.attempt)
	}
	if err != nil {
		return 0, g.classifyRisk(roomID, err)
	}
	if code == riskCode352 {
		log.Warn("risk control -352, refreshing and retrying once", "op", rc.attempt.op, "room", roomID)
		g.transport.refreshRiskState(ctx)
		code, err = g.callOnce(ctx, roomID, &rc.attempt)
		if err != nil {
			return 0, g.classifyRisk(roomID, err)
		}
	}
	if code == riskCode352 {
		if rc.fallback != nil {
			log.Warn("still -352 after retry, trying fallback", "op", rc.attempt.op, "room", roomID)
			code, err = g.callOnce(ctx, roomID, rc.fallback)
			if err == nil && code == 0 {
				g.noteSuccess(roomID)
				return 0, nil
			}
		}
		return 0, g.classifyRisk(roomID, fmt.Errorf("%w: room_id=%d", errRiskControl352, roomID))
	}
	if code == 0 {
		g.noteSuccess(roomID)
	}
	return code, nil
}

// callOnce 构造并发送一次请求，不含任何风控重试：拼接路径与查询参数 → 按需
// 签名 → 注入新鲜指纹 → 发送并解码 → 读业务码 → 执行端点的后处理。
// HTTP 层风控由 transport 映射为 errHTTPRiskControl，供 call 的重试分支
// 识别。
func (g *riskGuard) callOnce(ctx context.Context, roomID int64, r *riskRequest) (int, error) {
	endpoint := liveAPIBase + r.path
	if len(r.query) > 0 {
		endpoint += "?" + r.query.Encode()
	}
	if r.sign {
		endpoint = g.transport.signEndpoint(ctx, endpoint)
	}

	cookie := g.transport.antiRiskCookie(ctx)
	if err := g.transport.fetchEndpointJSON(ctx, endpoint, roomID, cookie, r.out); err != nil {
		return 0, err
	}

	code := r.out.bizCode()
	if code != 0 {
		return code, nil
	}
	if r.done != nil {
		if err := r.done(); err != nil {
			return 0, err
		}
	}
	return 0, nil
}

// checkCooldown 检查该房间的 API 请求是否处于冷却期；若是则返回
// biz.ErrRiskControl。
func (g *riskGuard) checkCooldown(roomID int64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	cd := g.cooldowns[roomID]
	if cd != nil && time.Now().Before(cd.until) {
		return fmt.Errorf("%w: room %d cooling down until %s",
			biz.ErrRiskControl, roomID, cd.until.Format(time.RFC3339))
	}

	return nil
}

// classifyRisk 把风控类错误记入冷却并包装为 biz.ErrRiskControl；
// 其余错误原样透传。
func (g *riskGuard) classifyRisk(roomID int64, err error) error {
	if !stderrors.Is(err, errRiskControl352) && !stderrors.Is(err, errHTTPRiskControl) {
		return err
	}
	g.noteFailure(roomID)
	return fmt.Errorf("%w: %v", biz.ErrRiskControl, err)
}

// noteFailure 记录该房间的 API 请求被风控拒绝，增加冷却时长。
func (g *riskGuard) noteFailure(roomID int64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	cd := g.cooldowns[roomID]
	if cd == nil {
		cd = &riskCooldown{}
		g.cooldowns[roomID] = cd
	}
	cd.failures++
	idx := min(cd.failures-1, len(riskCooldownLadder)-1)
	cd.until = time.Now().Add(riskCooldownLadder[idx])
	log.Warn("room risk-control cooldown started", "room", roomID, "failures", cd.failures, "until", cd.until.Format(time.RFC3339))
}

// noteSuccess 记录该房间的 API 请求成功，清除冷却状态。
func (g *riskGuard) noteSuccess(roomID int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.cooldowns, roomID)
}
