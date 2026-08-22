package data

import (
	"context"
	stderrors "errors"
	"fmt"
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

// riskCall 描述一次受风控保护的平台 API 调用。
type riskCall struct {
	// op 是端点名称，仅用于日志定位。
	op string
	// attempt 尝试一次 API 调用，返回业务码与传输/解析错误；
	// 响应体的解码与捕获在闭包内完成。
	attempt func(ctx context.Context) (code int, err error)
	// fallback 在 -352 重试后仍被风控时调用；成功（code==0 且
	// err==nil）视为整体成功。可选，目前仅弹幕取 token 一路使用
	// （旧版 getConf，无需 WBI 签名）。
	fallback func(ctx context.Context) (code int, err error)
}

// riskGuard 是所有 B 站 API 流量的风控编排模块：冷却闸门、412/-352
// 刷新重试、兜底调用、错误分类与每房间冷却阶梯，全部收在这里。
// 端点代码只负责构造请求、解析响应与翻译业务码。
type riskGuard struct {
	mu        sync.Mutex
	cooldowns map[int64]*riskCooldown
	refresh   func()
}

func newRiskGuard(refresh func()) *riskGuard {
	return &riskGuard{
		cooldowns: make(map[int64]*riskCooldown),
		refresh:   refresh,
	}
}

// call 执行一次受风控保护的 API 调用：冷却检查 → 尝试 → 风控重试 →
// 兜底 → 分类。HTTP 层风控（412/403/429）与 -352 一律刷新并重试一次；
// 风控类失败记入冷却并包装为 biz.ErrRiskControl；code==0 时清除冷却。
// 业务码非零且非风控时原样返回，由端点翻译。
func (g *riskGuard) call(ctx context.Context, roomID int64, rc riskCall) (int, error) {
	if err := g.checkCooldown(roomID); err != nil {
		return 0, err
	}

	code, err := rc.attempt(ctx)
	if err != nil && stderrors.Is(err, errHTTPRiskControl) {
		log.Warn("http-layer risk control, refreshing and retrying once", "op", rc.op, "room", roomID)
		g.refresh()
		code, err = rc.attempt(ctx)
	}
	if err != nil {
		return 0, g.classifyRisk(roomID, err)
	}
	if code == riskCode352 {
		log.Warn("risk control -352, refreshing and retrying once", "op", rc.op, "room", roomID)
		g.refresh()
		code, err = rc.attempt(ctx)
		if err != nil {
			return 0, g.classifyRisk(roomID, err)
		}
	}
	if code == riskCode352 {
		if rc.fallback != nil {
			log.Warn("still -352 after retry, trying fallback", "op", rc.op, "room", roomID)
			code, err = rc.fallback(ctx)
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

// checkCooldown 检查该房间的 API 请求是否处于冷却期；若是则返回
// biz.ErrRiskControl。
func (g *riskGuard) checkCooldown(roomID int64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	cd := g.cooldowns[roomID]
	if cd != nil && time.Now().Before(cd.until) {
		return fmt.Errorf("%w: room %d cooling down until %s", biz.ErrRiskControl, roomID, cd.until.Format(time.RFC3339))
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
