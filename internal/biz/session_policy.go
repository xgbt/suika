package biz

// sessionPolicy 是会话启停策略的唯一归属：依据房间 enabled 门控、最新
// 直播信息与会话阶段（idle / running / finishing）决定会话何时开始、停止
// 与恢复（含"启用到达时会话正在收尾"的恢复规则）。监控协程（watchRoom）
// 的 select 分支只负责投递输入（房间信息到达、enabled 翻转、会话结束）并
// 执行返回的决策，自身不再包含任何启停判断。
//
// 模块只做决策：不接触 RoomRegistry、存储或 goroutine，由单个监控协程独占，
// 无需互斥锁。决策矩阵见 .scratch/session-policy/spec.md，矩阵每一行都有
// 对应测试；另见 ADR-0001 与 CONTEXT.md 的"Session policy"词条。
type sessionPolicy struct {
	// enabled 是房间的录制启用门控。
	enabled bool
	// latestRoomInfo 是最近一次到达的房间信息。
	latestRoomInfo *RoomInfo
	// phase 是会话阶段：空闲、录制中、收尾中（已发送停止、尚未结束）。
	phase sessionPhase
	// resumeOnFinish 在"启用到达时会话正在收尾"置位：收尾完成后若最新
	// 信息仍显示在播则恢复录制。
	resumeOnFinish bool
}

// sessionPhase 是会话的三个阶段。
type sessionPhase int

const (
	phaseIdle sessionPhase = iota
	phaseRunning
	phaseFinishing
)

// policyDecision 是会话策略对单个事件的裁决。输出字母表为
// Start(info) / Stop / None；恢复不是独立决策——对监控而言它与开始
// 是同一动作。
type policyDecision struct {
	kind decisionKind
	// info 是 kind == decisionStart 时启动会话所使用的房间信息，其余
	// 决策下为 nil。
	info *RoomInfo
}

type decisionKind int

const (
	decisionNone decisionKind = iota
	decisionStart
	decisionStop
)

// newSessionPolicy 创建会话策略，enabled 为房间的初始启用状态。
func newSessionPolicy(enabled bool) *sessionPolicy {
	return &sessionPolicy{enabled: enabled}
}

// RoomInfoArrived 处理到达的房间信息——弹幕房间状态事件与回退轮询的共享
// 入口。无论决策如何，最新房间信息总是更新。
func (p *sessionPolicy) RoomInfoArrived(info *RoomInfo) policyDecision {
	p.latestRoomInfo = info
	switch {
	case info.Live && p.enabled && p.phase == phaseIdle:
		p.phase = phaseRunning
		return policyDecision{kind: decisionStart, info: info}
	case !info.Live && p.phase == phaseRunning:
		p.phase = phaseFinishing
		return policyDecision{kind: decisionStop}
	default:
		return policyDecision{}
	}
}

// EnabledFlipped 处理房间启用状态的重评估信号（由监控的 roomChanged 分支
// 从注册表读取后投递）。值与当前状态一致时为无操作决策，从而吸收合并或
// 重复的信号。
func (p *sessionPolicy) EnabledFlipped(enabled bool) policyDecision {
	if enabled == p.enabled {
		return policyDecision{}
	}
	p.enabled = enabled

	// 启用到达时会话空闲：若最新信息显示在播则立即开始录制。
	if enabled {
		switch p.phase {
		case phaseIdle:
			if p.latestRoomInfo != nil && p.latestRoomInfo.Live {
				p.phase = phaseRunning
				return policyDecision{kind: decisionStart, info: p.latestRoomInfo}
			}
		case phaseFinishing:
			// 启用到达时会话正在收尾：收尾完成后若仍在播则恢复录制。
			p.resumeOnFinish = true
		}
		return policyDecision{}
	}

	// 禁用到达时会话正在录制或收尾：立即停止。
	p.resumeOnFinish = false
	if p.phase == phaseRunning {
		p.phase = phaseFinishing
		return policyDecision{kind: decisionStop}
	}
	return policyDecision{}
}

// SessionFinished 处理会话协程结束：若恢复标志置位且最新信息仍显示在播
// 则立即恢复录制（标志随之清除），否则回到空闲。
func (p *sessionPolicy) SessionFinished() policyDecision {
	if p.resumeOnFinish && p.latestRoomInfo != nil && p.latestRoomInfo.Live {
		p.resumeOnFinish = false
		p.phase = phaseRunning
		return policyDecision{kind: decisionStart, info: p.latestRoomInfo}
	}
	p.phase = phaseIdle
	return policyDecision{}
}
