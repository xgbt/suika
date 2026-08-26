package biz

// sessionPolicy 是会话启停策略的唯一归属。策略是电平触发的：每个输入
// （房间信息到达、record_enabled 翻转、会话结束）先更新策略所知的世界状态，
// 然后由同一个 decide 按唯一判据 shouldRecord（录制门控开着且最新信息
// 在播）对照会话阶段裁决——该录而没有会话 → Start，在录而不该录 →
// Stop，收尾中不产生新决策。监控协程（runMonitorConnection）的 select 分支只负
// 责投递输入并执行返回的决策，自身不含任何启停判断。
//
// 停止是异步的（取消之后还有合并收尾），"收尾完成后恢复录制"因此
// 不是立即执行的动作，而是由会话结束时点推导：被停止的会话经过收尾阶
// 段，收尾完成时若世界状态已变回"该录"则立即恢复；自然结束的会话
// （未经停止）自身就是"此刻录不下去"的最新证据，不凭陈旧的在播信息
// 重启，等新到的世界状态再裁决。
//
// 模块只做决策：不接触 RoomRegistry、存储或 goroutine，由单个监控协程
// 独占，无需互斥锁。决策矩阵见 .scratch/session-policy/spec.md，矩阵
// 每一行都有对应测试；另见 ADR-0001、ADR-0002 与 CONTEXT.md 的
// "Session policy" 词条。
type sessionPolicy struct {
	// recordEnabled 是房间的录制门控（配置是否录制该房间）。
	recordEnabled bool
	// latest 是最近一次到达的房间信息。
	latest *RoomInfo
	// phase 是会话阶段：空闲、录制中、收尾中（已发送停止、尚未结束）。
	phase sessionPhase
}

// sessionPhase 是会话的三个阶段。
type sessionPhase int

const (
	phaseIdle      sessionPhase = iota // 空闲：无录制会话
	phaseRunning                       // 录制中：有录制会话
	phaseFinishing                     // 收尾中：已发送停止、尚未结束
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

// newSessionPolicy 创建会话策略，recordEnabled 为房间的初始录制开关状态。
func newSessionPolicy(recordEnabled bool) *sessionPolicy {
	return &sessionPolicy{recordEnabled: recordEnabled}
}

// shouldRecord 是策略的唯一判据：录制门控开着，且最新信息显示在播。
func (p *sessionPolicy) shouldRecord() bool {
	return p.recordEnabled && p.latest != nil && p.latest.Live
}

// decide 按当前世界状态对照会话阶段做差额裁决，是房间信息到达与录制
// 开关翻转两个入口共享的唯一决策逻辑。收尾阶段不产生决策：停止是异步的，
// 收尾期间到达的输入只更新世界状态，恢复与否留待会话结束时裁决。
func (p *sessionPolicy) decide() policyDecision {
	switch {
	case p.phase == phaseIdle && p.shouldRecord():
		p.phase = phaseRunning
		return policyDecision{kind: decisionStart, info: p.latest}
	case p.phase == phaseRunning && !p.shouldRecord():
		p.phase = phaseFinishing
		return policyDecision{kind: decisionStop}
	default:
		return policyDecision{}
	}
}

// RoomInfoArrived 处理到达的房间信息——弹幕房间状态事件与回退轮询的共享
// 入口。最新房间信息总是更新，然后重算裁决。
func (p *sessionPolicy) RoomInfoArrived(info *RoomInfo) policyDecision {
	p.latest = info
	return p.decide()
}

// RecordEnabledFlipped 处理房间录制开关状态的重评估信号（由监控的
// roomChange 分支从注册表读取后投递）。值与当前状态一致时重算结果
// 不变，从而吸收合并或重复的信号。
func (p *sessionPolicy) RecordEnabledFlipped(recordEnabled bool) policyDecision {
	p.recordEnabled = recordEnabled
	return p.decide()
}

// SessionFinished 处理会话结束事件。
// 收尾阶段的会话结束时，若世界状态已变回"该录"则立即恢复。
func (p *sessionPolicy) SessionFinished() policyDecision {
	stopped := p.phase == phaseFinishing
	p.phase = phaseIdle
	if stopped && p.shouldRecord() {
		p.phase = phaseRunning
		return policyDecision{kind: decisionStart, info: p.latest}
	}
	return policyDecision{}
}
