package biz

// sessionPolicy 归拢会话启停策略，只做纯决策：输入事件，输出
// Start(info) / Stop / None，不接触 registry、存储或 goroutine。
//
// 策略是电平触发的：每次输入先更新世界状态，再按 shouldRecord
// （record_enabled 打开且 latest 表示在播）对照 status 裁决。停止是异步
// 的，因此 finishing 阶段只更新状态不直接重启，是否恢复留到
// OnSessionFinished 时重算。
//
// 详细决策矩阵见 .scratch/session-policy/spec.md；设计背景见
// docs/adr/0001-session-policy-module.md 与
// docs/adr/0002-level-triggered-session-policy.md。
type sessionPolicy struct {
	// recordEnabled 是房间的录制门控（配置是否录制该房间）。
	recordEnabled bool
	// latest 是最近一次到达的房间信息。
	latest *RoomInfo
	// status 是会话状态：空闲、录制中、收尾中（已发送停止、尚未结束）。
	status sessionStatus
}

// sessionStatus 是会话的三个状态。
type sessionStatus int

const (
	statusIdle      sessionStatus = iota // 空闲：无录制会话
	statusRunning                        // 录制中：有录制会话
	statusFinishing                      // 收尾中：已发送停止、尚未结束
)

// sessionAction 是会话策略对单个事件的裁决。输出字母表为
// Start(info) / Stop / None；恢复不是独立决策——对监控而言它与开始
// 是同一动作。
type sessionAction struct {
	kind actionKind
	// info 是 kind == actionStart 时启动会话所使用的房间信息，其余
	// 决策下为 nil。
	info *RoomInfo
}

type actionKind int

const (
	actionNone actionKind = iota
	actionStart
	actionStop
)

func (k actionKind) String() string {
	switch k {
	case actionStart:
		return "Start"
	case actionStop:
		return "Stop"
	default:
		return "None"
	}
}

// newSessionPolicy 创建会话策略，recordEnabled 为房间的初始录制开关状态。
func newSessionPolicy(recordEnabled bool) *sessionPolicy {
	return &sessionPolicy{
		recordEnabled: recordEnabled,
	}
}

// shouldRecord 是策略的唯一判据：录制门控开着，且最新信息显示在播。
func (p *sessionPolicy) shouldRecord() bool {
	return p.recordEnabled && p.latest != nil && p.latest.Live
}

// decide 按当前世界状态对照会话状态做差额裁决，是房间信息到达与录制
// 开关翻转两个入口共享的唯一决策逻辑。收尾阶段不产生决策：停止是异步的，
// 收尾期间到达的输入只更新世界状态，恢复与否留待会话结束时裁决。
func (p *sessionPolicy) decide() sessionAction {
	switch {
	case p.status == statusIdle && p.shouldRecord():
		p.status = statusRunning
		return sessionAction{kind: actionStart, info: p.latest}
	case p.status == statusRunning && !p.shouldRecord():
		p.status = statusFinishing
		return sessionAction{kind: actionStop}
	default:
		return sessionAction{}
	}
}

// OnRoomInfo 处理到达的房间信息——弹幕房间状态事件与回退轮询的共享
// 入口。最新房间信息总是更新，然后重算裁决。
func (p *sessionPolicy) OnRoomInfo(info *RoomInfo) sessionAction {
	p.latest = info
	return p.decide()
}

// OnRecordEnabled 处理房间录制开关状态的重评估信号（由监控的
// roomChange 分支从注册表读取后投递）。值与当前状态一致时重算结果
// 不变，从而吸收合并或重复的信号。
func (p *sessionPolicy) OnRecordEnabled(recordEnabled bool) sessionAction {
	p.recordEnabled = recordEnabled
	return p.decide()
}

// OnSessionFinished 处理会话结束事件。
// 收尾阶段的会话结束时，若世界状态已变回"该录"则立即恢复。
func (p *sessionPolicy) OnSessionFinished() sessionAction {
	stopped := p.status == statusFinishing
	p.status = statusIdle
	if stopped && p.shouldRecord() {
		p.status = statusRunning
		return sessionAction{kind: actionStart, info: p.latest}
	}
	return sessionAction{}
}
