package biz

import "testing"

// sessionPolicy 的测试以纯事件序列驱动策略模块：无 goroutine、无时钟、
// 无 fake。每个用例是一串"投递事件 → 断言决策"的步骤，与
// .scratch/session-policy/spec.md 决策矩阵逐行对应；阶段（idle /
// running / finishing）不直接断言，而是通过后续事件的决策体现。
// 策略是电平触发的（ADR-0002）：收尾后续录等恢复行为不需要专门标志，
// 是会话结束后重算的自然结果，相关用例同样逐行锁定。

type policyStep struct {
	send func(*sessionPolicy) sessionAction
	want sessionAction
}

func onRoomInfo(info *RoomInfo) func(*sessionPolicy) sessionAction {
	return func(p *sessionPolicy) sessionAction { return p.OnRoomInfo(info) }
}

func onRecordEnabled(recordEnabled bool) func(*sessionPolicy) sessionAction {
	return func(p *sessionPolicy) sessionAction { return p.OnRecordEnabled(recordEnabled) }
}

func onSessionFinished() func(*sessionPolicy) sessionAction {
	return func(p *sessionPolicy) sessionAction { return p.OnSessionFinished() }
}

func wantStart(info *RoomInfo) sessionAction { return sessionAction{kind: actionStart, info: info} }
func wantStop() sessionAction                { return sessionAction{kind: actionStop} }
func wantNone() sessionAction                { return sessionAction{} }

func runPolicySteps(t *testing.T, initialRecordEnabled bool, steps []policyStep) {
	t.Helper()
	p := newSessionPolicy(initialRecordEnabled)
	for i, step := range steps {
		if got := step.send(p); got != step.want {
			t.Fatalf("step %d: decision kind=%s info=%+v, want kind=%s info=%+v", i, got.kind, got.info, step.want.kind, step.want.info)
		}
	}
}

// TestSessionPolicyOnRoomInfo 覆盖决策矩阵中"room info arrived"的
// 全部行（含 latest 始终更新一行经由后续事件间接验证）。
func TestSessionPolicyOnRoomInfo(t *testing.T) {
	live := &RoomInfo{RoomID: 42, Live: true, Title: "on-air"}
	liveAgain := &RoomInfo{RoomID: 42, Live: true, Title: "still-on-air"}
	offline := &RoomInfo{RoomID: 42, Title: "preparing"}

	cases := []struct {
		name                 string
		initialRecordEnabled bool
		steps                []policyStep
	}{
		{
			name:                 "live record_enabled idle starts",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
			},
		},
		{
			name:                 "live record_enabled running does not start again",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRoomInfo(liveAgain), want: wantNone()},
			},
		},
		{
			name:                 "live record_enabled finishing resumes after finish",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRoomInfo(offline), want: wantStop()},
				// 收尾期间到达的开播信息只更新 latest，不立即启动。
				{send: onRoomInfo(liveAgain), want: wantNone()},
				// 收尾完成：重算发现仍该录（配置录制且在播），立即恢复。
				{send: onSessionFinished(), want: wantStart(liveAgain)},
			},
		},
		{
			name:                 "live record_enabled off does not start",
			initialRecordEnabled: false,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantNone()},
			},
		},
		{
			name:                 "live record_enabled off finishing does not start",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRecordEnabled(false), want: wantStop()},
				// live · record_enabled off · finishing：收尾期间到达的开播信息
				// 只更新 latest，不启动会话。
				{send: onRoomInfo(liveAgain), want: wantNone()},
				// 收尾完成：门控仍关着，即便最新信息说在播也不恢复。
				{send: onSessionFinished(), want: wantNone()},
			},
		},
		{
			name:                 "not live running stops",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRoomInfo(offline), want: wantStop()},
			},
		},
		{
			name:                 "not live finishing gives no redundant stop",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRoomInfo(offline), want: wantStop()},
				{send: onRoomInfo(offline), want: wantNone()},
			},
		},
		{
			name:                 "not live idle does nothing",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(offline), want: wantNone()},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runPolicySteps(t, tc.initialRecordEnabled, tc.steps)
		})
	}
}

// TestSessionPolicyOnRecordEnabled 覆盖决策矩阵中"record_enabled updated"的全部
// 行；收尾后续录由 TestSessionPolicyOnSessionFinished 与
// TestSessionPolicyPreservedQuirks 验证。
func TestSessionPolicyOnRecordEnabled(t *testing.T) {
	live := &RoomInfo{RoomID: 42, Live: true}
	offline := &RoomInfo{RoomID: 42}

	cases := []struct {
		name                 string
		initialRecordEnabled bool
		steps                []policyStep
	}{
		{
			name:                 "flip on idle with live latest starts with latest",
			initialRecordEnabled: false,
			steps: []policyStep{
				// 未配置录制期间到达的信息只更新 latest，不启动会话。
				{send: onRoomInfo(live), want: wantNone()},
				{send: onRecordEnabled(true), want: wantStart(live)},
			},
		},
		{
			name:                 "flip on idle without any info does nothing",
			initialRecordEnabled: false,
			steps: []policyStep{
				{send: onRecordEnabled(true), want: wantNone()},
			},
		},
		{
			name:                 "flip on idle with offline latest does nothing",
			initialRecordEnabled: false,
			steps: []policyStep{
				{send: onRoomInfo(offline), want: wantNone()},
				{send: onRecordEnabled(true), want: wantNone()},
			},
		},
		{
			name:                 "flip on while already on is absorbed",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRecordEnabled(true), want: wantNone()},
			},
		},
		{
			name:                 "flip off running stops",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRecordEnabled(false), want: wantStop()},
			},
		},
		{
			name:                 "flip off finishing gives no redundant stop",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRoomInfo(offline), want: wantStop()},
				{send: onRecordEnabled(false), want: wantNone()},
			},
		},
		{
			name:                 "flip off idle does nothing",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRecordEnabled(false), want: wantNone()},
			},
		},
		{
			name:                 "coalesced off-on flip nets out during running",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				// 关闭→开启录制在一次信号内合并：值未变，决策为无操作，
				// 会话继续录制（阶段仍为 running，下播仍会停止）。
				{send: onRecordEnabled(true), want: wantNone()},
				{send: onRoomInfo(offline), want: wantStop()},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runPolicySteps(t, tc.initialRecordEnabled, tc.steps)
		})
	}
}

// TestSessionPolicyOnSessionFinished 覆盖决策矩阵中"session finished"的行。
func TestSessionPolicyOnSessionFinished(t *testing.T) {
	live := &RoomInfo{RoomID: 42, Live: true}
	offline := &RoomInfo{RoomID: 42}

	cases := []struct {
		name                 string
		initialRecordEnabled bool
		steps                []policyStep
	}{
		{
			name:                 "natural end goes idle",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				// 自然结束（未经停止）：无决策，阶段回到空闲，
				// 下一次开播信息可再次启动会话。
				{send: onSessionFinished(), want: wantNone()},
				{send: onRoomInfo(live), want: wantStart(live)},
			},
		},
		{
			name:                 "stopped session finishes to idle without resume",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRoomInfo(offline), want: wantStop()},
				{send: onSessionFinished(), want: wantNone()},
			},
		},
		{
			name:                 "flip on during finishing resumes after finish",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRecordEnabled(false), want: wantStop()},
				{send: onRecordEnabled(true), want: wantNone()},
				{send: onSessionFinished(), want: wantStart(live)},
			},
		},
		{
			name:                 "resume is one-shot: second finish goes idle",
			initialRecordEnabled: true,
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRecordEnabled(false), want: wantStop()},
				{send: onRecordEnabled(true), want: wantNone()},
				{send: onSessionFinished(), want: wantStart(live)},
				{send: onSessionFinished(), want: wantNone()},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runPolicySteps(t, tc.initialRecordEnabled, tc.steps)
		})
	}
}

// TestSessionPolicyPreservedQuirks 锁定原电平触发实现需要专门补丁（恢复
// 标志等）才能给出的三个行为。电平触发重算（ADR-0002）让它们成为重算的
// 自然结果，但这些可观察行为不变，继续逐例锁定。
func TestSessionPolicyPreservedQuirks(t *testing.T) {
	live := &RoomInfo{RoomID: 42, Live: true, Title: "stale"}
	liveFresh := &RoomInfo{RoomID: 42, Live: true, Title: "fresh"}
	offline := &RoomInfo{RoomID: 42}

	cases := []struct {
		name  string
		steps []policyStep
	}{
		{
			name: "stale-live resume: flip on during finishing resumes even if stream died",
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRecordEnabled(false), want: wantStop()},
				{send: onRecordEnabled(true), want: wantNone()},
				// 收尾完成：尽管流可能已死，latest 仍说在播，照录。
				// 新会话随后会在开流失败时优雅结束（由录制循环负责）。
				{send: onSessionFinished(), want: wantStart(live)},
				// 新鲜的下播信息到达才真正停下。
				{send: onRoomInfo(offline), want: wantStop()},
				{send: onSessionFinished(), want: wantNone()},
			},
		},
		{
			name: "resume uses the freshest known room info",
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRecordEnabled(false), want: wantStop()},
				// 收尾期间到达的新信息更新 latest。
				{send: onRoomInfo(liveFresh), want: wantNone()},
				{send: onRecordEnabled(true), want: wantNone()},
				{send: onSessionFinished(), want: wantStart(liveFresh)},
			},
		},
		{
			name: "flip off during finishing suppresses the resume",
			steps: []policyStep{
				{send: onRoomInfo(live), want: wantStart(live)},
				{send: onRecordEnabled(false), want: wantStop()},
				{send: onRecordEnabled(true), want: wantNone()},
				{send: onRecordEnabled(false), want: wantNone()},
				// 门控最终是关的：收尾完成不恢复。
				{send: onSessionFinished(), want: wantNone()},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runPolicySteps(t, true, tc.steps)
		})
	}
}
