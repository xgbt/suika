package biz

import "testing"

// sessionPolicy 的测试以纯事件序列驱动策略模块：无 goroutine、无时钟、
// 无 fake。每个用例是一串"投递事件 → 断言决策"的步骤，与
// .scratch/session-policy/spec.md 决策矩阵逐行对应；阶段（idle /
// running / finishing）不直接断言，而是通过后续事件的决策体现。

type policyStep struct {
	send func(*sessionPolicy) policyDecision
	want policyDecision
}

func roomInfoArrived(info *RoomInfo) func(*sessionPolicy) policyDecision {
	return func(p *sessionPolicy) policyDecision { return p.RoomInfoArrived(info) }
}

func enabledFlipped(enabled bool) func(*sessionPolicy) policyDecision {
	return func(p *sessionPolicy) policyDecision { return p.EnabledFlipped(enabled) }
}

func sessionFinished() func(*sessionPolicy) policyDecision {
	return func(p *sessionPolicy) policyDecision { return p.SessionFinished() }
}

func wantStart(info *RoomInfo) policyDecision { return policyDecision{kind: decisionStart, info: info} }
func wantStop() policyDecision                { return policyDecision{kind: decisionStop} }
func wantNone() policyDecision                { return policyDecision{} }

func runPolicySteps(t *testing.T, initialEnabled bool, steps []policyStep) {
	t.Helper()
	p := newSessionPolicy(initialEnabled)
	for i, step := range steps {
		if got := step.send(p); got != step.want {
			t.Fatalf("step %d: decision = %+v, want %+v", i, got, step.want)
		}
	}
}

// TestSessionPolicyRoomInfoArrived 覆盖决策矩阵中"room info arrived"的
// 全部行（含 latest 始终更新一行经由后续事件间接验证）。
func TestSessionPolicyRoomInfoArrived(t *testing.T) {
	live := &RoomInfo{RoomID: 42, Live: true, Title: "on-air"}
	liveAgain := &RoomInfo{RoomID: 42, Live: true, Title: "still-on-air"}
	offline := &RoomInfo{RoomID: 42, Title: "preparing"}

	cases := []struct {
		name           string
		initialEnabled bool
		steps          []policyStep
	}{
		{
			name:           "live enabled idle starts",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
			},
		},
		{
			name:           "live enabled running does not start again",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: roomInfoArrived(liveAgain), want: wantNone()},
			},
		},
		{
			name:           "live enabled finishing does not start",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: roomInfoArrived(offline), want: wantStop()},
				{send: roomInfoArrived(liveAgain), want: wantNone()},
			},
		},
		{
			name:           "live disabled does not start",
			initialEnabled: false,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantNone()},
			},
		},
		{
			name:           "live disabled finishing does not start",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: enabledFlipped(false), want: wantStop()},
				// live · disabled · finishing：收尾期间到达的开播信息
				// 只更新 latest，不启动会话。
				{send: roomInfoArrived(liveAgain), want: wantNone()},
			},
		},
		{
			name:           "not live running stops",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: roomInfoArrived(offline), want: wantStop()},
			},
		},
		{
			name:           "not live finishing gives no redundant stop",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: roomInfoArrived(offline), want: wantStop()},
				{send: roomInfoArrived(offline), want: wantNone()},
			},
		},
		{
			name:           "not live idle does nothing",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(offline), want: wantNone()},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runPolicySteps(t, tc.initialEnabled, tc.steps)
		})
	}
}

// TestSessionPolicyEnabledFlipped 覆盖决策矩阵中"enabled flipped"的全部
// 行；恢复标志的可观察后果（收尾后续录）由
// TestSessionPolicySessionFinished 与 TestSessionPolicyPreservedQuirks 验证。
func TestSessionPolicyEnabledFlipped(t *testing.T) {
	live := &RoomInfo{RoomID: 42, Live: true}
	offline := &RoomInfo{RoomID: 42}

	cases := []struct {
		name           string
		initialEnabled bool
		steps          []policyStep
	}{
		{
			name:           "flip on idle with live latest starts with latest",
			initialEnabled: false,
			steps: []policyStep{
				// 禁用期间到达的信息只更新 latest，不启动会话。
				{send: roomInfoArrived(live), want: wantNone()},
				{send: enabledFlipped(true), want: wantStart(live)},
			},
		},
		{
			name:           "flip on idle without any info does nothing",
			initialEnabled: false,
			steps: []policyStep{
				{send: enabledFlipped(true), want: wantNone()},
			},
		},
		{
			name:           "flip on idle with offline latest does nothing",
			initialEnabled: false,
			steps: []policyStep{
				{send: roomInfoArrived(offline), want: wantNone()},
				{send: enabledFlipped(true), want: wantNone()},
			},
		},
		{
			name:           "flip on while already on is absorbed",
			initialEnabled: true,
			steps: []policyStep{
				{send: enabledFlipped(true), want: wantNone()},
			},
		},
		{
			name:           "flip off running stops",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: enabledFlipped(false), want: wantStop()},
			},
		},
		{
			name:           "flip off finishing gives no redundant stop",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: roomInfoArrived(offline), want: wantStop()},
				{send: enabledFlipped(false), want: wantNone()},
			},
		},
		{
			name:           "flip off idle does nothing",
			initialEnabled: true,
			steps: []policyStep{
				{send: enabledFlipped(false), want: wantNone()},
			},
		},
		{
			name:           "coalesced disable-enable nets out during running",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				// 禁用→启用在一次信号内合并：值未变，决策为无操作，
				// 会话继续录制（阶段仍为 running，下播仍会停止）。
				{send: enabledFlipped(true), want: wantNone()},
				{send: roomInfoArrived(offline), want: wantStop()},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runPolicySteps(t, tc.initialEnabled, tc.steps)
		})
	}
}

// TestSessionPolicySessionFinished 覆盖决策矩阵中"session finished"的行。
func TestSessionPolicySessionFinished(t *testing.T) {
	live := &RoomInfo{RoomID: 42, Live: true}
	offline := &RoomInfo{RoomID: 42}

	cases := []struct {
		name           string
		initialEnabled bool
		steps          []policyStep
	}{
		{
			name:           "natural end goes idle",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				// 自然结束（未经停止）：无决策，阶段回到空闲，
				// 下一次开播信息可再次启动会话。
				{send: sessionFinished(), want: wantNone()},
				{send: roomInfoArrived(live), want: wantStart(live)},
			},
		},
		{
			name:           "stopped session finishes to idle without resume",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: roomInfoArrived(offline), want: wantStop()},
				{send: sessionFinished(), want: wantNone()},
			},
		},
		{
			name:           "resume flag set during finishing starts on finish",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: enabledFlipped(false), want: wantStop()},
				{send: enabledFlipped(true), want: wantNone()},
				{send: sessionFinished(), want: wantStart(live)},
			},
		},
		{
			name:           "resume flag cleared after resume",
			initialEnabled: true,
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: enabledFlipped(false), want: wantStop()},
				{send: enabledFlipped(true), want: wantNone()},
				{send: sessionFinished(), want: wantStart(live)},
				{send: sessionFinished(), want: wantNone()},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runPolicySteps(t, tc.initialEnabled, tc.steps)
		})
	}
}

// TestSessionPolicyPreservedQuirks 锁定规格要求原样保留的三个既有行为，
// 即便它们看起来像缺陷。
func TestSessionPolicyPreservedQuirks(t *testing.T) {
	live := &RoomInfo{RoomID: 42, Live: true, Title: "stale"}
	liveFresh := &RoomInfo{RoomID: 42, Live: true, Title: "fresh"}
	offline := &RoomInfo{RoomID: 42}

	cases := []struct {
		name  string
		steps []policyStep
	}{
		{
			name: "stale-live resume: enable during finishing resumes even if stream died",
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: enabledFlipped(false), want: wantStop()},
				{send: enabledFlipped(true), want: wantNone()},
				// 收尾完成：尽管流可能已死，latest 仍说在播，照录。
				// 新会话随后会在开流失败时优雅结束（由录制循环负责）。
				{send: sessionFinished(), want: wantStart(live)},
				// 新鲜的下播信息到达才真正停下。
				{send: roomInfoArrived(offline), want: wantStop()},
				{send: sessionFinished(), want: wantNone()},
			},
		},
		{
			name: "resume uses the freshest known room info",
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: enabledFlipped(false), want: wantStop()},
				// 收尾期间到达的新信息更新 latest。
				{send: roomInfoArrived(liveFresh), want: wantNone()},
				{send: enabledFlipped(true), want: wantNone()},
				{send: sessionFinished(), want: wantStart(liveFresh)},
			},
		},
		{
			name: "flip off after enable clears the resume flag",
			steps: []policyStep{
				{send: roomInfoArrived(live), want: wantStart(live)},
				{send: enabledFlipped(false), want: wantStop()},
				{send: enabledFlipped(true), want: wantNone()},
				{send: enabledFlipped(false), want: wantNone()},
				// 标志已被清除：收尾完成不再恢复。
				{send: sessionFinished(), want: wantNone()},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runPolicySteps(t, true, tc.steps)
		})
	}
}
