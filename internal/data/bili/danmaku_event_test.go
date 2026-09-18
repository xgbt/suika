package bili

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"suika/internal/biz"
)

var receivedAt = time.Date(2026, 8, 19, 20, 0, 0, 0, time.UTC)

// assertEventEqual 逐字段比较事件（含 []byte 字段，结构体整体不可比较）。
func assertEventEqual(t *testing.T, got *biz.DanmakuEvent, want biz.DanmakuEvent) {
	t.Helper()
	if got == nil {
		t.Fatal("event is nil")
	}
	if !bytes.Equal(got.Raw, want.Raw) {
		t.Fatalf("Raw = %s, want %s", got.Raw, want.Raw)
	}
	gotWithoutRaw, wantWithoutRaw := *got, want
	gotWithoutRaw.Raw, wantWithoutRaw.Raw = nil, nil
	if !reflect.DeepEqual(gotWithoutRaw, wantWithoutRaw) {
		t.Fatalf("event = %+v, want %+v", *got, want)
	}
}

func TestParseDanmakuEvent(t *testing.T) {
	raw := json.RawMessage(`{"cmd":"DANMU_MSG","info":[[0,6,25,16777215,1755633600123],"你好世界",[42,"某用户"]]}`)

	ev := parseDanmakuEvent(raw, receivedAt)
	if ev == nil {
		t.Fatal("parseDanmakuEvent returned nil")
	}
	want := biz.DanmakuEvent{
		TS: receivedAt, SendTS: 1755633600123, Type: biz.EventDanmaku, Text: "你好世界", Raw: raw,
		UID: 42, Uname: "某用户", Mode: 6, Color: 16777215,
	}
	assertEventEqual(t, ev, want)
}

func TestParseDanmakuEventSendTs(t *testing.T) {
	// 发送时刻缺失、非数字或非正数时保持未知（0），不影响其余字段。
	cases := map[string]struct {
		raw  json.RawMessage
		want int64
	}{
		"meta shorter than 5":  {json.RawMessage(`{"info":[[0,1,0,0],"text",[1,"n"]]}`), 0},
		"send ts zero":         {json.RawMessage(`{"info":[[0,1,0,0,0],"text",[1,"n"]]}`), 0},
		"send ts negative":     {json.RawMessage(`{"info":[[0,1,0,0,-5],"text",[1,"n"]]}`), 0},
		"send ts not a number": {json.RawMessage(`{"info":[[0,1,0,0,"x"],"text",[1,"n"]]}`), 0},
		"send ts as string":    {json.RawMessage(`{"info":[[0,1,0,0,"1755633600123"],"text",[1,"n"]]}`), 1755633600123},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ev := parseDanmakuEvent(tc.raw, receivedAt)
			if ev == nil {
				t.Fatal("parseDanmakuEvent returned nil")
			}
			if ev.SendTS != tc.want {
				t.Fatalf("SendTs = %d, want %d", ev.SendTS, tc.want)
			}
			if ev.Text != "text" || ev.UID != 1 {
				t.Fatalf("other fields broken: %+v", ev)
			}
		})
	}
}

func TestParseDanmakuEventStringUID(t *testing.T) {
	raw := json.RawMessage(`{"info":[[0,1,0,0],"text",["123","name"]]}`)

	ev := parseDanmakuEvent(raw, receivedAt)
	if ev == nil || ev.UID != 123 {
		t.Fatalf("event = %+v, want UID parsed from string 123", ev)
	}
}

func TestParseDanmakuEventKeepsDefaultModeOnZero(t *testing.T) {
	raw := json.RawMessage(`{"info":[[0,0,0,0],"text",[1,"n"]]}`)

	ev := parseDanmakuEvent(raw, receivedAt)
	if ev == nil || ev.Mode != 1 {
		t.Fatalf("event = %+v, want default Mode 1 when meta mode is 0", ev)
	}
}

func TestParseDanmakuEventRejectsInvalid(t *testing.T) {
	cases := map[string]json.RawMessage{
		"bad json":   json.RawMessage(`{`),
		"short info": json.RawMessage(`{"info":[[0,1,0,0],"text"]}`),
		"empty text": json.RawMessage(`{"info":[[0,1,0,0],"",[1,"n"]]}`),
	}
	for name, raw := range cases {
		if ev := parseDanmakuEvent(raw, receivedAt); ev != nil {
			t.Fatalf("%s: got %+v, want nil", name, ev)
		}
	}
}

func TestParseGiftEvent(t *testing.T) {
	raw := json.RawMessage(`{"cmd":"SEND_GIFT","data":{"uid":7,"uname":"赠送者","giftName":"辣条","num":3,"price":100,"coin_type":"silver"}}`)

	ev := parseGiftEvent(raw, receivedAt)
	if ev == nil {
		t.Fatal("parseGiftEvent returned nil")
	}
	want := biz.DanmakuEvent{
		TS: receivedAt, Type: biz.EventGift, Raw: raw,
		UID: 7, Uname: "赠送者", GiftName: "辣条", Num: 3, Price: 100, CoinType: "silver",
	}
	assertEventEqual(t, ev, want)
	if parseGiftEvent(json.RawMessage(`{"data":[]}`), receivedAt) != nil {
		t.Fatal("want nil for malformed gift payload")
	}
}

func TestParseSuperChatEvent(t *testing.T) {
	raw := json.RawMessage(`{"cmd":"SUPER_CHAT_MESSAGE","data":{"uid":8,"user_info":{"uname":"醒目留言"},"price":50,"message":"主播好","time":120}}`)

	ev := parseSuperChatEvent(raw, receivedAt)
	if ev == nil {
		t.Fatal("parseSuperChatEvent returned nil")
	}
	want := biz.DanmakuEvent{
		TS: receivedAt, Type: biz.EventSuperChat, Raw: raw,
		UID: 8, Uname: "醒目留言", Price: 50, Text: "主播好", Duration: 120,
	}
	assertEventEqual(t, ev, want)
	if parseSuperChatEvent(json.RawMessage(`{"data":[]}`), receivedAt) != nil {
		t.Fatal("want nil for malformed super chat payload")
	}
}

func TestParseGuardEvent(t *testing.T) {
	raw := json.RawMessage(`{"cmd":"GUARD_BUY","data":{"uid":9,"username":"舰长","guard_level":3,"num":1}}`)

	ev := parseGuardEvent(raw, receivedAt)
	if ev == nil {
		t.Fatal("parseGuardEvent returned nil")
	}
	want := biz.DanmakuEvent{
		TS: receivedAt, Type: biz.EventGuard, Raw: raw,
		UID: 9, Uname: "舰长", Level: 3, Num: 1,
	}
	assertEventEqual(t, ev, want)
	if parseGuardEvent(json.RawMessage(`{"data":[]}`), receivedAt) != nil {
		t.Fatal("want nil for malformed guard payload")
	}
}

func TestParseEntryEffectEvent(t *testing.T) {
	raw := json.RawMessage(`{"cmd":"ENTRY_EFFECT","data":{"uid":10,"copy_writing":"欢迎 <%大佬%> 进入房间"}}`)

	ev := parseEntryEffectEvent(raw, receivedAt)
	if ev == nil {
		t.Fatal("parseEntryEffectEvent returned nil")
	}
	want := biz.DanmakuEvent{
		TS: receivedAt, Type: biz.EventEntryEffect, Raw: raw,
		UID: 10, Text: "欢迎 <%大佬%> 进入房间",
	}
	assertEventEqual(t, ev, want)
	if parseEntryEffectEvent(json.RawMessage(`{"data":[]}`), receivedAt) != nil {
		t.Fatal("want nil for malformed entry effect payload")
	}
}

func TestToInt64(t *testing.T) {
	if got := toInt64(float64(42)); got != 42 {
		t.Fatalf("toInt64(float64) = %d, want 42", got)
	}
	if got := toInt64("123"); got != 123 {
		t.Fatalf(`toInt64("123") = %d, want 123`, got)
	}
	if got := toInt64("abc"); got != 0 {
		t.Fatalf(`toInt64("abc") = %d, want 0`, got)
	}
	if got := toInt64(nil); got != 0 {
		t.Fatalf("toInt64(nil) = %d, want 0", got)
	}
}
