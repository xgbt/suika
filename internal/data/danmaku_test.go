package data

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/andybalholm/brotli"

	"suika/internal/biz"
)

// --- 包编解码 ---

func TestPackParseDanmakuPacketRoundTrip(t *testing.T) {
	body := []byte(`{"cmd":"DANMU_MSG"}`)
	packet := packPacket(operationMessage, 0, body)

	operation, parsed, err := parseDanmakuPacket(packet)
	if err != nil {
		t.Fatalf("parseDanmakuPacket: %v", err)
	}
	if operation != operationMessage {
		t.Fatalf("operation = %d, want %d", operation, operationMessage)
	}
	if !bytes.Equal(parsed, body) {
		t.Fatalf("body = %q, want %q", parsed, body)
	}
}

func TestPackParseHeartbeatEmptyBody(t *testing.T) {
	packet := packPacket(operationHeartbeat, 1, nil)

	operation, parsed, err := parseDanmakuPacket(packet)
	if err != nil {
		t.Fatalf("parseDanmakuPacket: %v", err)
	}
	if operation != operationHeartbeat || len(parsed) != 0 {
		t.Fatalf("got (%d, %q), want (%d, empty)", operation, parsed, operationHeartbeat)
	}
}

func TestParseDanmakuPacketRejectsShortInput(t *testing.T) {
	if _, _, err := parseDanmakuPacket([]byte{1, 2, 3}); err == nil {
		t.Fatal("want error for input shorter than the header")
	}
}

func TestParseDanmakuPacketRejectsBadLength(t *testing.T) {
	tooLong := packPacket(operationMessage, 0, []byte("x"))
	// 声称的包长超过实际数据。
	tooLong[3] += 8
	if _, _, err := parseDanmakuPacket(tooLong[:packetHeaderLength]); err == nil {
		t.Fatal("want error when packet length exceeds data")
	}

	// 包长小于头长。
	packet := packPacket(operationMessage, 0, []byte("x"))
	packet[4], packet[5] = 0, byte(packetHeaderLength+2)
	packet[0], packet[1], packet[2], packet[3] = 0, 0, 0, byte(packetHeaderLength)
	if _, _, err := parseDanmakuPacket(packet); err == nil {
		t.Fatal("want error when packet length is below header length")
	}
}

func TestUnpackMessagesPlain(t *testing.T) {
	first, second := []byte(`{"cmd":"A"}`), []byte(`{"cmd":"B"}`)
	data := append(packPacket(operationMessage, 0, first), packPacket(operationMessage, 0, second)...)

	messages, err := unpackMessages(data)
	if err != nil {
		t.Fatalf("unpackMessages: %v", err)
	}
	if len(messages) != 2 || string(messages[0]) != string(first) || string(messages[1]) != string(second) {
		t.Fatalf("messages = %v, want [%s %s] in order", messages, first, second)
	}
}

func TestUnpackMessagesIgnoresNonMessage(t *testing.T) {
	body := []byte(`{"cmd":"A"}`)
	data := append(packPacket(operationHeartbeat, 0, nil), packPacket(operationMessage, 0, body)...)
	data = append(data, packPacket(operationAuthReply, 0, nil)...)

	messages, err := unpackMessages(data)
	if err != nil {
		t.Fatalf("unpackMessages: %v", err)
	}
	if len(messages) != 1 || string(messages[0]) != string(body) {
		t.Fatalf("messages = %v, want only the operationMessage body", messages)
	}
}

func TestUnpackMessagesZlibNested(t *testing.T) {
	inner := []byte(`{"cmd":"INNER"}`)
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	if _, err := writer.Write(packPacket(operationMessage, 0, inner)); err != nil {
		t.Fatalf("zlib write: %v", err)
	}
	writer.Close()

	messages, err := unpackMessages(packPacket(operationMessage, 2, compressed.Bytes()))
	if err != nil {
		t.Fatalf("unpackMessages: %v", err)
	}
	if len(messages) != 1 || string(messages[0]) != string(inner) {
		t.Fatalf("messages = %v, want the zlib-nested message", messages)
	}
}

func TestUnpackMessagesBrotliNested(t *testing.T) {
	inner := []byte(`{"cmd":"INNER"}`)
	var compressed bytes.Buffer
	writer := brotli.NewWriter(&compressed)
	if _, err := writer.Write(packPacket(operationMessage, 0, inner)); err != nil {
		t.Fatalf("brotli write: %v", err)
	}
	writer.Close()

	messages, err := unpackMessages(packPacket(operationMessage, 3, compressed.Bytes()))
	if err != nil {
		t.Fatalf("unpackMessages: %v", err)
	}
	if len(messages) != 1 || string(messages[0]) != string(inner) {
		t.Fatalf("messages = %v, want the brotli-nested message", messages)
	}
}

func TestUnpackMessagesRejectsUnsupportedProtover(t *testing.T) {
	_, err := unpackMessages(packPacket(operationMessage, 7, []byte("{}")))
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("err = %v, want unsupported protocol version error", err)
	}
}

func TestUnpackMessagesRejectsTruncatedPacket(t *testing.T) {
	packet := packPacket(operationMessage, 0, []byte("payload"))
	if _, err := unpackMessages(packet[:packetHeaderLength+2]); err == nil {
		t.Fatal("want error for truncated packet")
	}
}

// --- 压缩 ---

func TestZlibInflateRoundTrip(t *testing.T) {
	original := []byte("danmaku over zlib")
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	if _, err := writer.Write(original); err != nil {
		t.Fatalf("zlib write: %v", err)
	}
	writer.Close()

	got, err := zlibInflate(compressed.Bytes())
	if err != nil {
		t.Fatalf("zlibInflate: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("got %q, want %q", got, original)
	}

	if _, err := zlibInflate([]byte("not zlib")); err == nil {
		t.Fatal("want error for non-zlib input")
	}
}

func TestBrotliInflateRoundTrip(t *testing.T) {
	original := []byte("danmaku over brotli")
	var compressed bytes.Buffer
	writer := brotli.NewWriter(&compressed)
	if _, err := writer.Write(original); err != nil {
		t.Fatalf("brotli write: %v", err)
	}
	writer.Close()

	got, err := brotliInflate(compressed.Bytes())
	if err != nil {
		t.Fatalf("brotliInflate: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("got %q, want %q", got, original)
	}

	if _, err := brotliInflate([]byte{0xff, 0xfe, 0xfd}); err == nil {
		t.Fatal("want error for non-brotli input")
	}
}

// --- 事件解析 ---

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
	raw := json.RawMessage(`{"cmd":"DANMU_MSG","info":[[0,6,25,16777215],"你好世界",[42,"某用户"]]}`)

	ev := parseDanmakuEvent(raw, receivedAt)
	if ev == nil {
		t.Fatal("parseDanmakuEvent returned nil")
	}
	want := biz.DanmakuEvent{
		Ts: receivedAt, Type: biz.EventDanmaku, Text: "你好世界", Raw: raw,
		UID: 42, Uname: "某用户", Mode: 6, Color: 16777215,
	}
	assertEventEqual(t, ev, want)
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
		Ts: receivedAt, Type: biz.EventGift, Raw: raw,
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
		Ts: receivedAt, Type: biz.EventSuperChat, Raw: raw,
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
		Ts: receivedAt, Type: biz.EventGuard, Raw: raw,
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
		Ts: receivedAt, Type: biz.EventEntryEffect, Raw: raw,
		UID: 10, Text: "欢迎 <%大佬%> 进入房间",
	}
	assertEventEqual(t, ev, want)
	if parseEntryEffectEvent(json.RawMessage(`{"data":[]}`), receivedAt) != nil {
		t.Fatal("want nil for malformed entry effect payload")
	}
}

func TestParseInteractEvent(t *testing.T) {
	raw := json.RawMessage(`{"cmd":"INTERACT_WORD","data":{"uid":11,"uname":"路过的人"}}`)

	ev := parseInteractEvent(raw, receivedAt)
	if ev == nil {
		t.Fatal("parseInteractEvent returned nil")
	}
	want := biz.DanmakuEvent{
		Ts: receivedAt, Type: biz.EventInteract, Raw: raw,
		UID: 11, Uname: "路过的人",
	}
	assertEventEqual(t, ev, want)
	if parseInteractEvent(json.RawMessage(`{"data":[]}`), receivedAt) != nil {
		t.Fatal("want nil for malformed interact payload")
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
