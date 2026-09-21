package bili

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"
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

// --- 认证载荷 ---

// 登录后 getDanmuInfo 返回的 token 与账号绑定，弹幕服务器要求认证包
// uid 与 cookie 身份一致，否则直接断开连接（表现为 close 1006）。
// 回归测试保证认证包 uid 跟随当前生效的登录态。
func TestBuildAuthBodyUIDFollowsCookie(t *testing.T) {
	cookie := "SESSDATA=fake-sessdata; bili_jct=fake-jct; DedeUserID=123456; DedeUserID__ckMd5=fake-md5; sid=fake-sid"
	var body struct {
		UID    int64  `json:"uid"`
		RoomID int64  `json:"roomid"`
		Key    string `json:"key"`
		Buvid  string `json:"buvid"`
	}
	if err := json.Unmarshal(buildAuthBody(42, "token-x", 3, "buvid3-x", cookie), &body); err != nil {
		t.Fatalf("unmarshal auth body: %v", err)
	}
	if body.UID != 123456 {
		t.Fatalf("uid = %d, want 123456（登录态认证包必须携带 DedeUserID）", body.UID)
	}
	if body.RoomID != 42 || body.Key != "token-x" || body.Buvid != "buvid3-x" {
		t.Fatalf("auth body fields wrong: %+v", body)
	}
}

func TestBuildAuthBodyUIDAnonymousWithoutCookie(t *testing.T) {
	var body struct {
		UID int64 `json:"uid"`
	}
	if err := json.Unmarshal(buildAuthBody(42, "token-x", 3, "buvid3-x", ""), &body); err != nil {
		t.Fatalf("unmarshal auth body: %v", err)
	}
	if body.UID != 0 {
		t.Fatalf("uid = %d, want 0（未登录时保持匿名）", body.UID)
	}
}

func TestDanmakuAuthUID(t *testing.T) {
	cases := []struct {
		name   string
		cookie string
		want   int64
	}{
		{"空 cookie", "", 0},
		{"无 DedeUserID", "SESSDATA=x; sid=y", 0},
		{"DedeUserID 非数字", "DedeUserID=abc", 0},
		{"正常登录 cookie", "SESSDATA=x; DedeUserID=42; sid=y", 42},
	}
	for _, tc := range cases {
		if got := danmakuAuthUID(tc.cookie); got != tc.want {
			t.Errorf("%s: danmakuAuthUID = %d, want %d", tc.name, got, tc.want)
		}
	}
}
