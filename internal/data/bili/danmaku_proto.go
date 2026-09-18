// danmaku_proto.go 实现弹幕 websocket 的二进制包协议：16 字节包头的打包
// 与解包、zlib/brotli 嵌套解压，以及进房认证包的构造与握手校验。
package bili

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/gorilla/websocket"
)

// https://her-cat.com/posts/2021/04/01/workerman-to-access-bilibili-barrage-protocol/
// B 站弹幕 websocket 二进制协议常量（均为大端序）。
// 每个数据帧以 16 字节包头开始，布局见 packPacket / parseDanmakuPacket：
//
//	[0:4]   包总长（含包头）
//	[4:6]   包头长度（固定 16）
//	[6:8]   压缩协议版本（0/1=明文, 2=zlib, 3=brotli）
//	[8:12]  操作码
//	[12:16] 序列号（固定为 1）
const (
	packetHeaderLength = 16
	// operationHeartbeat 客户端 → 服务器：心跳保活包（空载荷）。
	// 服务器回以人气值包（op 3），本实现不关心，在 unpackMessages 中被跳过。
	operationHeartbeat = 2
	// operationMessage 服务器 → 客户端：弹幕与房间消息。
	// 一帧内可能合并多个包，且可能整体被压缩，由 unpackMessages 递归解包。
	operationMessage = 5
	// operationAuth 客户端 → 服务器：进房认证包，JSON 载荷见 buildAuthBody。
	operationAuth = 7
	// operationAuthReply 服务器 → 客户端：认证结果，code=0 表示成功（见 waitAuthSuccess）。
	operationAuthReply = 8
)

// buildAuthBody 构造认证包（op 7）的 JSON 载荷：
// uid 与房间绑定、token 来自 getDanmuInfo、protover 声明压缩协议、
// buvid 为设备指纹。platform/type 模拟 web 播放器的固定取值。
func buildAuthBody(roomID int64, token string, protover int, buvid, cookie string) []byte {
	body := map[string]any{
		"uid":      danmakuAuthUID(cookie),
		"roomid":   roomID,
		"protover": protover,
		"platform": "web",
		"type":     2,
		"key":      token,
		"buvid":    buvid,
	}
	data, _ := json.Marshal(body)
	return data
}

// danmakuAuthUID 返回弹幕认证包使用的 uid：登录后 getDanmuInfo 返回的
// token 与账号绑定，弹幕服务器要求认证包 uid 与 cookie 身份一致，
// 否则直接断开连接；未登录（或 cookie 缺 DedeUserID）时为 0（匿名）。
func danmakuAuthUID(cookie string) int64 {
	uid, err := strconv.ParseInt(cookieValue(cookie, "DedeUserID"), 10, 64)
	if err != nil {
		return 0
	}
	return uid
}

// waitAuthSuccess 等待并校验认证回复（op 8）：读取第一个入站帧，
// 确认操作码为认证回复且 code=0。5 秒内无回复视为认证失败。
// 成功后清空读超时，交给 readLoop 接管超时控制。
func waitAuthSuccess(conn *websocket.Conn) error {
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	defer conn.SetReadDeadline(time.Time{})

	_, data, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	operation, body, err := parseDanmakuPacket(data)
	if err != nil {
		return err
	}
	if operation != operationAuthReply {
		return fmt.Errorf("unexpected danmaku auth operation %d", operation)
	}
	var reply struct {
		Code int `json:"code"`
	}
	if err := json.Unmarshal(body, &reply); err != nil {
		return err
	}
	if reply.Code != 0 {
		return fmt.Errorf("danmaku auth failed code=%d", reply.Code)
	}
	return nil
}

// parseDanmakuPacket 解析单个数据帧的包头，返回操作码与包体切片。
// 仅用于认证握手阶段（waitAuthSuccess）；常规消息流走 unpackMessages。
func parseDanmakuPacket(data []byte) (uint32, []byte, error) {
	if len(data) < packetHeaderLength {
		return 0, nil, stderrors.New("invalid danmaku packet length")
	}
	packetLength := int(binary.BigEndian.Uint32(data[0:4]))
	headerLength := int(binary.BigEndian.Uint16(data[4:6]))
	if packetLength < headerLength || packetLength > len(data) {
		return 0, nil, stderrors.New("invalid danmaku packet length")
	}
	operation := binary.BigEndian.Uint32(data[8:12])
	return operation, data[headerLength:packetLength], nil
}

// packPacket 按弹幕协议打包一个数据帧（16 字节包头 + 载荷），
// 用于发送心跳（空载荷）和认证包。序列号字段固定填 1。
func packPacket(operation uint32, protocolVersion uint16, body []byte) []byte {
	length := packetHeaderLength + len(body)
	packet := make([]byte, length)
	binary.BigEndian.PutUint32(packet[0:4], uint32(length))
	binary.BigEndian.PutUint16(packet[4:6], packetHeaderLength)
	binary.BigEndian.PutUint16(packet[6:8], protocolVersion)
	binary.BigEndian.PutUint32(packet[8:12], operation)
	binary.BigEndian.PutUint32(packet[12:16], 1)
	copy(packet[packetHeaderLength:], body)
	return packet
}

// unpackMessages 把一帧字节流还原成逐条的 JSON 消息：
// 一帧可能首尾相连地合并多个包（循环切片）；压缩包（协议版本
// 2=zlib / 3=brotli）先解压，解压结果本身又是同样的包序列，
// 因此递归解包。非消息包（如人气值包）直接跳过。
func unpackMessages(data []byte) ([]json.RawMessage, error) {
	var messages []json.RawMessage
	for len(data) >= packetHeaderLength {
		packetLength := int(binary.BigEndian.Uint32(data[0:4]))
		headerLength := int(binary.BigEndian.Uint16(data[4:6]))
		protocolVersion := binary.BigEndian.Uint16(data[6:8])
		operation := binary.BigEndian.Uint32(data[8:12])
		if packetLength < headerLength || packetLength > len(data) {
			return nil, stderrors.New("invalid danmaku packet length")
		}
		body := data[headerLength:packetLength]
		if operation == operationMessage {
			switch protocolVersion {
			case 0, 1:
				messages = append(messages, json.RawMessage(body))
			case 2:
				decompressed, err := zlibInflate(body)
				if err != nil {
					return nil, err
				}
				nested, err := unpackMessages(decompressed)
				if err != nil {
					return nil, err
				}
				messages = append(messages, nested...)
			case 3:
				decompressed, err := brotliInflate(body)
				if err != nil {
					return nil, err
				}
				nested, err := unpackMessages(decompressed)
				if err != nil {
					return nil, err
				}
				messages = append(messages, nested...)
			default:
				return nil, fmt.Errorf("unsupported danmaku protocol version %d", protocolVersion)
			}
		}
		data = data[packetLength:]
	}
	return messages, nil
}

func zlibInflate(data []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func brotliInflate(data []byte) ([]byte, error) {
	return io.ReadAll(brotli.NewReader(bytes.NewReader(data)))
}

// shuffledStrings 返回打乱顺序的副本（不改原切片），供 dial 使用。
func shuffledStrings(items []string) []string {
	shuffled := append([]string(nil), items...)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled
}
