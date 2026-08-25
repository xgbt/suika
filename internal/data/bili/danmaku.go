package bili

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"suika/internal/biz"

	"github.com/andybalholm/brotli"
	"github.com/go-kratos/kratos/v3/log"
	"github.com/gorilla/websocket"
)

// 以下常量分三组：通道缓冲容量、连接生命周期时序、弹幕二进制协议参数。
const (
	// danmakuEventBuffer 是弹幕事件通道（events）的缓冲容量。
	// 缓冲满时新事件直接丢弃（见 emit），因此容量要足够大，
	// 能吸收录制端短暂变慢（如切分段、收尾合并）时的事件峰值。
	danmakuEventBuffer = 4096

	// danmakuRoomStateUpdateBuffer 是房间状态通道（roomStateUpdates）的缓冲容量。
	// 消费方只关心最新状态，旧快照满时直接丢弃（见 pushRoomState），小缓冲足够。
	danmakuRoomStateUpdateBuffer = 16

	// danmakuHeartbeatInterval 是心跳包（op 2）的发送间隔。
	// 弹幕服务器要求客户端定期发送心跳保活，长时间无心跳会被服务端断开。
	danmakuHeartbeatInterval = 30 * time.Second

	// danmakuReadTimeout 用于掐掉半开连接：错过三轮心跳仍无入站帧则强制重连。
	// 取值为 3 × danmakuHeartbeatInterval。
	danmakuReadTimeout = 90 * time.Second

	// danmakuReconnectBase / danmakuReconnectMax 是重连退避的初始值与上限。
	// 每次连接失败后退避翻倍（见 run），封顶以避免在服务端故障或
	// 风控期间高频重连加重风险。
	danmakuReconnectBase = 2 * time.Second
	danmakuReconnectMax  = 30 * time.Second

	// https://her-cat.com/posts/2021/04/01/workerman-to-access-bilibili-barrage-protocol/
	// B 站弹幕 websocket 二进制协议常量（均为大端序）。
	// 每个数据帧以 16 字节包头开始，布局见 packPacket / parseDanmakuPacket：
	//   [0:4]   包总长（含包头）
	//   [4:6]   包头长度（固定 16）
	//   [6:8]   压缩协议版本（0/1=明文, 2=zlib, 3=brotli）
	//   [8:12]  操作码
	//   [12:16] 序列号（固定为 1）
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

// danmakuConn 是一个房间的常驻弹幕 websocket，同时服务于开播检测
// （RoomStateUpdates）和弹幕录制（Events）；内部自行重连，每次重连后
// 重新探测并推送房间状态，以补上断连期间错过的事件。
type danmakuConn struct {
	lc               *liveClient
	roomID           int64
	events           chan *biz.DanmakuEvent
	roomStateUpdates chan *biz.RoomInfo
	closed           chan struct{}
	closeOnce        sync.Once
}

func (c *danmakuConn) Events() <-chan *biz.DanmakuEvent { return c.events }

func (c *danmakuConn) RoomStateUpdates() <-chan *biz.RoomInfo { return c.roomStateUpdates }

// Close 标记连接关闭：后台的 run / readLoop / 各投递点都通过
// closed 通道感知退出。幂等，可安全多次调用。
func (c *danmakuConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

// isClosed 以非阻塞方式检查连接是否已被 Close。
func (c *danmakuConn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// run 持续重连，直到连接被关闭或 ctx 结束。
func (c *danmakuConn) run(ctx context.Context) {
	backoff := danmakuReconnectBase
	for {
		if c.isClosed() || ctx.Err() != nil {
			return
		}
		err := c.connectAndServe(ctx)
		if c.isClosed() || ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Warn("danmaku connection interrupted, reconnecting", "room", c.roomID, "backoff", backoff, "err", err)
		}
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-c.closed:
			t.Stop()
			return
		case <-t.C:
		}
		backoff = min(backoff*2, danmakuReconnectMax)
	}
}

// connectAndServe 完整地跑一次 websocket 连接尝试。
func (c *danmakuConn) connectAndServe(ctx context.Context) error {
	info, err := c.lc.danmuInfo(ctx, c.roomID)
	if err != nil {
		return fmt.Errorf("get danmu info: %w", err)
	}
	if len(info.addresses) == 0 {
		return stderrors.New("no danmaku websocket address")
	}

	conn, err := c.dial(ctx, shuffledStrings(info.addresses), info.token, info.buvid)
	if err != nil {
		return err
	}
	defer conn.Close()

	// （重）连接后重新探测房间状态，以补上断连期间错过的事件。
	c.pushRoomState(ctx)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.readLoop(conn)
	}()

	heartbeat := time.NewTicker(danmakuHeartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.closed:
			return nil
		case err := <-errCh:
			return err
		case <-heartbeat.C:
			if err := conn.WriteMessage(websocket.BinaryMessage, packPacket(operationHeartbeat, 1, nil)); err != nil {
				return err
			}
		}
	}
}

// dial 依次尝试打乱顺序后的主机列表；每个主机先试 protover 3（brotli），
// 失败再退回 2（zlib）。随机顺序避免所有房间固定打同一台边缘节点。
// 全部失败时返回最后一个错误。
func (c *danmakuConn) dial(ctx context.Context, addresses []string, token, buvid string) (*websocket.Conn, error) {
	var lastErr error
	for _, address := range addresses {
		// 优先 protover 3（brotli），其次 2（zlib）。
		for _, protover := range []int{3, 2} {
			conn, err := c.dialAndAuth(ctx, address, token, protover, buvid)
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		log.Warn("danmaku websocket host failed", "room", c.roomID, "address", address, "err", lastErr)
	}
	if lastErr == nil {
		return nil, stderrors.New("no danmaku websocket address")
	}
	return nil, lastErr
}

// dialAndAuth 完成一次「握手 + 认证」：带浏览器伪装头拨号到指定地址，
// 发送认证包并等待服务器确认。任一步失败都会关闭底层连接并返回错误，
// 由 dial 换下一地址/协议版本重试。
func (c *danmakuConn) dialAndAuth(ctx context.Context, address, token string, protover int, buvid string) (*websocket.Conn, error) {
	// cookie 快照同时用于握手头与认证包：登录后 getDanmuInfo 的 token
	// 与账号绑定，认证包的 uid 必须与 cookie 身份一致。
	cookie := c.lc.client.Cookie()
	header := http.Header{
		"User-Agent": {biliUserAgent},
		"Origin":     {"https://live.bilibili.com"},
		"Referer":    {fmt.Sprintf("https://live.bilibili.com/%d", c.roomID)},
	}
	if cookie != "" {
		header.Set("Cookie", cookie)
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, address, header)
	if err != nil {
		return nil, err
	}
	auth := buildAuthBody(c.roomID, token, protover, buvid, cookie)
	if err := conn.WriteMessage(websocket.BinaryMessage, packPacket(operationAuth, 1, auth)); err != nil {
		conn.Close()
		return nil, err
	}
	if err := waitAuthSuccess(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// readLoop 循环读取入站帧、解包并分发，每收到一帧就刷新读超时。
// 返回的错误由 connectAndServe 上报给 run 触发重连；返回 nil 表示
// 连接已被主动关闭。在独立 goroutine 中运行，与心跳写入并发
// （gorilla/websocket 允许一读一写并发）。
func (c *danmakuConn) readLoop(conn *websocket.Conn) error {
	if err := conn.SetReadDeadline(time.Now().Add(danmakuReadTimeout)); err != nil {
		return err
	}
	for {
		if c.isClosed() {
			return nil
		}
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if err := conn.SetReadDeadline(time.Now().Add(danmakuReadTimeout)); err != nil {
			return err
		}
		messages, err := unpackMessages(data)
		if err != nil {
			return err
		}
		receivedAt := time.Now()
		for _, raw := range messages {
			c.dispatch(context.Background(), raw, receivedAt)
		}
	}
}

// pushRoomState 重新探测房间，并把状态推送到房间状态更新通道。
func (c *danmakuConn) pushRoomState(ctx context.Context) {
	info, err := c.lc.GetRoomInfo(ctx, c.roomID)
	if err != nil {
		log.Warn("danmaku room state probe failed", "room", c.roomID, "err", err)
		return
	}
	select {
	case c.roomStateUpdates <- info:
	case <-c.closed:
	default:
		// 房间状态缓冲已满：下一个事件会传达最新状态。
	}
}

// dispatch 将一条解码后的弹幕消息路由到房间状态更新或事件通道。
func (c *danmakuConn) dispatch(ctx context.Context, raw json.RawMessage, receivedAt time.Time) {
	var head struct {
		Cmd string `json:"cmd"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return
	}
	cmd := head.Cmd
	if i := strings.IndexByte(cmd, ':'); i >= 0 {
		cmd = cmd[:i] // B 站会在 Cmd 后附加变体，如 DANMU_MSG:4:0:3:...
	}
	switch cmd {
	case "LIVE", "PREPARING", "ROUND", "ROOM_CHANGE":
		c.pushRoomState(ctx)
	case "DANMU_MSG":
		c.emit(parseDanmakuEvent(raw, receivedAt))
	case "SEND_GIFT":
		c.emit(parseGiftEvent(raw, receivedAt))
	case "SUPER_CHAT_MESSAGE":
		c.emit(parseSuperChatEvent(raw, receivedAt))
	case "GUARD_BUY":
		c.emit(parseGuardEvent(raw, receivedAt))
	case "ENTRY_EFFECT":
		c.emit(parseEntryEffectEvent(raw, receivedAt))
	}
}

// emit 非阻塞地投递事件；缓冲已满时丢弃（仅可能发生在无会话消费时）。
func (c *danmakuConn) emit(ev *biz.DanmakuEvent) {
	if ev == nil {
		return
	}
	select {
	case c.events <- ev:
	case <-c.closed:
	default:
	}
}

// --- 事件解析 ---

// parseDanmakuEvent 解析 DANMU_MSG：弹幕文本、发送者、模式与颜色。
// 载荷形状是数组（info[0]=弹幕元数据, info[1]=文本, info[2]=用户信息），
// 字段缺失或形状不符时返回 nil（该事件被丢弃）。
// info[0][4] 是平台侧的发送时刻（unix 毫秒），比接收时刻更贴近视频时间
// 轴（录制积压、网络抖动时差异明显），解析为 SendTs 供切片对齐；缺失或
// 非正数时保持 0（未知）。
func parseDanmakuEvent(raw json.RawMessage, receivedAt time.Time) *biz.DanmakuEvent {
	var m struct {
		Info []any `json:"info"`
	}
	if err := json.Unmarshal(raw, &m); err != nil || len(m.Info) < 3 {
		return nil
	}
	text, _ := m.Info[1].(string)
	if text == "" {
		return nil
	}
	ev := &biz.DanmakuEvent{Ts: receivedAt, Type: biz.EventDanmaku, Text: text, Raw: raw, Mode: 1}
	if user, ok := m.Info[2].([]any); ok && len(user) >= 2 {
		ev.UID = toInt64(user[0])
		ev.Uname, _ = user[1].(string)
	}
	if meta, ok := m.Info[0].([]any); ok {
		if len(meta) > 1 {
			if mode := int32(toInt64(meta[1])); mode > 0 {
				ev.Mode = mode
			}
		}
		if len(meta) > 3 {
			ev.Color = int32(toInt64(meta[3]))
		}
		if len(meta) > 4 {
			if sendTs := toInt64(meta[4]); sendTs > 0 {
				ev.SendTs = sendTs
			}
		}
	}
	return ev
}

// parseGiftEvent 解析 SEND_GIFT（礼物）事件。
func parseGiftEvent(raw json.RawMessage, receivedAt time.Time) *biz.DanmakuEvent {
	var m struct {
		Data struct {
			UID      int64  `json:"uid"`
			Uname    string `json:"uname"`
			GiftName string `json:"giftName"`
			Num      int32  `json:"num"`
			Price    int64  `json:"price"`
			CoinType string `json:"coin_type"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return &biz.DanmakuEvent{
		Ts: receivedAt, Type: biz.EventGift, Raw: raw,
		UID: m.Data.UID, Uname: m.Data.Uname, GiftName: m.Data.GiftName,
		Num: m.Data.Num, Price: m.Data.Price, CoinType: m.Data.CoinType,
	}
}

// parseSuperChatEvent 解析 SUPER_CHAT_MESSAGE（醒目留言）事件。
func parseSuperChatEvent(raw json.RawMessage, receivedAt time.Time) *biz.DanmakuEvent {
	var m struct {
		Data struct {
			UID      int64 `json:"uid"`
			UserInfo struct {
				Uname string `json:"uname"`
			} `json:"user_info"`
			Price   int64  `json:"price"`
			Message string `json:"message"`
			Time    int32  `json:"time"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return &biz.DanmakuEvent{
		Ts: receivedAt, Type: biz.EventSuperChat, Raw: raw,
		UID: m.Data.UID, Uname: m.Data.UserInfo.Uname,
		Price: m.Data.Price, Text: m.Data.Message, Duration: m.Data.Time,
	}
}

// parseGuardEvent 解析 GUARD_BUY（舰长/提督/总督购买）事件。
func parseGuardEvent(raw json.RawMessage, receivedAt time.Time) *biz.DanmakuEvent {
	var m struct {
		Data struct {
			UID        int64  `json:"uid"`
			Username   string `json:"username"`
			GuardLevel int32  `json:"guard_level"`
			Num        int32  `json:"num"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return &biz.DanmakuEvent{
		Ts: receivedAt, Type: biz.EventGuard, Raw: raw,
		UID: m.Data.UID, Uname: m.Data.Username, Level: m.Data.GuardLevel, Num: m.Data.Num,
	}
}

// parseEntryEffectEvent 解析 ENTRY_EFFECT（高等级用户进场特效）事件。
func parseEntryEffectEvent(raw json.RawMessage, receivedAt time.Time) *biz.DanmakuEvent {
	var m struct {
		Data struct {
			UID         int64  `json:"uid"`
			CopyWriting string `json:"copy_writing"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return &biz.DanmakuEvent{
		Ts: receivedAt, Type: biz.EventEntryEffect, Raw: raw,
		UID: m.Data.UID, Text: m.Data.CopyWriting,
	}
}

// toInt64 把 B 站载荷中类型不稳定的数字字段统一转成 int64：
// JSON 数字解析为 float64，部分字段则是数字字符串；其余类型返回 0。
func toInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case string:
		parsed, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}

// --- 弹幕 websocket 二进制协议（移植自 hikami-go）---

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

// --- getDanmuInfo（token + 主机列表），带 -352 重试与旧接口兜底 ---

// danmuInfo 是弹幕连接所需的认证三要素：
// token（进房鉴权）、addresses（wss 主机列表）、buvid（设备指纹 buvid3）。
type danmuInfo struct {
	token     string
	addresses []string
	buvid     string
}

// danmuInfo 返回房间的弹幕认证信息：token、主机列表与 buvid3。
func (lc *liveClient) danmuInfo(ctx context.Context, roomID int64) (*danmuInfo, error) {
	var info *danmuInfo
	attempt := func(ctx context.Context) (int, error) {
		cookie := lc.client.injectAntiRisk(ctx)
		endpoint := liveAPIBase + "/xlive/web-room/v1/index/getDanmuInfo?id=" + strconv.FormatInt(roomID, 10) + "&type=0"
		var raw struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Data    struct {
				Token    string `json:"token"`
				HostList []struct {
					Host    string `json:"host"`
					WSSPort int    `json:"wss_port"`
				} `json:"host_list"`
			} `json:"data"`
		}
		if err := lc.client.fetchJSON(ctx, lc.client.signURL(endpoint), roomID, cookie, &raw); err != nil {
			return 0, err
		}
		parsed := &danmuInfo{token: raw.Data.Token}
		for _, host := range raw.Data.HostList {
			if host.Host != "" && host.WSSPort > 0 {
				parsed.addresses = append(parsed.addresses, fmt.Sprintf("wss://%s:%d/sub", host.Host, host.WSSPort))
			}
		}
		info = parsed
		return raw.Code, nil
	}
	// 旧版 getConf 接口不需要 WBI 签名，双重 -352 时兜底。
	var confInfo *danmuInfo
	fallback := func(ctx context.Context) (int, error) {
		conf, err := lc.danmuConf(ctx, roomID)
		if err != nil {
			return 0, err
		}
		if conf.token == "" {
			return 0, stderrors.New("legacy getConf returned empty token")
		}
		confInfo = conf
		return 0, nil
	}

	code, err := lc.risk.call(ctx, roomID, riskCall{op: "getDanmuInfo", attempt: attempt, fallback: fallback})
	if err != nil {
		return nil, err
	}
	if confInfo != nil {
		return confInfo, nil
	}
	if code != 0 {
		return nil, fmt.Errorf("getDanmuInfo code=%d", code)
	}

	if len(info.addresses) == 0 {
		info.addresses = []string{defaultDanmakuServer}
	}
	info.buvid = lc.danmuBuvid(ctx)
	return info, nil
}

// danmuConf 调用旧版 getConf 接口，字段形状与 getDanmuInfo 不同
// （host_server_list / wss_port），但语义相同。它不走 WBI 签名，
// 作为 getDanmuInfo 被风控（-352）时的兜底。
func (lc *liveClient) danmuConf(ctx context.Context, roomID int64) (*danmuInfo, error) {
	cookie := lc.client.injectAntiRisk(ctx)
	endpoint := liveAPIBase + "/room/v1/Danmu/getConf?room_id=" + strconv.FormatInt(roomID, 10) + "&platform=pc&player=web"
	var raw struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Token          string `json:"token"`
			HostServerList []struct {
				Host    string `json:"host"`
				WssPort int    `json:"wss_port"`
			} `json:"host_server_list"`
		} `json:"data"`
	}
	if err := lc.client.fetchJSON(ctx, endpoint, roomID, cookie, &raw); err != nil {
		return nil, err
	}
	if raw.Code != 0 {
		return nil, fmt.Errorf("getConf code=%d msg=%s", raw.Code, raw.Msg)
	}
	info := &danmuInfo{token: raw.Data.Token}
	for _, h := range raw.Data.HostServerList {
		if h.Host != "" && h.WssPort > 0 {
			info.addresses = append(info.addresses, fmt.Sprintf("wss://%s:%d/sub", h.Host, h.WssPort))
		}
	}
	if len(info.addresses) == 0 {
		info.addresses = []string{defaultDanmakuServer}
	}
	info.buvid = lc.danmuBuvid(ctx)
	return info, nil
}

// danmuBuvid 返回弹幕认证载荷使用的 buvid3：优先取当前生效 cookie 中的，
// 其次回退到指纹存储。
func (lc *liveClient) danmuBuvid(ctx context.Context) string {
	cookie := lc.client.Cookie()
	if buvid := cookieValue(cookie, "buvid3"); buvid != "" {
		return buvid
	}
	b3, _, err := lc.client.buvids.getBuvids(ctx, cookie)
	if err != nil {
		log.Warn("get buvid3 for danmaku failed, continuing without", "err", err)
		return ""
	}
	return b3
}

// cookieValue 从 Cookie 头字符串中提取指定名称的值，不存在时返回空串。
func cookieValue(cookieHeader, name string) string {
	for item := range strings.SplitSeq(cookieHeader, ";") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) == 2 && parts[0] == name {
			return parts[1]
		}
	}
	return ""
}
