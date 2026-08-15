package data

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

const (
	danmakuEventBuffer           = 4096
	danmakuRoomStateUpdateBuffer = 16

	danmakuHeartbeatInterval = 30 * time.Second
	// danmakuReadTimeout 用于掐掉半开连接：这么久（错过三轮心跳）
	// 没收到任何入站帧就强制重连。
	danmakuReadTimeout = 90 * time.Second

	danmakuReconnectBase = 2 * time.Second
	danmakuReconnectMax  = 30 * time.Second

	packetHeaderLength = 16
	operationHeartbeat = 2
	operationMessage   = 5
	operationAuth      = 7
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
	recordInteract   bool
}

func (c *danmakuConn) Events() <-chan *biz.DanmakuEvent { return c.events }

func (c *danmakuConn) RoomStateUpdates() <-chan *biz.RoomInfo { return c.roomStateUpdates }

func (c *danmakuConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

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

func (c *danmakuConn) dialAndAuth(ctx context.Context, address, token string, protover int, buvid string) (*websocket.Conn, error) {
	header := http.Header{
		"User-Agent": {biliUserAgent},
		"Origin":     {"https://live.bilibili.com"},
		"Referer":    {fmt.Sprintf("https://live.bilibili.com/%d", c.roomID)},
	}
	if c.lc.d.cookie != "" {
		header.Set("Cookie", c.lc.d.cookie)
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, address, header)
	if err != nil {
		return nil, err
	}
	auth := buildAuthBody(c.roomID, token, protover, buvid)
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
	info, err := c.lc.RoomStatus(ctx, c.roomID)
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
	case "INTERACT_WORD":
		if c.recordInteract {
			c.emit(parseInteractEvent(raw, receivedAt))
		}
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
	}
	return ev
}

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

func parseInteractEvent(raw json.RawMessage, receivedAt time.Time) *biz.DanmakuEvent {
	var m struct {
		Data struct {
			UID   int64  `json:"uid"`
			Uname string `json:"uname"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return &biz.DanmakuEvent{
		Ts: receivedAt, Type: biz.EventInteract, Raw: raw,
		UID: m.Data.UID, Uname: m.Data.Uname,
	}
}

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

func buildAuthBody(roomID int64, token string, protover int, buvid string) []byte {
	body := map[string]any{
		"uid":      0,
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

func shuffledStrings(items []string) []string {
	shuffled := append([]string(nil), items...)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled
}

// --- getDanmuInfo（token + 主机列表），带 -352 重试与旧接口兜底 ---

type danmuInfo struct {
	token     string
	addresses []string
	buvid     string
}

func (lc *liveClient) danmuInfo(ctx context.Context, roomID int64) (*danmuInfo, error) {
	if err := lc.enterRiskGate(roomID); err != nil {
		return nil, err
	}

	query := func() (int, *danmuInfo, error) {
		cookie := lc.d.injectAntiRisk(ctx)
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
		if err := lc.d.fetchJSON(ctx, lc.d.signURL(endpoint), roomID, cookie, &raw); err != nil {
			return 0, nil, err
		}
		info := &danmuInfo{token: raw.Data.Token}
		for _, host := range raw.Data.HostList {
			if host.Host != "" && host.WSSPort > 0 {
				info.addresses = append(info.addresses, fmt.Sprintf("wss://%s:%d/sub", host.Host, host.WSSPort))
			}
		}
		return raw.Code, info, nil
	}

	code, info, err := query()
	if err != nil {
		if stderrors.Is(err, errHTTPRiskControl) {
			lc.noteRiskFailure(roomID)
			return nil, fmt.Errorf("%w: %v", biz.ErrRiskControl, err)
		}
		return nil, err
	}
	if code == -352 {
		log.Warn("getDanmuInfo risk control -352, refreshing and retrying", "room", roomID)
		lc.d.refreshRisk()
		code, info, err = query()
		if err != nil {
			return nil, err
		}
	}
	if code == -352 {
		// 旧版 getConf 接口不需要 WBI 签名。
		log.Warn("getDanmuInfo still -352, trying legacy getConf", "room", roomID)
		if confInfo, confErr := lc.danmuConf(ctx, roomID); confErr == nil && confInfo.token != "" {
			lc.noteSuccess(roomID)
			return confInfo, nil
		}
		lc.noteRiskFailure(roomID)
		return nil, fmt.Errorf("%w: room_id=%d", errRiskControl352, roomID)
	}
	if code != 0 {
		return nil, fmt.Errorf("getDanmuInfo code=%d", code)
	}
	lc.noteSuccess(roomID)

	if len(info.addresses) == 0 {
		info.addresses = []string{defaultDanmakuServer}
	}
	info.buvid = lc.danmuBuvid(ctx)
	return info, nil
}

func (lc *liveClient) danmuConf(ctx context.Context, roomID int64) (*danmuInfo, error) {
	cookie := lc.d.injectAntiRisk(ctx)
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
	if err := lc.d.fetchJSON(ctx, endpoint, roomID, cookie, &raw); err != nil {
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

// danmuBuvid 返回弹幕认证载荷使用的 buvid3：优先取配置 cookie 中的，
// 其次回退到指纹存储。
func (lc *liveClient) danmuBuvid(ctx context.Context) string {
	if buvid := cookieValue(lc.d.cookie, "buvid3"); buvid != "" {
		return buvid
	}
	b3, _, err := lc.d.buvids.getBuvids(ctx, lc.d.cookie)
	if err != nil {
		log.Warn("get buvid3 for danmaku failed, continuing without", "err", err)
		return ""
	}
	return b3
}

func cookieValue(cookieHeader, name string) string {
	for _, item := range strings.Split(cookieHeader, ";") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) == 2 && parts[0] == name {
			return parts[1]
		}
	}
	return ""
}
