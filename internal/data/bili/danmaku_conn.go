// danmaku_conn.go 实现一个房间的常驻弹幕 websocket：拨号认证、心跳保活、
// 读超时断线与退避重连，并把入站帧分发到房间状态和弹幕事件两个通道。
package bili

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"suika/internal/biz"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/gorilla/websocket"
)

// 以下常量分两组：通道缓冲容量与连接生命周期时序。
// 弹幕二进制协议参数见 danmaku_proto.go。
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
		"Referer":    {liveReferer(c.roomID)},
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
