package data

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"suika/internal/biz"

	"github.com/go-kratos/kratos/v3/log"
)

// biliUserAgent 是所有 B 站请求使用的 User-Agent。
const biliUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"

const (
	liveAPIBase              = "https://api.live.bilibili.com" // B 站直播 API 基础 URL
	riskCode352              = -352                            // B 站直播 API 的 -352 风控错误码
	flvStreamPriorityDefault = 90                              // 默认 FLV 流优先级
	flvStreamPriorityAVC     = 100                             // AVC 编码 FLV 流优先级（更优）
	liveStatusOn             = 1
	defaultDanmakuServer     = "wss://broadcastlv.chat.bilibili.com:2245/sub" // getDanmuInfo 和旧版 getConf 都被风控时的兜底弹幕端点
)

var (
	errRiskControl352  = stderrors.New("bilibili -352 risk control")
	errHTTPRiskControl = stderrors.New("bilibili http-layer risk control")
)

// riskCooldownLadder 风控被拒后，递增的冷却时长
var riskCooldownLadder = []time.Duration{5 * time.Minute, 10 * time.Minute, 20 * time.Minute}

// qnNames 将清晰度编号映射为展示名称（API 未返回 g_qn_desc 时兜底）。
var qnNames = map[int32]string{
	20000: "4K",
	10000: "原画",
	400:   "蓝光",
	250:   "超清",
	150:   "高清",
	80:    "流畅",
}

// injectAntiRisk 返回注入了新鲜 buvid3/buvid4 指纹的配置 cookie；
// 失败时退化为原 cookie。
func (d *Data) injectAntiRisk(ctx context.Context) string {
	b3, b4, err := d.buvids.getBuvids(ctx, d.cookie)
	if err != nil {
		log.Warn("get buvids failed, continuing without buvid", "err", err)
		return d.cookie
	}

	if b3 == "" && b4 == "" {
		return d.cookie
	}

	return injectBuvids(d.cookie, b3, b4)
}

// refreshRisk 在风控重试前刷新 WBI 密钥并丢弃缓存的 buvid。
func (d *Data) refreshRisk() {
	if err := d.signer.refreshKeys(); err != nil {
		log.Warn("wbi key refresh failed, retrying with existing keys", "err", err)
	}
	d.buvids.invalidate(d.cookie)
}

// signURL 对 endpoint 做 WBI 签名；失败时退化为未签名 URL。
func (d *Data) signURL(endpoint string) string {
	signed, err := d.signer.signURL(endpoint)
	if err != nil {
		log.Warn("wbi sign failed, continuing unsigned", "err", err)
		return endpoint
	}
	return signed
}

// fetchJSON 携带抗风控 header 发 GET 请求，并把 JSON 响应体解码到 out。
// HTTP 412/403/429 映射为 errHTTPRiskControl。
func (d *Data) fetchJSON(ctx context.Context, endpoint string, roomID int64, cookie string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", biliUserAgent)
	req.Header.Set("Referer", liveReferer(roomID))
	req.Header.Set("Origin", "https://live.bilibili.com")
	if cookie != "" {
		req.Header.Set("Cookie", cookie)
	}
	resp, err := d.apiClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		switch resp.StatusCode {
		case http.StatusPreconditionFailed, http.StatusForbidden, http.StatusTooManyRequests:
			return fmt.Errorf("%w: status=%d", errHTTPRiskControl, resp.StatusCode)
		default:
			return fmt.Errorf("bilibili http status %d", resp.StatusCode)
		}
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type riskCooldown struct {
	failures int
	until    time.Time
}

// liveClient 实现所有 B 站 API 与弹幕 websocket 流量，
// 包括风控重试和按房间的冷却。
type liveClient struct {
	data      *Data
	mu        sync.Mutex
	cooldowns map[int64]*riskCooldown // key roomID
}

func NewLiveClient(data *Data) biz.LiveClient {
	return &liveClient{
		data:      data,
		cooldowns: make(map[int64]*riskCooldown),
	}
}

// checkRiskCooldown 检查该房间的 API 请求是否进入冷却期；若是则返回 biz.ErrRiskControl。
func (lc *liveClient) checkRiskCooldown(roomID int64) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	cd := lc.cooldowns[roomID]
	if cd != nil && time.Now().Before(cd.until) {
		return fmt.Errorf("%w: room %d cooling down until %s", biz.ErrRiskControl, roomID, cd.until.Format(time.RFC3339))
	}

	return nil
}

// noteRiskFailure 记录该房间的 API 请求被风控拒绝，增加冷却时长。
func (lc *liveClient) noteRiskFailure(roomID int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	cd := lc.cooldowns[roomID]
	if cd == nil {
		cd = &riskCooldown{}
		lc.cooldowns[roomID] = cd
	}
	cd.failures++
	idx := min(cd.failures-1, len(riskCooldownLadder)-1)
	cd.until = time.Now().Add(riskCooldownLadder[idx])
	log.Warn("room risk-control cooldown started", "room", roomID, "failures", cd.failures, "until", cd.until.Format(time.RFC3339))
}

// noteSuccess 记录该房间的 API 请求成功，清除冷却状态。
func (lc *liveClient) noteSuccess(roomID int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.cooldowns, roomID)
}

func (lc *liveClient) wrapRiskErr(roomID int64, err error) error {
	if !stderrors.Is(err, errRiskControl352) && !stderrors.Is(err, errHTTPRiskControl) {
		return err
	}
	lc.noteRiskFailure(roomID)
	return fmt.Errorf("%w: %v", biz.ErrRiskControl, err)
}

// GetRoomInfo 经 getInfoByRoom 返回房间当前的开播状态。
func (lc *liveClient) GetRoomInfo(ctx context.Context, roomID int64) (*biz.RoomInfo, error) {
	if err := lc.checkRiskCooldown(roomID); err != nil {
		return nil, err
	}

	info, err := lc.roomInfo(ctx, roomID)
	if err != nil {
		return nil, lc.wrapRiskErr(roomID, err)
	}

	lc.noteSuccess(roomID)
	return info, nil
}

// roomInfo 调用 API 获取房间信息，处理 -352 风控。
func (lc *liveClient) roomInfo(ctx context.Context, roomID int64) (*biz.RoomInfo, error) {
	var resp roomInfoResponse
	query := func() error {
		cookie := lc.data.injectAntiRisk(ctx)
		endpoint := liveAPIBase + "/xlive/web-room/v1/index/getInfoByRoom?room_id=" + strconv.FormatInt(roomID, 10)
		return lc.data.fetchJSON(ctx, lc.data.signURL(endpoint), roomID, cookie, &resp)
	}
	if err := query(); err != nil {
		if !stderrors.Is(err, errHTTPRiskControl) {
			return nil, err
		}
		log.Warn("getInfoByRoom http-layer risk control, refreshing and retrying once", "room", roomID)
		lc.data.refreshRisk()
		if err = query(); err != nil {
			return nil, err
		}
	}
	if resp.Code == riskCode352 {
		log.Warn("getInfoByRoom risk control -352, refreshing and retrying once", "room", roomID)
		lc.data.refreshRisk()
		if err := query(); err != nil {
			return nil, err
		}
	}
	if resp.Code == riskCode352 {
		return nil, fmt.Errorf("%w: room_id=%d", errRiskControl352, roomID)
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("getInfoByRoom code=%d message=%s", resp.Code, resp.Message)
	}

	room := resp.Data.RoomInfo
	title := room.Title
	if title == "" {
		title = resp.Data.AnchorInfo.BaseInfo.UName
	}
	startedAt := time.Unix(room.LiveStartTime, 0)
	if room.LiveStartTime <= 0 {
		startedAt = time.Now()
	}
	return &biz.RoomInfo{
		RoomID:        room.RoomID,
		Live:          room.LiveStatus == liveStatusOn,
		Title:         title,
		StreamerName:  resp.Data.AnchorInfo.BaseInfo.UName,
		LiveStartTime: startedAt,
	}, nil
}

// OpenStream 选择最优 FLV 流地址并打开读取。打开/读取失败若看似 CDN
// 侧，则包装为 biz.ErrStreamTransient，供决策树重新选择流地址。
func (lc *liveClient) OpenStream(ctx context.Context, roomID int64) (*biz.StreamHandle, error) {
	if err := lc.checkRiskCooldown(roomID); err != nil {
		return nil, err
	}

	streamURL, quality, err := lc.selectStreamURL(ctx, roomID)
	if err != nil {
		return nil, lc.wrapRiskErr(roomID, err)
	}
	lc.noteSuccess(roomID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", biliUserAgent)
	req.Header.Set("Referer", liveReferer(roomID))
	if lc.data.cookie != "" {
		req.Header.Set("Cookie", lc.data.cookie)
	}
	resp, err := lc.data.streamClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", biz.ErrStreamTransient, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("%w: stream http status %d", biz.ErrStreamTransient, resp.StatusCode)
	}
	log.Info("stream opened", "room", roomID, "qn", quality.Qn, "desc", quality.Desc)
	return &biz.StreamHandle{URL: streamURL, Quality: quality, Body: resp.Body}, nil
}

func (lc *liveClient) selectStreamURL(ctx context.Context, roomID int64) (string, biz.StreamQuality, error) {
	endpoint := liveAPIBase + "/xlive/web-room/v2/index/getRoomPlayInfo?room_id=" +
		strconv.FormatInt(roomID, 10) +
		"&protocol=0,1&format=0,1,2&codec=0,1&qn=" + strconv.Itoa(lc.data.qualityQN) + "&platform=web"

	var resp playInfoResponse
	fetch := func() error {
		cookie := lc.data.injectAntiRisk(ctx)
		return lc.data.fetchJSON(ctx, lc.data.signURL(endpoint), roomID, cookie, &resp)
	}
	if err := fetch(); err != nil {
		return "", biz.StreamQuality{}, err
	}
	if resp.Code == riskCode352 {
		lc.data.refreshRisk()
		if err := fetch(); err != nil {
			return "", biz.StreamQuality{}, err
		}
	}
	if resp.Code == riskCode352 {
		return "", biz.StreamQuality{}, fmt.Errorf("%w: room_id=%d", errRiskControl352, roomID)
	}
	if resp.Code != 0 {
		return "", biz.StreamQuality{}, fmt.Errorf("getRoomPlayInfo code=%d message=%s", resp.Code, resp.Message)
	}

	bestURL := ""
	bestPriority := -1
	playURL := resp.Data.PlayURLInfo.PlayURL
	for _, stream := range playURL.Stream {
		for _, format := range stream.Format {
			for _, codec := range format.Codec {
				for _, urlInfo := range codec.URLInfo {
					if urlInfo.Host == "" || codec.BaseURL == "" {
						continue
					}
					if !isFLVStream(codec.BaseURL) {
						continue // 录制只收 FLV
					}
					priority := flvStreamPriorityDefault
					if codec.CodecName == "avc" {
						priority = flvStreamPriorityAVC
					}
					if priority > bestPriority {
						bestPriority = priority
						bestURL = urlInfo.Host + codec.BaseURL + urlInfo.Extra
					}
				}
			}
		}
	}
	if bestURL == "" {
		return "", biz.StreamQuality{}, fmt.Errorf("no FLV stream candidate for room %d", roomID)
	}

	// 即使授予的清晰度低于请求的 qn 也接受（cookie 过期会失去原画），
	// 并记入 meta。
	granted := playURL.CurrentQn
	if granted == 0 {
		granted = lc.data.qualityQN
	}
	desc := qnNames[int32(granted)]
	for _, qd := range playURL.GQnDesc {
		if qd.Qn == granted {
			desc = qd.Desc
			break
		}
	}
	if int32(granted) != int32(lc.data.qualityQN) {
		log.Warn("stream quality downgraded", "room", roomID, "requested", lc.data.qualityQN, "granted", granted)
	}
	return bestURL, biz.StreamQuality{Qn: int32(granted), Desc: desc}, nil
}

func (lc *liveClient) DanmakuConn(ctx context.Context, roomID int64) (biz.DanmakuConn, error) {
	conn := &danmakuConn{
		lc:               lc,
		roomID:           roomID,
		events:           make(chan *biz.DanmakuEvent, danmakuEventBuffer),
		roomStateUpdates: make(chan *biz.RoomInfo, danmakuRoomStateUpdateBuffer),
		closed:           make(chan struct{}),
		recordInteract:   lc.data.recordInteractWord,
	}
	go conn.run(ctx)
	return conn, nil
}

func isFLVStream(baseURL string) bool {
	return strings.Contains(strings.ToLower(baseURL), ".flv")
}

func liveReferer(roomID int64) string {
	return "https://live.bilibili.com/" + strconv.FormatInt(roomID, 10)
}

type roomInfoResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		RoomInfo struct {
			RoomID        int64  `json:"room_id"`
			LiveStatus    int    `json:"live_status"`
			Title         string `json:"title"`
			LiveStartTime int64  `json:"live_start_time"`
		} `json:"room_info"`
		AnchorInfo struct {
			BaseInfo struct {
				UName string `json:"uname"`
			} `json:"base_info"`
		} `json:"anchor_info"`
	} `json:"data"`
}

type playInfoResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		PlayURLInfo struct {
			PlayURL struct {
				CurrentQn int `json:"current_qn"`
				GQnDesc   []struct {
					Qn   int    `json:"qn"`
					Desc string `json:"desc"`
				} `json:"g_qn_desc"`
				Stream []struct {
					Format []struct {
						FormatName string `json:"format_name"`
						Codec      []struct {
							CodecName string `json:"codec_name"`
							BaseURL   string `json:"base_url"`
							URLInfo   []struct {
								Host  string `json:"host"`
								Extra string `json:"extra"`
							} `json:"url_info"`
						} `json:"codec"`
					} `json:"format"`
				} `json:"stream"`
			} `json:"playurl"`
		} `json:"playurl_info"`
	} `json:"data"`
}
