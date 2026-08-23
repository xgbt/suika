package bili

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"suika/internal/biz"

	"github.com/go-kratos/kratos/v3/log"
)

const (
	liveAPIBase              = "https://api.live.bilibili.com" // B 站直播 API 基础 URL
	riskCode352              = -352                            // B 站直播 API 的 -352 风控错误码
	flvStreamPriorityDefault = 90                              // 默认 FLV 流优先级
	flvStreamPriorityAVC     = 100                             // AVC 编码 FLV 流优先级（更优）
	liveStatusOn             = 1
	defaultDanmakuServer     = "wss://broadcastlv.chat.bilibili.com:2245/sub" // getDanmuInfo 和旧版 getConf 都被风控时的兜底弹幕端点
)

// sourceQualityQN 是请求的直播流清晰度：10000 = 原画。不做成配置项：
// 请求不到原画时平台会自动授予次高档位，没有理由请求更低的档。
const sourceQualityQN = 10000

// qnNames 将清晰度编号映射为展示名称（API 未返回 g_qn_desc 时兜底）。
var qnNames = map[int32]string{
	20000: "4K",
	10000: "原画",
	400:   "蓝光",
	250:   "超清",
	150:   "高清",
	80:    "流畅",
}

// liveClient 实现所有 B 站 API 与弹幕 websocket 流量；
// 风控重试与按房间的冷却由 riskGuard 统一编排。
type liveClient struct {
	client *Client
	risk   *riskGuard
}

func NewLiveClient(client *Client) biz.LiveClient {
	return &liveClient{
		client: client,
		risk:   newRiskGuard(client.refreshRisk),
	}
}

// GetRoomInfo 经 getInfoByRoom 返回房间当前的开播状态。
func (lc *liveClient) GetRoomInfo(ctx context.Context, roomID int64) (*biz.RoomInfo, error) {
	var resp roomInfoResponse
	attempt := func(ctx context.Context) (int, error) {
		cookie := lc.client.injectAntiRisk(ctx)
		endpoint := liveAPIBase + "/xlive/web-room/v1/index/getInfoByRoom?room_id=" + strconv.FormatInt(roomID, 10)
		return resp.Code, lc.client.fetchJSON(ctx, lc.client.signURL(endpoint), roomID, cookie, &resp)
	}
	code, err := lc.risk.call(ctx, roomID, riskCall{op: "getInfoByRoom", attempt: attempt})
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, fmt.Errorf("getInfoByRoom code=%d message=%s", code, resp.Message)
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

// OpenLiveStream 选择最优 FLV 流地址并打开读取。打开/读取失败若看似 CDN
// 侧，则包装为 biz.ErrStreamTransient，供决策树重新选择流地址。
// API 侧的冷却与风控重试在 selectStreamURL 内经 riskGuard 完成。
func (lc *liveClient) OpenLiveStream(ctx context.Context, roomID int64) (*biz.LiveStream, error) {
	streamURL, quality, err := lc.selectStreamURL(ctx, roomID)
	if err != nil {
		return nil, err
	}

	req := lc.client.streamClient.R().
		SetContext(ctx).
		SetHeader("User-Agent", biliUserAgent).
		SetHeader("Referer", liveReferer(roomID)).
		SetDoNotParseResponse(true)
	if cookie := lc.client.Cookie(); cookie != "" {
		req.SetHeader("Cookie", cookie)
	}
	resp, err := req.Get(streamURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", biz.ErrStreamTransient, err)
	}
	if resp.StatusCode() < 200 || resp.StatusCode() >= 300 {
		if resp.RawBody() != nil {
			_ = resp.RawBody().Close()
		}
		return nil, fmt.Errorf("%w: stream http status %d", biz.ErrStreamTransient, resp.StatusCode())
	}
	body := resp.RawBody()
	if body == nil {
		return nil, fmt.Errorf("%w: stream response body is empty", biz.ErrStreamTransient)
	}
	log.Info("stream opened", "room", roomID, "qn", quality.Qn, "desc", quality.Desc)
	return &biz.LiveStream{URL: streamURL, Quality: quality, Body: body}, nil
}

// selectStreamURL 调用 B 站接口获取房间的播放信息，并选择最优 FLV 流地址。
func (lc *liveClient) selectStreamURL(ctx context.Context, roomID int64) (string, biz.StreamQuality, error) {
	endpoint := liveAPIBase + "/xlive/web-room/v2/index/getRoomPlayInfo?room_id=" +
		strconv.FormatInt(roomID, 10) +
		"&protocol=0,1&format=0,1,2&codec=0,1&qn=" + strconv.Itoa(sourceQualityQN) + "&platform=web"

	var resp playInfoResponse
	attempt := func(ctx context.Context) (int, error) {
		cookie := lc.client.injectAntiRisk(ctx)
		return resp.Code, lc.client.fetchJSON(ctx, lc.client.signURL(endpoint), roomID, cookie, &resp)
	}
	code, err := lc.risk.call(ctx, roomID, riskCall{op: "getRoomPlayInfo", attempt: attempt})
	if err != nil {
		return "", biz.StreamQuality{}, err
	}
	if code != 0 {
		return "", biz.StreamQuality{}, fmt.Errorf("getRoomPlayInfo code=%d message=%s", code, resp.Message)
	}

	return pickFLVStream(resp.Data.PlayURLInfo.PlayURL, sourceQualityQN, roomID)
}

// pickFLVStream 从播放信息中挑选最优 FLV 流地址与清晰度：AVC 编码优先，
// 仅收 FLV。清晰度以平台实际授予为准（cookie 过期会失去原画），
// 即使低于请求档也接受。
func pickFLVStream(pu playURL, requestedQN int, roomID int64) (string, biz.StreamQuality, error) {
	bestURL := ""
	bestPriority := -1
	for _, stream := range pu.Stream {
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

	granted := pu.CurrentQn
	if granted == 0 {
		granted = requestedQN
	}
	desc := qnNames[int32(granted)]
	for _, qd := range pu.GQnDesc {
		if qd.Qn == granted {
			desc = qd.Desc
			break
		}
	}
	if granted != requestedQN {
		log.Warn("stream quality downgraded", "room", roomID, "requested", requestedQN, "granted", granted)
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
			PlayURL playURL `json:"playurl"`
		} `json:"playurl_info"`
	} `json:"data"`
}

// playURL 是 getRoomPlayInfo 返回的流地址清单。
type playURL struct {
	CurrentQn int          `json:"current_qn"`
	GQnDesc   []qnDesc     `json:"g_qn_desc"`
	Stream    []streamLine `json:"stream"`
}

type qnDesc struct {
	Qn   int    `json:"qn"`
	Desc string `json:"desc"`
}

type streamLine struct {
	Format []formatLine `json:"format"`
}

type formatLine struct {
	FormatName string      `json:"format_name"`
	Codec      []codecLine `json:"codec"`
}

type codecLine struct {
	CodecName string    `json:"codec_name"`
	BaseURL   string    `json:"base_url"`
	URLInfo   []hostURL `json:"url_info"`
}

type hostURL struct {
	Host  string `json:"host"`
	Extra string `json:"extra"`
}
