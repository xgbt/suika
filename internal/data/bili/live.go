// live.go 实现 biz.LiveClient 的直播侧：查询房间开播状态与元数据、从播放
// 信息中挑选并打开 FLV 流、弹幕连接的创建入口，以及弹幕连接所需认证信息
// （token、接入节点、buvid3）的获取。
package bili

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"suika/internal/biz"

	"github.com/go-kratos/kratos/v3/log"
)

const (
	liveStatusOn         = 1
	defaultDanmakuServer = "wss://broadcastlv.chat.bilibili.com:2245/sub" // getDanmuInfo 和旧版 getConf 都被风控时的兜底弹幕端点
)

// sourceQualityQN 是请求的直播流清晰度：10000 = 原画。不做成配置项：
// 请求不到原画时平台会自动授予次高档位，没有理由请求更低的档。
// codec 只请求 0（avc）：录制管线只接受 AVC 流（见 pickFLVStream 与
// ADR-0004），不从平台请求无法正确录制的编码。
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
		risk:   newRiskGuard(client),
	}
}

// GetRoomInfo 经 getInfoByRoom 返回房间当前的开播状态。
func (lc *liveClient) GetRoomInfo(ctx context.Context, roomID int64) (*biz.RoomInfo, error) {
	var resp roomInfoResponse
	code, err := lc.risk.call(ctx, roomID, riskCall{
		attempt: riskRequest{
			op:    "getInfoByRoom",
			path:  "/xlive/web-room/v1/index/getInfoByRoom",
			query: url.Values{"room_id": {strconv.FormatInt(roomID, 10)}},
			sign:  true,
			out:   &resp,
		},
	})
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

	req := browserRequest(lc.client.streamClient, liveReferer(roomID), "", lc.client.Cookie()).
		SetContext(ctx).
		SetDoNotParseResponse(true)
	resp, err := req.Get(streamURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", biz.ErrStreamTransient, err)
	}
	if !resp.IsSuccess() {
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
	var resp playInfoResponse
	code, err := lc.risk.call(ctx, roomID, riskCall{attempt: riskRequest{
		op:   "getRoomPlayInfo",
		path: "/xlive/web-room/v2/index/getRoomPlayInfo",
		query: url.Values{
			"room_id":  {strconv.FormatInt(roomID, 10)},
			"protocol": {"0,1"},
			"format":   {"0,1,2"},
			"codec":    {"0"}, // codec 只请求 0（avc），见 ADR-0004
			"qn":       {strconv.Itoa(sourceQualityQN)},
			"platform": {"web"},
		},
		sign: true,
		out:  &resp,
	}})
	if err != nil {
		return "", biz.StreamQuality{}, err
	}
	if code != 0 {
		return "", biz.StreamQuality{}, fmt.Errorf("getRoomPlayInfo code=%d message=%s", code, resp.Message)
	}

	return pickFLVStream(resp.Data.PlayURLInfo.PlayURL, sourceQualityQN, roomID)
}

// pickFLVStream 从播放信息中挑选最优 FLV 流地址与清晰度：仅收 FLV、
// 仅收 AVC 编码。P2P CDN（.mcdn.）节点不适合长时间拉流录制，优先排除；
// 排除后无候选时退回全部候选。清晰度以平台实际授予为准（cookie 过期会
// 失去原画），即使低于请求档也接受。
func pickFLVStream(pu playURL, requestedQN int, roomID int64) (string, biz.StreamQuality, error) {
	bestURL, selectedQn, ok := bestFLVStream(pu, true)
	if !ok {
		bestURL, selectedQn, ok = bestFLVStream(pu, false)
	}
	if !ok {
		return "", biz.StreamQuality{}, fmt.Errorf("no FLV stream candidate for room %d", roomID)
	}

	granted := selectedQn
	if granted == 0 {
		granted = pu.CurrentQn
	}
	desc := ""
	if granted != 0 {
		desc = qnNames[int32(granted)]
		for _, qd := range pu.GQnDesc {
			if qd.Qn == granted {
				desc = qd.Desc
				break
			}
		}
	}
	if granted != 0 && granted != requestedQN {
		log.Warn("stream quality downgraded", "room", roomID, "requested", requestedQN, "granted", granted)
	}
	return bestURL, biz.StreamQuality{Qn: int32(granted), Desc: desc}, nil
}

// bestFLVStream 挑选首个 AVC FLV 候选；excludeP2P 为 true 时跳过 P2P
// CDN（.mcdn.）主机。只接受 avc codec：flv 包的关键帧与序列头判定按
// AVC 布局实现，HEVC 流（尤其 enhanced-RTMP 的 FourCC 布局）会使判定
// 失效，切段与头注入退化（ADR-0004）。
func bestFLVStream(pu playURL, excludeP2P bool) (url string, qn int, ok bool) {
	for _, stream := range pu.Stream {
		for _, format := range stream.Format {
			for _, codec := range format.Codec {
				if codec.CodecName != "avc" {
					continue
				}
				for _, urlInfo := range codec.URLInfo {
					if urlInfo.Host == "" || codec.BaseURL == "" {
						continue
					}
					if !isFLVStream(codec.BaseURL) {
						continue // 录制只收 FLV
					}
					if excludeP2P && isP2PCDNHost(urlInfo.Host) {
						continue
					}
					return urlInfo.Host + codec.BaseURL + urlInfo.Extra, codec.CurrentQn, true
				}
			}
		}
	}
	return
}

// isP2PCDNHost 报告主机是否为 P2P CDN 节点（.mcdn.）。
func isP2PCDNHost(host string) bool {
	return strings.Contains(host, ".mcdn.")
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

// liveReferer 返回房间页的 Referer URL，用于构造浏览器伪装请求头。
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

// bizCode 让响应体满足 codedResponse，供 riskGuard 读业务码。
func (r *roomInfoResponse) bizCode() int { return r.Code }

type playInfoResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		PlayURLInfo struct {
			PlayURL playURL `json:"playurl"`
		} `json:"playurl_info"`
	} `json:"data"`
}

// bizCode 让响应体满足 codedResponse，供 riskGuard 读业务码。
func (r *playInfoResponse) bizCode() int { return r.Code }

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
	CurrentQn int       `json:"current_qn"`
	BaseURL   string    `json:"base_url"`
	URLInfo   []hostURL `json:"url_info"`
}

type hostURL struct {
	Host  string `json:"host"`
	Extra string `json:"extra"`
}

// danmuInfo 是弹幕连接所需的认证三要素：
// token（进房鉴权）、addresses（wss 主机列表）、buvid（设备指纹 buvid3）。
type danmuInfo struct {
	token     string
	addresses []string
	buvid     string
}

// danmuHost 是一个弹幕接入节点。主通道的 host_list 与旧版 getConf 的
// host_server_list 元素形状相同，共用这一个类型。
type danmuHost struct {
	Host    string `json:"host"`
	WssPort int    `json:"wss_port"`
}

// danmuInfoResponse 是主通道 getDanmuInfo 的响应体。
type danmuInfoResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Token    string      `json:"token"`
		HostList []danmuHost `json:"host_list"`
	} `json:"data"`
}

// bizCode 让响应体满足 codedResponse，供 riskGuard 读业务码。
func (r *danmuInfoResponse) bizCode() int { return r.Code }

// danmuConfResponse 是旧版兜底接口 getConf 的响应体：业务码在 msg 而非
// message，节点列表叫 host_server_list。
type danmuConfResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data struct {
		Token          string      `json:"token"`
		HostServerList []danmuHost `json:"host_server_list"`
	} `json:"data"`
}

// bizCode 让响应体满足 codedResponse，供 riskGuard 读业务码。
func (r *danmuConfResponse) bizCode() int { return r.Code }

// buildDanmuInfo 由 token 与接入节点列表构造认证信息：把节点拼成 wss 地址，
// 没有可用节点时退回默认端点。主通道与兜底通道共用。
func buildDanmuInfo(token string, hosts []danmuHost) *danmuInfo {
	info := &danmuInfo{token: token}
	for _, h := range hosts {
		if h.Host != "" && h.WssPort > 0 {
			info.addresses = append(info.addresses, fmt.Sprintf("wss://%s:%d/sub", h.Host, h.WssPort))
		}
	}
	if len(info.addresses) == 0 {
		info.addresses = []string{defaultDanmakuServer}
	}
	return info
}

// danmuInfo 返回房间的弹幕认证信息：token、主机列表与 buvid3。
// 主通道 getDanmuInfo 经 WBI 签名；被 -352 双重拒绝时由 riskGuard 转调
// 旧版 getConf 兜底，两条通道的结果由 buildDanmuInfo 统一成形。
func (lc *liveClient) danmuInfo(ctx context.Context, roomID int64) (*danmuInfo, error) {
	var resp danmuInfoResponse
	var confResp danmuConfResponse
	var info, confInfo *danmuInfo // 主通道 / 兜底通道成功时各自填充

	code, err := lc.risk.call(ctx, roomID, riskCall{
		attempt: riskRequest{
			op:    "getDanmuInfo",
			path:  "/xlive/web-room/v1/index/getDanmuInfo",
			query: url.Values{"id": {strconv.FormatInt(roomID, 10)}, "type": {"0"}},
			sign:  true,
			out:   &resp,
			done: func() error {
				info = buildDanmuInfo(resp.Data.Token, resp.Data.HostList)
				return nil
			},
		},
		fallback: &riskRequest{
			op:    "getConf",
			path:  "/room/v1/Danmu/getConf",
			query: url.Values{"room_id": {strconv.FormatInt(roomID, 10)}, "platform": {"pc"}, "player": {"web"}},
			sign:  false, // 旧版接口不需要 WBI 签名
			out:   &confResp,
			done: func() error {
				if confResp.Data.Token == "" {
					return stderrors.New("legacy getConf returned empty token")
				}
				confInfo = buildDanmuInfo(confResp.Data.Token, confResp.Data.HostServerList)
				return nil
			},
		},
	})
	if err != nil {
		return nil, err
	}

	// 兜底通道成功时用它的结果，否则用主通道的结果；两者都没成功则按
	// 业务码报错（风控类失败已在 riskGuard 里包装返回）。
	chosen := confInfo
	if chosen == nil {
		if code != 0 {
			return nil, fmt.Errorf("getDanmuInfo code=%d message=%s", code, resp.Message)
		}
		chosen = info
	}
	chosen.buvid = lc.danmuBuvid(ctx)
	return chosen, nil
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
