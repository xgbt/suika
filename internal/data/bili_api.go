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

// biliUserAgent is the User-Agent used for every Bilibili request.
const biliUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"

const (
	liveAPIBase = "https://api.live.bilibili.com"
	// defaultDanmakuServer is the last-resort danmaku endpoint when both
	// getDanmuInfo and the legacy getConf are risk-controlled.
	defaultDanmakuServer = "wss://broadcastlv.chat.bilibili.com:2245/sub"
)

// Internal risk-control sentinels; public callers only see biz.ErrRiskControl.
var (
	errRiskControl352  = stderrors.New("bilibili -352 risk control")
	errHTTPRiskControl = stderrors.New("bilibili http-layer risk control")
)

// riskCooldownLadder is the escalating per-room cooldown applied after
// repeated risk-control rejections.
var riskCooldownLadder = []time.Duration{5 * time.Minute, 10 * time.Minute, 20 * time.Minute}

// qnNames maps quality numbers to display names (fallback when the API
// does not return g_qn_desc).
var qnNames = map[int32]string{
	20000: "4K",
	10000: "原画",
	400:   "蓝光",
	250:   "超清",
	150:   "高清",
	80:    "流畅",
}

// injectAntiRisk returns the configured cookie with fresh buvid3/buvid4
// fingerprints injected. Failures degrade to the plain cookie.
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

// refreshRisk refreshes WBI keys and drops cached buvids before a
// risk-control retry.
func (d *Data) refreshRisk() {
	if err := d.signer.refreshKeys(); err != nil {
		log.Warn("wbi key refresh failed, retrying with existing keys", "err", err)
	}
	d.buvids.invalidate(d.cookie)
}

// signURL WBI-signs endpoint; on failure it degrades to the unsigned URL.
func (d *Data) signURL(endpoint string) string {
	signed, err := d.signer.signURL(endpoint)
	if err != nil {
		log.Warn("wbi sign failed, continuing unsigned", "err", err)
		return endpoint
	}
	return signed
}

// fetchJSON performs a GET with anti-risk-control headers and decodes the
// JSON body into out. HTTP 412/403/429 map to errHTTPRiskControl.
func (d *Data) fetchJSON(ctx context.Context, endpoint string, roomID int64, cookie string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", biliUserAgent)
	req.Header.Set("Referer", "https://live.bilibili.com/"+strconv.FormatInt(roomID, 10))
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

// liveClient implements biz.LiveClient: all Bilibili API and danmaku
// websocket traffic, including risk-control retries and per-room cooldowns.
type liveClient struct {
	d *Data

	mu        sync.Mutex
	cooldowns map[int64]*riskCooldown
}

// NewLiveClient creates the Bilibili platform seam.
func NewLiveClient(d *Data) biz.LiveClient {
	return &liveClient{d: d, cooldowns: make(map[int64]*riskCooldown)}
}

// enterRiskGate blocks API calls for a room that is cooling down.
func (lc *liveClient) enterRiskGate(roomID int64) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	cd := lc.cooldowns[roomID]
	if cd != nil && time.Now().Before(cd.until) {
		return fmt.Errorf("%w: room %d cooling down until %s", biz.ErrRiskControl, roomID, cd.until.Format(time.RFC3339))
	}
	return nil
}

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

func (lc *liveClient) noteSuccess(roomID int64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	delete(lc.cooldowns, roomID)
}

func isRiskControlError(err error) bool {
	return stderrors.Is(err, errRiskControl352) || stderrors.Is(err, errHTTPRiskControl)
}

// RoomStatus returns the room's current broadcast state via getInfoByRoom.
func (lc *liveClient) RoomStatus(ctx context.Context, roomID int64) (*biz.RoomInfo, error) {
	if err := lc.enterRiskGate(roomID); err != nil {
		return nil, err
	}
	info, err := lc.roomStatus(ctx, roomID)
	if err != nil {
		if isRiskControlError(err) {
			lc.noteRiskFailure(roomID)
			return nil, fmt.Errorf("%w: %v", biz.ErrRiskControl, err)
		}
		return nil, err
	}
	lc.noteSuccess(roomID)
	return info, nil
}

func (lc *liveClient) roomStatus(ctx context.Context, roomID int64) (*biz.RoomInfo, error) {
	var resp roomInfoResponse
	query := func() error {
		cookie := lc.d.injectAntiRisk(ctx)
		endpoint := liveAPIBase + "/xlive/web-room/v1/index/getInfoByRoom?room_id=" + strconv.FormatInt(roomID, 10)
		return lc.d.fetchJSON(ctx, lc.d.signURL(endpoint), roomID, cookie, &resp)
	}
	if err := query(); err != nil {
		if !stderrors.Is(err, errHTTPRiskControl) {
			return nil, err
		}
		log.Warn("getInfoByRoom http-layer risk control, refreshing and retrying once", "room", roomID)
		lc.d.refreshRisk()
		if err = query(); err != nil {
			return nil, err
		}
	}
	if resp.Code == -352 {
		log.Warn("getInfoByRoom risk control -352, refreshing and retrying once", "room", roomID)
		lc.d.refreshRisk()
		if err := query(); err != nil {
			return nil, err
		}
	}
	if resp.Code == -352 {
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
		Live:          room.LiveStatus == 1,
		Title:         title,
		StreamerName:  resp.Data.AnchorInfo.BaseInfo.UName,
		LiveStartTime: startedAt,
	}, nil
}

// OpenStream selects the best FLV stream URL and opens it for reading.
// Any open/read failure is wrapped as biz.ErrStreamTransient when it
// looks CDN-side, so the decision tree can re-select a stream URL.
func (lc *liveClient) OpenStream(ctx context.Context, roomID int64) (*biz.StreamHandle, error) {
	if err := lc.enterRiskGate(roomID); err != nil {
		return nil, err
	}
	streamURL, quality, err := lc.selectStreamURL(ctx, roomID)
	if err != nil {
		if isRiskControlError(err) {
			lc.noteRiskFailure(roomID)
			return nil, fmt.Errorf("%w: %v", biz.ErrRiskControl, err)
		}
		return nil, err
	}
	lc.noteSuccess(roomID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", biliUserAgent)
	req.Header.Set("Referer", "https://live.bilibili.com/"+strconv.FormatInt(roomID, 10))
	if lc.d.cookie != "" {
		req.Header.Set("Cookie", lc.d.cookie)
	}
	resp, err := lc.d.streamClient.Do(req)
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
	cookie := lc.d.injectAntiRisk(ctx)
	endpoint := liveAPIBase + "/xlive/web-room/v2/index/getRoomPlayInfo?room_id=" +
		strconv.FormatInt(roomID, 10) +
		"&protocol=0,1&format=0,1,2&codec=0,1&qn=" + strconv.Itoa(lc.d.qualityQN) + "&platform=web"

	var resp playInfoResponse
	if err := lc.d.fetchJSON(ctx, lc.d.signURL(endpoint), roomID, cookie, &resp); err != nil {
		return "", biz.StreamQuality{}, err
	}
	if resp.Code == -352 {
		lc.d.refreshRisk()
		if err := lc.d.fetchJSON(ctx, lc.d.signURL(endpoint), roomID, cookie, &resp); err != nil {
			return "", biz.StreamQuality{}, err
		}
	}
	if resp.Code == -352 {
		return "", biz.StreamQuality{}, fmt.Errorf("%w: room_id=%d", errRiskControl352, roomID)
	}
	if resp.Code != 0 {
		return "", biz.StreamQuality{}, fmt.Errorf("getRoomPlayInfo code=%d message=%s", resp.Code, resp.Message)
	}

	type candidate struct {
		url      string
		priority int
	}
	var candidates []candidate
	playURL := resp.Data.PlayURLInfo.PlayURL
	for _, stream := range playURL.Stream {
		for _, format := range stream.Format {
			for _, codec := range format.Codec {
				for _, urlInfo := range codec.URLInfo {
					if urlInfo.Host == "" || codec.BaseURL == "" {
						continue
					}
					if !isFLVStream(codec.BaseURL) {
						continue // recording needs FLV
					}
					priority := 90
					if codec.CodecName == "avc" {
						priority = 100
					}
					candidates = append(candidates, candidate{
						url:      urlInfo.Host + codec.BaseURL + urlInfo.Extra,
						priority: priority,
					})
				}
			}
		}
	}
	if len(candidates) == 0 {
		return "", biz.StreamQuality{}, fmt.Errorf("no FLV stream candidate for room %d", roomID)
	}
	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.priority > best.priority {
			best = c
		}
	}

	// Accept the granted quality even when it is below the requested qn
	// (expired cookies lose source quality); record it in meta.
	granted := playURL.CurrentQn
	if granted == 0 {
		granted = lc.d.qualityQN
	}
	desc := qnNames[int32(granted)]
	for _, qd := range playURL.GQnDesc {
		if qd.Qn == granted {
			desc = qd.Desc
			break
		}
	}
	if int32(granted) != int32(lc.d.qualityQN) {
		log.Warn("stream quality downgraded", "room", roomID, "requested", lc.d.qualityQN, "granted", granted)
	}
	return best.url, biz.StreamQuality{Qn: int32(granted), Desc: desc}, nil
}

func (lc *liveClient) DanmakuConn(ctx context.Context, roomID int64) (biz.DanmakuConn, error) {
	conn := &danmakuConn{
		lc:               lc,
		roomID:           roomID,
		events:           make(chan *biz.DanmakuEvent, danmakuEventBuffer),
		roomStateUpdates: make(chan *biz.RoomInfo, danmakuRoomStateUpdateBuffer),
		closed:           make(chan struct{}),
		recordInteract:   lc.d.recordInteractWord,
	}
	go conn.run(ctx)
	return conn, nil
}

func isFLVStream(baseURL string) bool {
	return strings.Contains(strings.ToLower(baseURL), ".flv")
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
