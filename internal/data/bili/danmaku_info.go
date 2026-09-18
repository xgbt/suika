// danmaku_info.go 获取弹幕连接所需的认证三要素（token、接入节点、buvid3）：
// 主通道 getDanmuInfo 走 WBI 签名，旧版 getConf 作为被风控时的兜底。
package bili

import (
	"context"
	stderrors "errors"
	"fmt"
	"strconv"

	"github.com/go-kratos/kratos/v3/log"
)

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
