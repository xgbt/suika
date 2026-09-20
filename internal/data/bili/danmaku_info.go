// danmaku_info.go 获取弹幕连接所需的认证三要素（token、接入节点、buvid3）：
// 主通道 getDanmuInfo 走 WBI 签名，旧版 getConf 作为被风控时的兜底。
// 两个接口的响应字段名不同、语义相同，解析收敛到 buildDanmuInfo 一处。
package bili

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/url"
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
