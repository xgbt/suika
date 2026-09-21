package bili

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/http"

	"github.com/go-kratos/kratos/v3/log"
)

var (
	errRiskControl352  = stderrors.New("bilibili -352 risk control")
	errHTTPRiskControl = stderrors.New("bilibili http-layer risk control")
)

const (
	liveAPIBase = "https://api.live.bilibili.com" // B 站直播 API 基础 URL
	riskCode352 = -352                            // B 站直播 API 的 -352 风控错误码
)

// riskTransport 是 riskGuard 依赖的最小传输契约。
// *Client 是唯一生产实现，测试使用 scriptedTransport 打桩。
type riskTransport interface {
	antiRiskCookie(ctx context.Context) string
	signEndpoint(ctx context.Context, endpoint string) string
	fetchEndpointJSON(ctx context.Context, endpoint string, roomID int64, cookie string, out any) error
	refreshRiskState(ctx context.Context)
}

// antiRiskCookie 返回注入了 buvid3/buvid4 指纹的当前 cookie；失败时退化为原值。
func (c *Client) antiRiskCookie(ctx context.Context) string {
	cookie := c.Cookie()
	b3, b4, err := c.buvids.getBuvids(ctx, cookie)
	if err != nil {
		log.Warn("get buvids failed, continuing without buvid", "err", err)
		return cookie
	}

	if b3 == "" && b4 == "" {
		return cookie
	}

	return injectBuvids(cookie, b3, b4)
}

// refreshRiskState 在风控重试前刷新 WBI 密钥并丢弃缓存 buvid。
func (c *Client) refreshRiskState(ctx context.Context) {
	if err := c.signer.fetchKeys(ctx); err != nil {
		log.Warn("wbi key refresh failed, retrying with existing keys", "err", err)
	}
	c.buvids.invalidate(c.Cookie())
}

// fetchEndpointJSON 发起一次直播站伪装 GET 并解码 JSON；
// HTTP 412/403/429 映射为 errHTTPRiskControl 供 riskGuard 识别。
func (c *Client) fetchEndpointJSON(ctx context.Context, endpoint string, roomID int64, cookie string, out any) error {
	_, err := getJSON(ctx, &jsonGet{
		client:   c.apiClient,
		referer:  liveReferer(roomID),
		origin:   liveOrigin,
		cookie:   cookie,
		endpoint: endpoint,
		out:      out,
	})
	if err != nil {
		if code, ok := httpStatusOf(err); ok {
			switch code {
			case http.StatusPreconditionFailed, http.StatusForbidden, http.StatusTooManyRequests:
				return fmt.Errorf("%w: status=%d", errHTTPRiskControl, code)
			}
		}
		return err
	}
	return nil
}

// signEndpoint 对 endpoint 做 WBI 签名；失败时退化为原 URL。
func (c *Client) signEndpoint(ctx context.Context, endpoint string) string {
	signed, err := c.signer.signURL(ctx, endpoint)
	if err != nil {
		log.Warn("wbi sign failed, continuing unsigned", "err", err)
		return endpoint
	}
	return signed
}
