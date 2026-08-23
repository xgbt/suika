package bili

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/go-resty/resty/v2"
)

var (
	errRiskControl352  = stderrors.New("bilibili -352 risk control")
	errHTTPRiskControl = stderrors.New("bilibili http-layer risk control")
)

// biliUserAgent 是所有 B 站请求使用的 User-Agent。
const biliUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"

// Client 持有所有与 B 站交互共享的长生命周期状态：携带 cookie 的
// HTTP 客户端、当前生效的登录态、以及风控助手（WBI 签名器、buvid
// 存储）。由 data 层在启动时构建并持有，录制器与账号模块经此访问平台。
type Client struct {
	// apiClient 用于短小的 B 站 API 调用。
	apiClient *resty.Client

	// streamClient 拉取直播流；不设超时，因为读取是长生命周期的，
	// 取消经请求 context 传递。
	streamClient *resty.Client

	// passportHTTP 专用于 B 站登录/账号平台接口；必须无 cookie jar，
	// 避免登录响应的 Set-Cookie 串染房间 API 请求。
	passportHTTP *resty.Client

	// cookie 是当前生效的唯一登录态，读写必须经 Cookie()/SetCookie()。
	mu     sync.RWMutex
	cookie string

	// signer 是 WBI 签名器，负责生成风控请求的签名参数。
	signer *wbiSigner

	// buvids 是 buvid 缓存，负责生成风控请求的 buvid3/buvid4 指纹。
	buvids *buvidStore
}

func NewClient(cookie string) *Client {
	apiClient := resty.New().SetTimeout(15 * time.Second)
	// passport 调用不需要携带也不会保留 cookie，禁用 jar。
	passportHTTP := resty.New().SetTimeout(15 * time.Second).SetCookieJar(nil)

	c := &Client{
		apiClient:    apiClient,
		streamClient: resty.New(),
		passportHTTP: passportHTTP,
		cookie:       cookie,
	}
	c.signer = newWBISigner(apiClient, c.Cookie)
	c.buvids = newBuvidStore(apiClient)
	return c
}

// Cookie 返回当前生效的 B 站 Cookie 头；未登录为 ""。
// 录制器的所有并发读取点都必须经此取值（读快照）。
func (c *Client) Cookie() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cookie
}

// SetCookie 热替换生效的 cookie（扫码登录成功/登出时由凭据仓储调用）。
// 替换后丢弃旧 cookie 的 buvid 缓存，避免旧指纹被继续注入。
func (c *Client) SetCookie(cookie string) {
	c.mu.Lock()
	old := c.cookie
	c.cookie = cookie
	c.mu.Unlock()
	if old != "" && old != cookie {
		c.buvids.invalidate(old)
	}
}

// injectAntiRisk 返回注入了新鲜 buvid3/buvid4 指纹的当前生效 cookie；
// 失败时退化为原 cookie。
func (c *Client) injectAntiRisk(ctx context.Context) string {
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

// refreshRisk 在风控重试前刷新 WBI 密钥并丢弃缓存的 buvid。
func (c *Client) refreshRisk() {
	if err := c.signer.refreshKeys(); err != nil {
		log.Warn("wbi key refresh failed, retrying with existing keys", "err", err)
	}
	c.buvids.invalidate(c.Cookie())
}

// signURL 对 endpoint 做 WBI 签名；失败时退化为未签名 URL。
func (c *Client) signURL(endpoint string) string {
	signed, err := c.signer.signURL(endpoint)
	if err != nil {
		log.Warn("wbi sign failed, continuing unsigned", "err", err)
		return endpoint
	}
	return signed
}

// fetchJSON 携带抗风控 header 发 GET 请求，并把 JSON 响应体解码到 out。
// HTTP 412/403/429 映射为 errHTTPRiskControl。
func (c *Client) fetchJSON(ctx context.Context, endpoint string, roomID int64, cookie string, out any) error {
	req := c.apiClient.R().
		SetContext(ctx).
		SetHeader("User-Agent", biliUserAgent).
		SetHeader("Referer", liveReferer(roomID)).
		SetHeader("Origin", "https://live.bilibili.com")
	if cookie != "" {
		req.SetHeader("Cookie", cookie)
	}
	resp, err := req.Get(endpoint)
	if err != nil {
		return err
	}
	if resp.StatusCode() < 200 || resp.StatusCode() >= 300 {
		switch resp.StatusCode() {
		case http.StatusPreconditionFailed, http.StatusForbidden, http.StatusTooManyRequests:
			return fmt.Errorf("%w: status=%d", errHTTPRiskControl, resp.StatusCode())
		default:
			return fmt.Errorf("bilibili http status %d", resp.StatusCode())
		}
	}
	if err := json.Unmarshal(resp.Body(), out); err != nil {
		return err
	}
	return nil
}
