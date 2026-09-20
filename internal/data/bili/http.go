// http.go 提供所有 B 站请求共用的 HTTP 原语：浏览器伪装头、JSON GET 与
// 状态码判定。各端点的 URL 构造、鉴权与业务码翻译不在这里 —— 直播侧经
// riskGuard（risk.go）声明端点形状，passport 侧自带端点常量。
package bili

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/url"

	"github.com/go-resty/resty/v2"
)

const (
	// biliUserAgent 是所有 B 站请求使用的 User-Agent。
	biliUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"

	// biliWWWURL 是主站地址，用作 www 侧接口（导航、指纹、账号）的 Referer，
	// 部分接口同时用作 Origin。
	biliWWWURL = "https://www.bilibili.com"

	// liveOrigin 是直播站的 Origin（Referer 是具体房间页，见 liveReferer）。
	liveOrigin = "https://live.bilibili.com"
)

// browserRequest 构造一个携带浏览器伪装头的请求。B 站接口都要求这三件套
// 才按网页端对待；origin 为空时不设置（部分端点从不发送 Origin）。
// context 由调用方按需 SetContext。
func browserRequest(client *resty.Client, referer, origin, cookie string) *resty.Request {
	req := client.R().
		SetHeader("User-Agent", biliUserAgent).
		SetHeader("Referer", referer)
	if origin != "" {
		req.SetHeader("Origin", origin)
	}
	if cookie != "" {
		req.SetHeader("Cookie", cookie)
	}
	return req
}

// jsonGet 描述一次 JSON GET：HTTP 客户端、浏览器伪装头的 Referer/Origin、
// cookie、端点与查询参数、解码落点。直播侧与 passport 侧都经 getJSON 发送，
// 差别只在填哪几个字段。
type jsonGet struct {
	client   *resty.Client
	referer  string
	origin   string // 为空时不发送 Origin 头（部分端点从不发送）
	cookie   string
	endpoint string
	query    url.Values
	out      any
}

// getJSON 按描述发一次 GET，把 2xx 响应体解码到 out。返回的响应供调用方
// 读取 Set-Cookie 等头部：解码成功时 resp 非 nil，出错时 resp 恒为 nil。
// 非 2xx 状态返回 *httpStatusError。
func getJSON(ctx context.Context, g jsonGet) (*resty.Response, error) {
	req := browserRequest(g.client, g.referer, g.origin, g.cookie).SetContext(ctx)
	for k, values := range g.query {
		for _, v := range values {
			req.SetQueryParam(k, v)
		}
	}
	return doJSON(req, g.endpoint, g.out)
}

// doJSON 发送 req 并把 2xx 响应体解码到 out。
// 返回的响应供调用方读取 Set-Cookie 等头部：解码成功时 resp 非 nil，
// 出错时 resp 恒为 nil。非 2xx 状态返回 *httpStatusError。
func doJSON(req *resty.Request, endpoint string, out any) (*resty.Response, error) {
	resp, err := req.Get(endpoint)
	if err != nil {
		return nil, err
	}
	if !resp.IsSuccess() {
		return nil, &httpStatusError{code: resp.StatusCode()}
	}
	if err := json.Unmarshal(resp.Body(), out); err != nil {
		return nil, fmt.Errorf("bilibili parse response: %w", err)
	}
	return resp, nil
}

// httpStatusError 表示响应状态码不在 2xx。需要按状态码分支的调用方
// （如把 412/403/429 归为 HTTP 层风控）用 httpStatusOf 取回码值。
type httpStatusError struct{ code int }

func (e *httpStatusError) Error() string { return fmt.Sprintf("bilibili http status %d", e.code) }

// httpStatusOf 返回 err 携带的 HTTP 状态码；err 不是状态错误时为 false。
func httpStatusOf(err error) (int, bool) {
	var se *httpStatusError
	if stderrors.As(err, &se) {
		return se.code, true
	}
	return 0, false
}
