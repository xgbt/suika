// http.go 提供所有 B 站请求共用的 HTTP 原语：浏览器伪装头、JSON GET 与
// 状态码判定。各端点的 URL 构造、鉴权与业务码翻译不在这里 —— 直播侧经
// riskGuard（risk.go）声明端点形状，passport 侧自带端点常量。
package bili

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/url"
	"strings"

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

// browserRequest 构造一个携带浏览器伪装头的请求
func browserRequest(client *resty.Client, referer, origin, cookie string) *resty.Request {
	req := client.R().
		SetHeader("User-Agent", biliUserAgent).
		SetHeader("Referer", referer)

	if strings.TrimSpace(origin) != "" {
		req.SetHeader("Origin", origin)
	}
	if strings.TrimSpace(cookie) != "" {
		req.SetHeader("Cookie", cookie)
	}

	return req
}

// jsonGet 描述一次 JSON GET：HTTP 客户端、浏览器伪装头的 Referer/Origin、
// cookie、端点与查询参数、解码落点。直播侧与 passport 侧都经 getJSON 发送，
// 差别只在填哪几个字段。
type jsonGet struct {
	client   *resty.Client // HTTP 客户端，用于发送请求
	referer  string        // 浏览器伪装头的 Referer
	origin   string        // 为空时不发送 Origin 头
	cookie   string        // 浏览器伪装头的 Cookie
	endpoint string        // 请求的 URL 端点
	query    url.Values    // URL 查询参数
	out      any           // 解码 JSON 响应的目标对象
}

// getJSON 按描述执行一次 JSON GET 请求，并将 2xx 响应体解码至 out
// 仅在成功时返回非 nil 的 *resty.Response 以供读取 Header（如 Set-Cookie）
func getJSON(ctx context.Context, g *jsonGet) (*resty.Response, error) {
	// 构造带浏览器伪装头的请求，并设置上下文与解码落点
	req := browserRequest(g.client, g.referer, g.origin, g.cookie).
		SetContext(ctx).
		SetResult(g.out)

	// 设置 Query 参数
	if len(g.query) > 0 {
		req.SetQueryParamsFromValues(g.query)
	}

	// 发送 GET 请求并检查响应状态码。
	resp, err := req.Get(g.endpoint)
	if err != nil {
		return nil, err
	}
	if !resp.IsSuccess() {
		return nil, &httpStatusError{code: resp.StatusCode()}
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
