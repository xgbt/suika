// http.go 提供所有 B 站请求共用的 HTTP 原语：浏览器伪装头、状态码判定
// 与 JSON 解码。各端点的 URL 构造与业务码翻译不在这里。
package bili

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"

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

// fetchJSON 携带抗风控 header 发 GET 请求，并把 JSON 响应体解码到 out。
// HTTP 412/403/429 映射为 errHTTPRiskControl。
func (c *Client) fetchJSON(ctx context.Context, endpoint string, roomID int64, cookie string, out any) error {
	// 构造携带浏览器伪装头的请求，Referer 为房间页，Origin 为直播站，注入 cookie
	req := browserRequest(c.apiClient, liveReferer(roomID), liveOrigin, cookie).SetContext(ctx)

	// 发送请求并处理 JSON 响应，非 2xx 状态码会被映射为 httpStatusError。
	if _, err := doJSON(req, endpoint, out); err != nil {
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
