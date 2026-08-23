package bili

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"suika/internal/biz"

	"github.com/go-resty/resty/v2"
)

const (
	defaultQRGenerateURL = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"
	defaultQRPollURL     = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"
	defaultAccountNavURL = "https://api.bilibili.com/x/web-interface/nav"

	// qrCodeTTL 是二维码有效期（B 站侧固定 180 秒）。
	qrCodeTTL = 180 * time.Second
	// passportReferer 是 passport/nav 请求携带的 Referer。
	passportReferer = "https://www.bilibili.com"
)

// B 站扫码轮询的内层业务码。
const (
	qrPollConfirmed  = 0
	qrPollExpired    = 86038
	qrPollScanned    = 86090
	qrPollNotScanned = 86101
)

// navCodeNotLogin 是 nav 接口在未登录时返回的业务码。
const navCodeNotLogin = -101

// passportClient 实现 biz.PassportClient：扫码登录的二维码生成与轮询，
// 以及登录状态核验。所有请求走无 cookie jar 的专用客户端，不经风控编排。
type passportClient struct {
	httpClient  *resty.Client
	generateURL string
	pollURL     string
	navURL      string
}

func NewPassportClient(c *Client) biz.PassportClient {
	return &passportClient{
		httpClient:  c.passportHTTP,
		generateURL: defaultQRGenerateURL,
		pollURL:     defaultQRPollURL,
		navURL:      defaultAccountNavURL,
	}
}

// CreateQRLogin 生成扫码登录二维码。
func (pc *passportClient) CreateQRLogin(ctx context.Context) (*biz.QRLoginSession, error) {
	var result struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			URL       string `json:"url"`
			QRCodeKey string `json:"qrcode_key"`
		} `json:"data"`
	}
	if _, err := pc.getJSON(ctx, pc.generateURL, nil, "", &result); err != nil {
		return nil, err
	}
	if result.Code != 0 {
		return nil, fmt.Errorf("%w: generate code=%d message=%s", biz.ErrPassportUnavailable, result.Code, result.Message)
	}
	if result.Data.URL == "" || result.Data.QRCodeKey == "" {
		return nil, fmt.Errorf("%w: generate returned empty qrcode", biz.ErrPassportUnavailable)
	}
	return &biz.QRLoginSession{
		URL:        result.Data.URL,
		QRCodeKey:  result.Data.QRCodeKey,
		ExpireTime: time.Now().Add(qrCodeTTL),
	}, nil
}

// PollQRLogin 轮询扫码状态。确认成功时解析 Set-Cookie 拼成完整 Cookie
// 头，随凭据一并返回；其余状态不携带凭据。
func (pc *passportClient) PollQRLogin(ctx context.Context, qrcodeKey string) (*biz.QRLoginPoll, *biz.Credential, error) {
	var result struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			URL          string `json:"url"`
			RefreshToken string `json:"refresh_token"`
			Timestamp    int64  `json:"timestamp"`
			Code         int    `json:"code"`
			Message      string `json:"message"`
		} `json:"data"`
	}
	resp, err := pc.getJSON(ctx, pc.pollURL, map[string]string{"qrcode_key": qrcodeKey}, "", &result)
	if err != nil {
		return nil, nil, err
	}
	if result.Code != 0 {
		return nil, nil, fmt.Errorf("%w: poll code=%d message=%s", biz.ErrPassportUnavailable, result.Code, result.Message)
	}

	status, err := qrPollStatus(result.Data.Code)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", biz.ErrPassportUnavailable, err)
	}
	poll := &biz.QRLoginPoll{Status: status}
	if status != biz.QRLoginConfirmed {
		return poll, nil, nil
	}

	cookie, err := assembleLoginCookie(resp.Cookies())
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", biz.ErrPassportUnavailable, err)
	}
	cred := &biz.Credential{
		Cookie:       cookie,
		RefreshToken: result.Data.RefreshToken,
	}
	return poll, cred, nil
}

// AccountInfo 用给定 cookie 向 nav 接口核验登录状态。
func (pc *passportClient) AccountInfo(ctx context.Context, cookie string) (*biz.AccountInfo, error) {
	var result struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			IsLogin bool   `json:"isLogin"`
			UName   string `json:"uname"`
			Mid     int64  `json:"mid"`
		} `json:"data"`
	}
	if _, err := pc.getJSON(ctx, pc.navURL, nil, cookie, &result); err != nil {
		return nil, err
	}
	switch result.Code {
	case 0:
		if result.Data.IsLogin {
			return &biz.AccountInfo{State: biz.AccountLoggedIn, UName: result.Data.UName, Mid: result.Data.Mid}, nil
		}
		return &biz.AccountInfo{State: biz.AccountLoggedOut}, nil
	case navCodeNotLogin:
		return &biz.AccountInfo{State: biz.AccountLoggedOut}, nil
	default:
		return nil, fmt.Errorf("%w: nav code=%d message=%s", biz.ErrPassportUnavailable, result.Code, result.Message)
	}
}

// getJSON 携带 UA/Referer 发 GET 请求并把 JSON 响应体解码到 out。
// 网络与 HTTP 层错误统一包装为 biz.ErrPassportUnavailable。
func (pc *passportClient) getJSON(ctx context.Context, endpoint string, query map[string]string, cookie string, out any) (*resty.Response, error) {
	req := pc.httpClient.R().
		SetContext(ctx).
		SetHeader("User-Agent", biliUserAgent).
		SetHeader("Referer", passportReferer)
	for k, v := range query {
		req.SetQueryParam(k, v)
	}
	if cookie != "" {
		req.SetHeader("Cookie", cookie)
	}

	resp, err := req.Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", biz.ErrPassportUnavailable, err)
	}
	if resp.StatusCode() < 200 || resp.StatusCode() >= 300 {
		return nil, fmt.Errorf("%w: http status %d", biz.ErrPassportUnavailable, resp.StatusCode())
	}
	if out != nil {
		if err := json.Unmarshal(resp.Body(), out); err != nil {
			return nil, fmt.Errorf("%w: parse response: %v", biz.ErrPassportUnavailable, err)
		}
	}
	return resp, nil
}

// qrPollStatus 把 B 站轮询内层业务码映射为领域状态。
func qrPollStatus(code int) (biz.QRLoginStatus, error) {
	switch code {
	case qrPollConfirmed:
		return biz.QRLoginConfirmed, nil
	case qrPollExpired:
		return biz.QRLoginExpired, nil
	case qrPollScanned:
		return biz.QRLoginScanned, nil
	case qrPollNotScanned:
		return biz.QRLoginNotScanned, nil
	default:
		return biz.QRLoginUnknown, fmt.Errorf("unknown poll code %d", code)
	}
}

// assembleLoginCookie 把登录成功响应的 Set-Cookie 拼成完整 Cookie 头，
// 按到达顺序 name=value 串联，同名取第一个。必须包含 SESSDATA 与
// DedeUserID，否则视为异常的成功响应。
func assembleLoginCookie(cookies []*http.Cookie) (string, error) {
	seen := make(map[string]bool)
	var parts []string
	hasSessdata := false
	hasDedeUserID := false
	for _, c := range cookies {
		if c == nil || c.Name == "" || seen[c.Name] {
			continue
		}
		seen[c.Name] = true
		parts = append(parts, c.Name+"="+c.Value)
		switch c.Name {
		case "SESSDATA":
			hasSessdata = true
		case "DedeUserID":
			hasDedeUserID = true
		}
	}
	if !hasSessdata || !hasDedeUserID {
		return "", fmt.Errorf("login cookies missing SESSDATA or DedeUserID")
	}
	return strings.Join(parts, "; "), nil
}
