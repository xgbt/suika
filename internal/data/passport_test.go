package data

import (
	"context"
	stderrors "errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"suika/internal/biz"

	"github.com/go-resty/resty/v2"
)

func TestAssembleLoginCookie(t *testing.T) {
	tests := []struct {
		name    string
		cookies []*http.Cookie
		want    string
		wantErr bool
	}{
		{
			name: "assembles in order",
			cookies: []*http.Cookie{
				{Name: "SESSDATA", Value: "s"},
				{Name: "bili_jct", Value: "j"},
				{Name: "DedeUserID", Value: "42"},
			},
			want: "SESSDATA=s; bili_jct=j; DedeUserID=42",
		},
		{
			name: "duplicate names keep first",
			cookies: []*http.Cookie{
				{Name: "SESSDATA", Value: "first"},
				{Name: "DedeUserID", Value: "1"},
				{Name: "SESSDATA", Value: "second"},
			},
			want: "SESSDATA=first; DedeUserID=1",
		},
		{
			name: "skips nil and empty",
			cookies: []*http.Cookie{
				nil,
				{Name: "", Value: "x"},
				{Name: "SESSDATA", Value: "s"},
				{Name: "DedeUserID", Value: "1"},
			},
			want: "SESSDATA=s; DedeUserID=1",
		},
		{
			name: "missing SESSDATA errors",
			cookies: []*http.Cookie{
				{Name: "DedeUserID", Value: "1"},
			},
			wantErr: true,
		},
		{
			name: "missing DedeUserID errors",
			cookies: []*http.Cookie{
				{Name: "SESSDATA", Value: "s"},
			},
			wantErr: true,
		},
		{
			name:    "empty errors",
			cookies: nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := assembleLoginCookie(tt.cookies)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("assembleLoginCookie() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("assembleLoginCookie() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("assembleLoginCookie() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestQRPollStatus(t *testing.T) {
	tests := []struct {
		code    int
		want    biz.QRLoginStatus
		wantErr bool
	}{
		{qrPollConfirmed, biz.QRLoginConfirmed, false},
		{qrPollExpired, biz.QRLoginExpired, false},
		{qrPollScanned, biz.QRLoginScanned, false},
		{qrPollNotScanned, biz.QRLoginNotScanned, false},
		{12345, biz.QRLoginUnknown, true},
	}
	for _, tt := range tests {
		got, err := qrPollStatus(tt.code)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("qrPollStatus(%d) error = nil, want error", tt.code)
			}
			continue
		}
		if err != nil {
			t.Fatalf("qrPollStatus(%d) error = %v", tt.code, err)
		}
		if got != tt.want {
			t.Fatalf("qrPollStatus(%d) = %v, want %v", tt.code, got, tt.want)
		}
	}
}

// newTestPassportClient 返回指向给定测试服务器的客户端。
func newTestPassportClient(generateURL, pollURL, navURL string) *passportClient {
	return &passportClient{
		httpClient:  resty.New().SetTimeout(5 * time.Second).SetCookieJar(nil),
		generateURL: generateURL,
		pollURL:     pollURL,
		navURL:      navURL,
	}
}

func TestPassportCreateQRLogin(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"message":"0","ttl":1,"data":{"url":"https://passport.bilibili.com/scan?x=1","qrcode_key":"key-abc"}}`))
	}))
	defer server.Close()

	pc := newTestPassportClient(server.URL, "", "")
	before := time.Now()
	session, err := pc.CreateQRLogin(context.Background())
	if err != nil {
		t.Fatalf("CreateQRLogin() error = %v", err)
	}
	if session.URL != "https://passport.bilibili.com/scan?x=1" || session.QRCodeKey != "key-abc" {
		t.Fatalf("CreateQRLogin() = %+v, want url and key", session)
	}
	// 失效时刻约为生成时刻 + 180 秒。
	if session.ExpireTime.Before(before.Add(qrCodeTTL-time.Second)) || session.ExpireTime.After(before.Add(qrCodeTTL+5*time.Second)) {
		t.Fatalf("CreateQRLogin() expire_time = %v, want ~now+180s", session.ExpireTime)
	}
}

func TestPassportCreateQRLoginPlatformError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":-400,"message":"bad request","data":{}}`))
	}))
	defer server.Close()

	pc := newTestPassportClient(server.URL, "", "")
	if _, err := pc.CreateQRLogin(context.Background()); !stderrors.Is(err, biz.ErrPassportUnavailable) {
		t.Fatalf("CreateQRLogin(platform error) error = %v, want passport unavailable", err)
	}
}

func TestPassportPollQRLoginConfirmedCapturesSetCookie(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("qrcode_key") != "key-abc" {
			t.Errorf("poll qrcode_key = %q, want key-abc", r.URL.Query().Get("qrcode_key"))
		}
		http.SetCookie(w, &http.Cookie{Name: "SESSDATA", Value: "sess-val"})
		http.SetCookie(w, &http.Cookie{Name: "bili_jct", Value: "jct-val"})
		http.SetCookie(w, &http.Cookie{Name: "DedeUserID", Value: "42"})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"message":"0","ttl":1,"data":{"url":"","refresh_token":"rt-xyz","timestamp":1700000000,"code":0,"message":"0"}}`))
	}))
	defer server.Close()

	pc := newTestPassportClient("", server.URL, "")
	poll, cred, err := pc.PollQRLogin(context.Background(), "key-abc")
	if err != nil {
		t.Fatalf("PollQRLogin() error = %v", err)
	}
	if poll.Status != biz.QRLoginConfirmed {
		t.Fatalf("PollQRLogin() status = %v, want confirmed", poll.Status)
	}
	if cred == nil {
		t.Fatal("PollQRLogin() cred = nil, want credential")
	}
	if !strings.Contains(cred.Cookie, "SESSDATA=sess-val") || !strings.Contains(cred.Cookie, "DedeUserID=42") {
		t.Fatalf("PollQRLogin() cookie = %q, want assembled Set-Cookie", cred.Cookie)
	}
	if cred.RefreshToken != "rt-xyz" {
		t.Fatalf("PollQRLogin() refresh_token = %q, want rt-xyz", cred.RefreshToken)
	}
}

func TestPassportPollQRLoginPendingStates(t *testing.T) {
	tests := []struct {
		innerCode int
		want      biz.QRLoginStatus
	}{
		{qrPollExpired, biz.QRLoginExpired},
		{qrPollScanned, biz.QRLoginScanned},
		{qrPollNotScanned, biz.QRLoginNotScanned},
	}
	for _, tt := range tests {
		inner := tt.innerCode
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			body := `{"code":0,"message":"0","ttl":1,"data":{"code":` + strconv.Itoa(inner) + `,"message":"0"}}`
			_, _ = w.Write([]byte(body))
		}))

		pc := newTestPassportClient("", server.URL, "")
		poll, cred, err := pc.PollQRLogin(context.Background(), "key-abc")
		server.Close()
		if err != nil {
			t.Fatalf("PollQRLogin(inner=%d) error = %v", inner, err)
		}
		if poll.Status != tt.want {
			t.Fatalf("PollQRLogin(inner=%d) status = %v, want %v", inner, poll.Status, tt.want)
		}
		if cred != nil {
			t.Fatalf("PollQRLogin(inner=%d) cred = %+v, want nil", inner, cred)
		}
	}
}

func TestPassportAccountInfo(t *testing.T) {
	loggedIn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "SESSDATA=s" {
			t.Errorf("nav Cookie header = %q, want forwarded cookie", r.Header.Get("Cookie"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"message":"0","ttl":1,"data":{"isLogin":true,"uname":"alice","mid":42}}`))
	}))
	defer loggedIn.Close()

	pc := newTestPassportClient("", "", loggedIn.URL)
	info, err := pc.AccountInfo(context.Background(), "SESSDATA=s")
	if err != nil {
		t.Fatalf("AccountInfo(logged in) error = %v", err)
	}
	if info.State != biz.AccountLoggedIn || info.UName != "alice" || info.Mid != 42 {
		t.Fatalf("AccountInfo(logged in) = %+v, want alice/42", info)
	}

	notLogged := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":-101,"message":"账号未登录","ttl":1,"data":{}}`))
	}))
	defer notLogged.Close()

	pc = newTestPassportClient("", "", notLogged.URL)
	info, err = pc.AccountInfo(context.Background(), "SESSDATA=stale")
	if err != nil {
		t.Fatalf("AccountInfo(not logged) error = %v", err)
	}
	if info.State != biz.AccountLoggedOut {
		t.Fatalf("AccountInfo(not logged) state = %v, want logged out", info.State)
	}

	otherErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":-500,"message":"server error","ttl":1,"data":{}}`))
	}))
	defer otherErr.Close()

	pc = newTestPassportClient("", "", otherErr.URL)
	if _, err := pc.AccountInfo(context.Background(), "SESSDATA=s"); !stderrors.Is(err, biz.ErrPassportUnavailable) {
		t.Fatalf("AccountInfo(platform error) error = %v, want passport unavailable", err)
	}
}
