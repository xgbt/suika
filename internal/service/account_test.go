package service

import (
	"context"
	stderrors "errors"
	"path/filepath"
	"testing"
	"time"

	accountv1 "suika/api/account/v1"
	"suika/internal/biz"
	"suika/internal/conf"
	"suika/internal/data"

	"google.golang.org/protobuf/proto"
)

// fakePassportClient 实现 biz.PassportClient，供服务层测试驱动。
type fakePassportClient struct {
	session   *biz.QRLoginSession
	poll      *biz.QRLoginPoll
	pollCred  *biz.Credential
	info      *biz.AccountInfo
	createErr error
	pollErr   error
	infoErr   error
}

func (p *fakePassportClient) CreateQRLogin(_ context.Context) (*biz.QRLoginSession, error) {
	if p.createErr != nil {
		return nil, p.createErr
	}
	return p.session, nil
}

func (p *fakePassportClient) PollQRLogin(_ context.Context, _ string) (*biz.QRLoginPoll, *biz.Credential, error) {
	if p.pollErr != nil {
		return nil, nil, p.pollErr
	}
	return p.poll, p.pollCred, nil
}

func (p *fakePassportClient) AccountInfo(_ context.Context, _ string) (*biz.AccountInfo, error) {
	if p.infoErr != nil {
		return nil, p.infoErr
	}
	return p.info, nil
}

// newTestAccountEnv 在真实 *data.Data 上按 wireApp 的方式搭起账号服务链，
// 并暴露假 PassportClient 与真实凭据仓储。
func newTestAccountEnv(t *testing.T, d *data.Data, passport *fakePassportClient) *AccountService {
	t.Helper()
	repo := data.NewCredentialRepo(d)
	uc := biz.NewAccountUsecase(passport, repo)
	return NewAccountService(uc)
}

func TestAccountServiceQRLoginFlow(t *testing.T) {
	ctx := context.Background()
	d := newTestData(t)
	passport := &fakePassportClient{
		session: &biz.QRLoginSession{URL: "https://scan", QRCodeKey: "key-1", ExpireTime: time.Now().Add(180 * time.Second)},
	}
	svc := newTestAccountEnv(t, d, passport)

	created, err := svc.CreateQRLogin(ctx, &accountv1.CreateQRLoginRequest{})
	if err != nil {
		t.Fatalf("CreateQRLogin() error = %v", err)
	}
	if created.GetUrl() != "https://scan" || created.GetQrcodeKey() != "key-1" || created.GetExpireTime() == nil {
		t.Fatalf("CreateQRLogin() = %+v, want url/key/expire_time", created)
	}

	// 未确认前账号状态为登出，且 Cookie 未生效。
	status, err := svc.GetAccountStatus(ctx, &accountv1.GetAccountStatusRequest{})
	if err != nil {
		t.Fatalf("GetAccountStatus(before login) error = %v", err)
	}
	if status.GetAccount().GetState() != accountv1.AccountState_ACCOUNT_STATE_LOGGED_OUT {
		t.Fatalf("GetAccountStatus(before login) state = %v, want logged out", status.GetAccount().GetState())
	}
	if d.Cookie() != "" {
		t.Fatalf("Cookie() before login = %q, want empty", d.Cookie())
	}

	// 轮询成功 → 凭据落库并热生效。
	passport.poll = &biz.QRLoginPoll{Status: biz.QRLoginConfirmed}
	passport.pollCred = &biz.Credential{Cookie: "SESSDATA=x; DedeUserID=1", RefreshToken: "rt"}
	poll, err := svc.PollQRLogin(ctx, &accountv1.PollQRLoginRequest{QrcodeKey: "key-1"})
	if err != nil {
		t.Fatalf("PollQRLogin() error = %v", err)
	}
	if poll.GetStatus() != accountv1.QRLoginStatus_QR_LOGIN_STATUS_CONFIRMED {
		t.Fatalf("PollQRLogin() status = %v, want confirmed", poll.GetStatus())
	}
	if d.Cookie() != "SESSDATA=x; DedeUserID=1" {
		t.Fatalf("Cookie() after login = %q, want hot-swapped credential", d.Cookie())
	}

	// 核验登录账号。
	passport.info = &biz.AccountInfo{State: biz.AccountLoggedIn, UName: "alice", Mid: 42}
	status, err = svc.GetAccountStatus(ctx, &accountv1.GetAccountStatusRequest{})
	if err != nil {
		t.Fatalf("GetAccountStatus(after login) error = %v", err)
	}
	account := status.GetAccount()
	if account.GetState() != accountv1.AccountState_ACCOUNT_STATE_LOGGED_IN || account.GetUname() != "alice" || account.GetMid() != 42 {
		t.Fatalf("GetAccountStatus(after login) = %+v, want logged-in alice/42", account)
	}

	// 登出 → 凭据清除、Cookie 失效。
	if _, err := svc.Logout(ctx, &accountv1.LogoutRequest{}); err != nil {
		t.Fatalf("Logout() error = %v", err)
	}
	if d.Cookie() != "" {
		t.Fatalf("Cookie() after logout = %q, want empty", d.Cookie())
	}
	status, err = svc.GetAccountStatus(ctx, &accountv1.GetAccountStatusRequest{})
	if err != nil {
		t.Fatalf("GetAccountStatus(after logout) error = %v", err)
	}
	if status.GetAccount().GetState() != accountv1.AccountState_ACCOUNT_STATE_LOGGED_OUT {
		t.Fatalf("GetAccountStatus(after logout) state = %v, want logged out", status.GetAccount().GetState())
	}
}

func TestAccountServiceCredentialPersistsAcrossRestart(t *testing.T) {
	ctx := context.Background()

	// 用显式数据库路径，便于在清理后用同一文件重建 Data 模拟重启。
	dbPath := filepath.Join(t.TempDir(), "persist.db")
	confData := &conf.Data{Database: &conf.Data_Database{Source: dbPath}}

	d, cleanup, err := data.NewData(confData, &conf.Recorder{MergeEnabled: proto.Bool(false)})
	if err != nil {
		t.Fatalf("NewData(first) error = %v", err)
	}

	passport := &fakePassportClient{
		poll:     &biz.QRLoginPoll{Status: biz.QRLoginConfirmed},
		pollCred: &biz.Credential{Cookie: "SESSDATA=durable", RefreshToken: "rt"},
	}
	svc := newTestAccountEnv(t, d, passport)
	if _, err := svc.PollQRLogin(ctx, &accountv1.PollQRLoginRequest{QrcodeKey: "key-1"}); err != nil {
		t.Fatalf("PollQRLogin() error = %v", err)
	}
	if d.Cookie() != "SESSDATA=durable" {
		t.Fatalf("Cookie() after login = %q, want durable", d.Cookie())
	}
	cleanup()

	// 重启：在同一数据库文件上新建 Data，启动时应自动加载凭据。
	restarted, cleanup2, err := data.NewData(confData, &conf.Recorder{MergeEnabled: proto.Bool(false)})
	if err != nil {
		t.Fatalf("NewData(restart) error = %v", err)
	}
	defer cleanup2()
	if restarted.Cookie() != "SESSDATA=durable" {
		t.Fatalf("Cookie() after restart = %q, want credential loaded at startup", restarted.Cookie())
	}
}

func TestAccountServicePollQRLoginEmptyKey(t *testing.T) {
	ctx := context.Background()
	svc := newTestAccountEnv(t, newTestData(t), &fakePassportClient{})

	_, err := svc.PollQRLogin(ctx, &accountv1.PollQRLoginRequest{QrcodeKey: ""})
	if err == nil {
		t.Fatal("PollQRLogin(empty key) error = nil, want error")
	}
	if !stderrors.Is(err, biz.ErrAccountInvalidArgument) {
		t.Fatalf("PollQRLogin(empty key) error = %v, want invalid argument", err)
	}
}

func TestAccountServiceExpiredCredential(t *testing.T) {
	ctx := context.Background()
	d := newTestData(t)
	passport := &fakePassportClient{
		poll:     &biz.QRLoginPoll{Status: biz.QRLoginConfirmed},
		pollCred: &biz.Credential{Cookie: "SESSDATA=stale"},
	}
	svc := newTestAccountEnv(t, d, passport)
	if _, err := svc.PollQRLogin(ctx, &accountv1.PollQRLoginRequest{QrcodeKey: "key-1"}); err != nil {
		t.Fatalf("PollQRLogin() error = %v", err)
	}

	// 平台报告未登录 → 状态为失效，但凭据保留。
	passport.info = &biz.AccountInfo{State: biz.AccountLoggedOut}
	status, err := svc.GetAccountStatus(ctx, &accountv1.GetAccountStatusRequest{})
	if err != nil {
		t.Fatalf("GetAccountStatus(expired) error = %v", err)
	}
	if status.GetAccount().GetState() != accountv1.AccountState_ACCOUNT_STATE_EXPIRED {
		t.Fatalf("GetAccountStatus(expired) state = %v, want expired", status.GetAccount().GetState())
	}
	if _, err := data.NewCredentialRepo(d).GetCredential(ctx); err != nil {
		t.Fatalf("GetCredential(expired) error = %v, want credential kept", err)
	}
}

func TestAccountServicePlatformErrorPropagates(t *testing.T) {
	ctx := context.Background()
	d := newTestData(t)
	passport := &fakePassportClient{
		poll:     &biz.QRLoginPoll{Status: biz.QRLoginConfirmed},
		pollCred: &biz.Credential{Cookie: "SESSDATA=x"},
		infoErr:  stderrors.New("platform down"),
	}
	svc := newTestAccountEnv(t, d, passport)
	if _, err := svc.PollQRLogin(ctx, &accountv1.PollQRLoginRequest{QrcodeKey: "key-1"}); err != nil {
		t.Fatalf("PollQRLogin() error = %v", err)
	}

	if _, err := svc.GetAccountStatus(ctx, &accountv1.GetAccountStatusRequest{}); err == nil {
		t.Fatal("GetAccountStatus(platform error) error = nil, want error")
	}
}
