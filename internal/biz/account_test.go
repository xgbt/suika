package biz

import (
	"context"
	stderrors "errors"
	"testing"
)

// fakePassport 为账号用例测试模拟 PassportClient 行为。
type fakePassport struct {
	session    *QRLoginSession
	poll       *QRLoginPoll
	pollCred   *Credential
	info       *AccountInfo
	createErr  error
	pollErr    error
	infoErr    error
	pollCalls  []string
	infoCalls  []string
	infoCookie string
}

func (p *fakePassport) CreateQRLogin(_ context.Context) (*QRLoginSession, error) {
	if p.createErr != nil {
		return nil, p.createErr
	}
	return p.session, nil
}

func (p *fakePassport) PollQRLogin(_ context.Context, qrcodeKey string) (*QRLoginPoll, *Credential, error) {
	p.pollCalls = append(p.pollCalls, qrcodeKey)
	if p.pollErr != nil {
		return nil, nil, p.pollErr
	}
	return p.poll, p.pollCred, nil
}

func (p *fakePassport) AccountInfo(_ context.Context, cookie string) (*AccountInfo, error) {
	p.infoCalls = append(p.infoCalls, cookie)
	p.infoCookie = cookie
	if p.infoErr != nil {
		return nil, p.infoErr
	}
	return p.info, nil
}

// fakeCredentialRepo 为账号用例测试模拟 CredentialRepo 行为。
type fakeCredentialRepo struct {
	cred      *Credential
	getErr    error
	saveErr   error
	deleteErr error
	saved     []*Credential
	deleted   int
}

func (r *fakeCredentialRepo) GetCredential(_ context.Context) (*Credential, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	if r.cred == nil {
		return nil, ErrCredentialNotFound
	}
	return r.cred, nil
}

func (r *fakeCredentialRepo) SaveCredential(_ context.Context, cred *Credential) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	r.saved = append(r.saved, cred)
	r.cred = cred
	return nil
}

func (r *fakeCredentialRepo) DeleteCredential(_ context.Context) error {
	if r.deleteErr != nil {
		return r.deleteErr
	}
	r.deleted++
	r.cred = nil
	return nil
}

func TestAccountUsecasePollQRLoginSavesOnConfirmed(t *testing.T) {
	ctx := context.Background()
	passport := &fakePassport{
		poll:     &QRLoginPoll{Status: QRLoginConfirmed},
		pollCred: &Credential{Cookie: "SESSDATA=s; DedeUserID=1", RefreshToken: "rt"},
	}
	repo := &fakeCredentialRepo{}
	uc := NewAccountUsecase(passport, repo)

	poll, err := uc.PollQRLogin(ctx, "key-1")
	if err != nil {
		t.Fatalf("PollQRLogin() error = %v", err)
	}
	if poll.Status != QRLoginConfirmed {
		t.Fatalf("PollQRLogin() status = %v, want confirmed", poll.Status)
	}
	if len(repo.saved) != 1 || repo.saved[0].Cookie != "SESSDATA=s; DedeUserID=1" || repo.saved[0].RefreshToken != "rt" {
		t.Fatalf("PollQRLogin() saved = %+v, want persisted credential", repo.saved)
	}
}

func TestAccountUsecasePollQRLoginNonConfirmedDoesNotSave(t *testing.T) {
	ctx := context.Background()
	for _, status := range []QRLoginStatus{QRLoginNotScanned, QRLoginScanned, QRLoginExpired} {
		passport := &fakePassport{poll: &QRLoginPoll{Status: status}}
		repo := &fakeCredentialRepo{}
		uc := NewAccountUsecase(passport, repo)

		poll, err := uc.PollQRLogin(ctx, "key-1")
		if err != nil {
			t.Fatalf("PollQRLogin(status=%v) error = %v", status, err)
		}
		if poll.Status != status {
			t.Fatalf("PollQRLogin(status=%v) = %v, want same status", status, poll.Status)
		}
		if len(repo.saved) != 0 {
			t.Fatalf("PollQRLogin(status=%v) saved %d credentials, want none", status, len(repo.saved))
		}
	}
}

func TestAccountUsecasePollQRLoginValidation(t *testing.T) {
	ctx := context.Background()
	uc := NewAccountUsecase(&fakePassport{}, &fakeCredentialRepo{})

	if _, err := uc.PollQRLogin(ctx, ""); !stderrors.Is(err, ErrAccountInvalidArgument) {
		t.Fatalf("PollQRLogin(empty key) error = %v, want invalid argument", err)
	}

	// 确认成功但缺失凭据 → 平台不可用。
	uc = NewAccountUsecase(&fakePassport{poll: &QRLoginPoll{Status: QRLoginConfirmed}}, &fakeCredentialRepo{})
	if _, err := uc.PollQRLogin(ctx, "key-1"); !stderrors.Is(err, ErrPassportUnavailable) {
		t.Fatalf("PollQRLogin(confirmed without cred) error = %v, want passport unavailable", err)
	}

	// 平台错误原样上抛。
	platformErr := stderrors.New("boom")
	uc = NewAccountUsecase(&fakePassport{pollErr: platformErr}, &fakeCredentialRepo{})
	if _, err := uc.PollQRLogin(ctx, "key-1"); !stderrors.Is(err, platformErr) {
		t.Fatalf("PollQRLogin(platform error) error = %v, want platform error", err)
	}

	// 持久化失败上抛。
	uc = NewAccountUsecase(
		&fakePassport{poll: &QRLoginPoll{Status: QRLoginConfirmed}, pollCred: &Credential{Cookie: "c"}},
		&fakeCredentialRepo{saveErr: stderrors.New("disk full")},
	)
	if _, err := uc.PollQRLogin(ctx, "key-1"); err == nil {
		t.Fatal("PollQRLogin(save error) error = nil, want error")
	}
}

func TestAccountUsecaseAccountStatus(t *testing.T) {
	ctx := context.Background()

	// 无凭据 → 登出，且不核验平台。
	uc := NewAccountUsecase(&fakePassport{}, &fakeCredentialRepo{})
	info, err := uc.AccountStatus(ctx)
	if err != nil {
		t.Fatalf("AccountStatus(no cred) error = %v", err)
	}
	if info.State != AccountLoggedOut {
		t.Fatalf("AccountStatus(no cred) state = %v, want logged out", info.State)
	}

	// 在线 → 透传昵称与 mid。
	passport := &fakePassport{info: &AccountInfo{State: AccountLoggedIn, UName: "alice", Mid: 42}}
	uc = NewAccountUsecase(passport, &fakeCredentialRepo{cred: &Credential{Cookie: "ck"}})
	info, err = uc.AccountStatus(ctx)
	if err != nil {
		t.Fatalf("AccountStatus(logged in) error = %v", err)
	}
	if info.State != AccountLoggedIn || info.UName != "alice" || info.Mid != 42 {
		t.Fatalf("AccountStatus(logged in) = %+v, want alice/42", info)
	}
	if passport.infoCookie != "ck" {
		t.Fatalf("AccountStatus() used cookie %q, want persisted cookie", passport.infoCookie)
	}

	// 平台报告未登录 → 凭据失效（保留凭据）。
	uc = NewAccountUsecase(
		&fakePassport{info: &AccountInfo{State: AccountLoggedOut}},
		&fakeCredentialRepo{cred: &Credential{Cookie: "ck"}},
	)
	info, err = uc.AccountStatus(ctx)
	if err != nil {
		t.Fatalf("AccountStatus(expired) error = %v", err)
	}
	if info.State != AccountExpired {
		t.Fatalf("AccountStatus(expired) state = %v, want expired", info.State)
	}

	// 平台错误上抛，不误报登出。
	uc = NewAccountUsecase(
		&fakePassport{infoErr: stderrors.New("network down")},
		&fakeCredentialRepo{cred: &Credential{Cookie: "ck"}},
	)
	if _, err := uc.AccountStatus(ctx); err == nil {
		t.Fatal("AccountStatus(platform error) error = nil, want error")
	}

	// 仓储读取错误上抛。
	uc = NewAccountUsecase(&fakePassport{}, &fakeCredentialRepo{getErr: stderrors.New("db down")})
	if _, err := uc.AccountStatus(ctx); err == nil {
		t.Fatal("AccountStatus(repo error) error = nil, want error")
	}
}

func TestAccountUsecaseLogout(t *testing.T) {
	ctx := context.Background()
	repo := &fakeCredentialRepo{cred: &Credential{Cookie: "ck"}}
	uc := NewAccountUsecase(&fakePassport{}, repo)

	if err := uc.Logout(ctx); err != nil {
		t.Fatalf("Logout() error = %v", err)
	}
	if repo.deleted != 1 {
		t.Fatalf("Logout() deleted = %d, want 1", repo.deleted)
	}
}
