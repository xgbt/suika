package biz

import (
	"context"
	"time"

	v1 "suika/api/account/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

var (
	ErrCredentialNotFound     = errors.NotFound(v1.ErrorReason_ERROR_REASON_NOT_FOUND.String(), "credential not found")
	ErrAccountInvalidArgument = errors.BadRequest(v1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT.String(), "invalid account argument")
	ErrPassportUnavailable    = errors.ServiceUnavailable(v1.ErrorReason_ERROR_REASON_UNAVAILABLE.String(), "bilibili passport unavailable")
)

// QRLoginStatus 扫码登录的轮询状态。
type QRLoginStatus int

const (
	QRLoginUnknown    QRLoginStatus = iota // 未知状态
	QRLoginNotScanned                      // 尚未扫码
	QRLoginScanned                         // 已扫码、待确认
	QRLoginExpired                         // 二维码已过期
	QRLoginConfirmed                       // 确认成功，凭据已保存
)

// AccountState 账号登录状态。
type AccountState int

const (
	AccountLoggedOut AccountState = iota // 无凭据
	AccountLoggedIn                      // 有凭据且在线
	AccountExpired                       // 有凭据但已失效
)

// QRLoginSession 是一次扫码登录会话，包含二维码内容和轮询凭证。
type QRLoginSession struct {
	URL        string    // 二维码内容（URL）
	QRCodeKey  string    // 轮询凭证
	ExpireTime time.Time // 二维码失效时刻
}

// QRLoginPoll 是一次扫码轮询的结果。
type QRLoginPoll struct {
	Status QRLoginStatus // 轮询状态
}

// Credential 是持久化的 B 站登录凭据。
type Credential struct {
	Cookie       string    // 完整 Cookie 头（含 SESSDATA）
	RefreshToken string    // B 站刷新令牌
	UpdateTime   time.Time // 更新时间
}

// AccountInfo 是账号登录信息。
type AccountInfo struct {
	State AccountState // 登录状态
	UName string       // 昵称，未登录时为空
	Mid   int64        // mid，未登录时为 0
}

// PassportClient 是 B 站账号平台的客户端接口，网络 IO 在 data 层。
// 它负责扫码登录的二维码生成与轮询，以及登录状态核验。
type PassportClient interface {
	// CreateQRLogin 生成扫码登录二维码。
	CreateQRLogin(ctx context.Context) (*QRLoginSession, error)
	// PollQRLogin 轮询扫码状态。确认成功时返回的 QRLoginPoll 状态为
	// QRLoginConfirmed，且实现方已把 Set-Cookie 拼成完整 Cookie 头写入凭据。
	PollQRLogin(ctx context.Context, qrcodeKey string) (*QRLoginPoll, *Credential, error)
	// AccountInfo 用给定 cookie 核验登录状态。
	AccountInfo(ctx context.Context, cookie string) (*AccountInfo, error)
}

// CredentialRepo 是登录凭据的存储接口，由 data 层实现。
type CredentialRepo interface {
	GetCredential(ctx context.Context) (*Credential, error)
	SaveCredential(ctx context.Context, cred *Credential) error
	DeleteCredential(ctx context.Context) error
}

// AccountUsecase 管理录制器充当的 B 站账号：扫码登录、状态查询与登出。
// 凭据持久化在数据库中，是唯一登录态来源。
type AccountUsecase struct {
	client PassportClient
	repo   CredentialRepo
}

func NewAccountUsecase(client PassportClient, repo CredentialRepo) *AccountUsecase {
	return &AccountUsecase{client: client, repo: repo}
}

// CreateQRLogin 生成扫码登录二维码。
func (uc *AccountUsecase) CreateQRLogin(ctx context.Context) (*QRLoginSession, error) {
	return uc.client.CreateQRLogin(ctx)
}

// PollQRLogin 轮询扫码状态。确认成功时把凭据持久化，之后由 data 层
// 的凭据仓储负责让新登录态即时生效。
func (uc *AccountUsecase) PollQRLogin(ctx context.Context, qrcodeKey string) (*QRLoginPoll, error) {
	if qrcodeKey == "" {
		return nil, ErrAccountInvalidArgument
	}
	poll, cred, err := uc.client.PollQRLogin(ctx, qrcodeKey)
	if err != nil {
		return nil, err
	}
	if poll.Status != QRLoginConfirmed {
		return poll, nil
	}
	if cred == nil || cred.Cookie == "" {
		return nil, ErrPassportUnavailable
	}
	if err := uc.repo.SaveCredential(ctx, cred); err != nil {
		return nil, err
	}
	return poll, nil
}

// AccountStatus 查询当前登录账号。无凭据返回已登出；有凭据则向平台
// 核验。平台不可达时返回错误，而不是误报登出。凭据失效不删除凭据，
// 让用户重新扫码或手动登出。
func (uc *AccountUsecase) AccountStatus(ctx context.Context) (*AccountInfo, error) {
	cred, err := uc.repo.GetCredential(ctx)
	if errors.Is(err, ErrCredentialNotFound) {
		return &AccountInfo{State: AccountLoggedOut}, nil
	}
	if err != nil {
		return nil, err
	}
	info, err := uc.client.AccountInfo(ctx, cred.Cookie)
	if err != nil {
		return nil, err
	}
	if info.State != AccountLoggedIn {
		return &AccountInfo{State: AccountExpired}, nil
	}
	return info, nil
}

// Logout 本地登出：删除数据库凭据并清除内存登录态。
func (uc *AccountUsecase) Logout(ctx context.Context) error {
	return uc.repo.DeleteCredential(ctx)
}
