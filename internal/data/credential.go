package data

import (
	"context"
	stderrors "errors"
	"time"

	"suika/internal/biz"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// credentialSingletonID 是凭据表单例行的固定主键。登录凭据全局唯一，
// 用常量主键保证"最多一行"的不变式。
const credentialSingletonID int64 = 1

// credentialPO 持久化 B 站登录凭据。
type credentialPO struct {
	ID           int64     `gorm:"primaryKey"`
	Cookie       string    `gorm:"not null"`
	RefreshToken string
	CreateTime   time.Time `gorm:"autoCreateTime"`
	UpdateTime   time.Time `gorm:"autoUpdateTime"`
}

func (credentialPO) TableName() string { return "credentials" }

func toCredentialPO(cred *biz.Credential) *credentialPO {
	if cred == nil {
		return nil
	}
	return &credentialPO{
		ID:           credentialSingletonID,
		Cookie:       cred.Cookie,
		RefreshToken: cred.RefreshToken,
		UpdateTime:   cred.UpdateTime,
	}
}

func toCredentialDO(po *credentialPO) *biz.Credential {
	if po == nil {
		return nil
	}
	return &biz.Credential{
		Cookie:       po.Cookie,
		RefreshToken: po.RefreshToken,
		UpdateTime:   po.UpdateTime,
	}
}

type credentialRepo struct {
	data *Data
}

func NewCredentialRepo(d *Data) biz.CredentialRepo {
	return &credentialRepo{data: d}
}

// GetCredential 读取凭据；无凭据返回 biz.ErrCredentialNotFound。
func (r *credentialRepo) GetCredential(ctx context.Context) (*biz.Credential, error) {
	var po credentialPO
	err := r.data.db.WithContext(ctx).Where("id = ?", credentialSingletonID).First(&po).Error
	if err != nil {
		if stderrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, biz.ErrCredentialNotFound
		}
		return nil, err
	}
	return toCredentialDO(&po), nil
}

// SaveCredential 以固定主键 upsert 凭据，持久化成功后热替换生效的
// cookie，使录制器无需重启即可使用新登录态。
func (r *credentialRepo) SaveCredential(ctx context.Context, cred *biz.Credential) error {
	po := toCredentialPO(cred)
	err := r.data.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			DoUpdates: clause.AssignmentColumns([]string{"cookie", "refresh_token", "update_time"}),
		}).
		Create(po).Error
	if err != nil {
		return err
	}
	r.data.setCookie(po.Cookie)
	return nil
}

// DeleteCredential 删除凭据并清除内存登录态；无凭据时幂等成功。
func (r *credentialRepo) DeleteCredential(ctx context.Context) error {
	if err := r.data.db.WithContext(ctx).Where("id = ?", credentialSingletonID).Delete(&credentialPO{}).Error; err != nil {
		return err
	}
	r.data.setCookie("")
	return nil
}
