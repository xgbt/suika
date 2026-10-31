package data

import (
	"context"
	stderrors "errors"

	"suika/internal/biz"

	"github.com/go-kratos/kratos/v3/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type modelConfigPO struct {
	ID       int64  `gorm:"primaryKey"`
	Provider string `gorm:"not null"`
	Model    string `gorm:"not null"`
	APIKey   string `gorm:"not null"`
}

func (modelConfigPO) TableName() string {
	return "model_configs"
}

type modelConfigRepo struct {
	data *Data
}

func NewModelConfigRepo(d *Data) biz.ModelConfigRepo {
	return &modelConfigRepo{
		data: d,
	}
}

// 凭据 SQL 禁止进入 GORM 日志；失败只记录操作名，不记录参数或驱动错误。
func (r *modelConfigRepo) db(ctx context.Context) *gorm.DB {
	return r.data.db.WithContext(ctx).Session(&gorm.Session{
		Logger: logger.Default.LogMode(logger.Silent),
	})
}

func (r *modelConfigRepo) Get(ctx context.Context) (*biz.ModelConfig, error) {
	var po modelConfigPO
	err := r.db(ctx).First(&po, 1).Error
	if stderrors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, modelConfigError("get")
	}
	return toModelConfigDO(&po), nil
}

func (r *modelConfigRepo) Save(ctx context.Context, config *biz.ModelConfig) error {
	err := r.db(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{
				Name: "id",
			},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"provider",
			"model",
			"api_key",
		}),
	}).Create(toModelConfigPO(config)).Error
	if err != nil {
		return modelConfigError("save")
	}
	return nil
}

func (r *modelConfigRepo) Delete(ctx context.Context) error {
	if err := r.db(ctx).Where("id = ?", 1).Delete(&modelConfigPO{}).Error; err != nil {
		return modelConfigError("delete")
	}
	return nil
}

func modelConfigError(operation string) error {
	log.Error("model configuration storage operation failed", "operation", operation)
	return biz.ErrModelConfigInternal
}

func toModelConfigPO(config *biz.ModelConfig) *modelConfigPO {
	return &modelConfigPO{
		ID:       1,
		Provider: config.Provider,
		Model:    config.Model,
		APIKey:   config.APIKey,
	}
}

func toModelConfigDO(po *modelConfigPO) *biz.ModelConfig {
	return &biz.ModelConfig{
		Provider: po.Provider,
		Model:    po.Model,
		APIKey:   po.APIKey,
	}
}
