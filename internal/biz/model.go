package biz

import (
	"context"
	"strings"
	"sync"

	v1 "suika/api/chat/v1"

	"github.com/go-kratos/kratos/v3/errors"
)

var ErrModelConfigInternal = errors.InternalServer(v1.ErrorReason_ERROR_REASON_INTERNAL.String(), "model configuration storage failed")

const (
	ModelProviderDeepSeek = "deepseek"
	ModelDeepSeekFlash    = "deepseek-flash"
)

// ModelConfig 是当前模型配置。APIKey 只供后端使用，禁止序列化。
type ModelConfig struct {
	Provider string
	Model    string
	APIKey   string `json:"-"`
}

type ModelRepo interface {
	GetConfig(context.Context) (*ModelConfig, error)
	SaveConfig(context.Context, *ModelConfig) error
	DeleteConfig(context.Context) error
}

// ChatModelClient 隔离模型厂商 SDK，配置只在后端传递。
type ChatModelClient interface {
	New(context.Context, *ModelConfig) (ChatModel, error)
	ListModels(context.Context, string) ([]AvailableModel, error)
}

type AvailableModel struct {
	ID   string
	Name string
}

type ModelUsecase struct {
	repo   ModelRepo
	client ChatModelClient
	mu     sync.Mutex
}

func NewModelUsecase(repo ModelRepo, client ChatModelClient) *ModelUsecase {
	return &ModelUsecase{
		repo:   repo,
		client: client,
	}
}

func (uc *ModelUsecase) GetConfig(ctx context.Context) (*ModelConfig, error) {
	return uc.repo.GetConfig(ctx)
}

func SupportedChatModel(provider, model string) bool {
	return provider == ModelProviderDeepSeek && strings.TrimSpace(model) != "" && len(model) <= 256
}

func validModelAPIKey(key string) bool {
	return len(key) <= 4096 && strings.IndexFunc(key, func(r rune) bool {
		return r <= ' ' || r >= 127
	}) < 0
}

// resolveKey 根据提供的 provider 和 key 解析出有效的 APIKey，如果 key 为空则尝试从当前配置中获取。
func (uc *ModelUsecase) resolveKey(ctx context.Context, provider, key string) (string, error) {
	if provider != ModelProviderDeepSeek || !validModelAPIKey(key) {
		return "", ErrChatInvalidArgument
	}
	if key != "" {
		return key, nil
	}
	current, err := uc.repo.GetConfig(ctx)
	if err != nil {
		return "", err
	}
	if current == nil || current.Provider != provider || current.APIKey == "" {
		return "", ErrChatInvalidArgument
	}
	return current.APIKey, nil
}

func (uc *ModelUsecase) ListModels(ctx context.Context, provider, key string) ([]AvailableModel, error) {
	key, err := uc.resolveKey(ctx, provider, key)
	if err != nil {
		return nil, err
	}
	return uc.client.ListModels(ctx, key)
}

func (uc *ModelUsecase) SaveConfig(ctx context.Context, config ModelConfig) (*ModelConfig, error) {
	if !SupportedChatModel(config.Provider, config.Model) {
		return nil, ErrChatInvalidArgument
	}

	uc.mu.Lock()
	defer uc.mu.Unlock()
	key, err := uc.resolveKey(ctx, config.Provider, config.APIKey)
	if err != nil {
		return nil, err
	}
	config.APIKey = key
	models, err := uc.client.ListModels(ctx, key)
	if err != nil {
		return nil, err
	}
	found := false
	for _, model := range models {
		if model.ID == config.Model {
			found = true
			break
		}
	}
	if !found {
		return nil, ErrChatInvalidArgument
	}
	if err := uc.repo.SaveConfig(ctx, &config); err != nil {
		return nil, err
	}
	return &config, nil
}

func (uc *ModelUsecase) DeleteConfig(ctx context.Context) error {
	uc.mu.Lock()
	defer uc.mu.Unlock()
	return uc.repo.DeleteConfig(ctx)
}
