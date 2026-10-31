package deepseek

import (
	"context"
	"fmt"
	"time"

	"suika/internal/biz"

	einodeepseek "github.com/cloudwego/eino-ext/components/model/deepseek"
	"github.com/go-kratos/kratos/v3/log"
	"github.com/go-resty/resty/v2"
	"github.com/samber/lo"
)

const BaseURL = "https://api.deepseek.com"

type modelItem struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Client struct {
	http *resty.Client
}

func NewClient() biz.ChatModelClient {
	return &Client{
		http: resty.New().SetBaseURL(BaseURL).SetTimeout(15 * time.Second),
	}
}

func (c *Client) ListModels(ctx context.Context, key string) ([]biz.AvailableModel, error) {
	var result struct {
		Data []modelItem `json:"data"`
	}
	response, err := c.http.R().SetContext(ctx).SetAuthToken(key).SetResult(&result).Get("/models")
	if err != nil {
		log.Warn("DeepSeek model discovery request failed")
		return nil, biz.ErrChatUnavailable.WithCause(fmt.Errorf("DeepSeek model discovery failed"))
	}
	if response.StatusCode() != 200 {
		log.Warn("DeepSeek model discovery rejected", "status", response.StatusCode())
		return nil, biz.ErrChatUnavailable.WithCause(fmt.Errorf("DeepSeek model discovery HTTP %d", response.StatusCode()))
	}
	return lo.Map(result.Data, func(item modelItem, _ int) biz.AvailableModel {
		return biz.AvailableModel{
			ID:   item.ID,
			Name: lo.CoalesceOrEmpty(item.Name, item.ID),
		}
	}), nil
}

func (*Client) New(ctx context.Context, config *biz.ModelConfig) (biz.ChatModel, error) {
	if !biz.SupportedChatModel(config.Provider, config.Model) {
		return nil, biz.ErrChatInvalidArgument
	}
	cm, err := einodeepseek.NewChatModel(ctx, &einodeepseek.ChatModelConfig{
		APIKey: config.APIKey,
		Model:  config.Model,
	})
	if err != nil {
		return nil, err
	}
	return cm, nil
}
