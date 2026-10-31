package biz

import (
	"context"
	"errors"
	"testing"
)

type catalogClient struct {
	fakeChatModelClient
	key  string
	fail bool
}

func (c *catalogClient) ListModels(_ context.Context, key string) ([]AvailableModel, error) {
	c.key = key
	if c.fail {
		return nil, ErrChatUnavailable
	}
	return []AvailableModel{
		{
			ID:   "official-new-model",
			Name: "Official New Model",
		},
	}, nil
}

func TestModelDiscoveryAndSavingOfficialModels(t *testing.T) {
	ctx := context.Background()
	repo := &fakeModelConfigRepo{}
	client := &catalogClient{}
	uc := NewModelConfigUsecase(repo, client)
	if _, err := uc.ListModels(ctx, ModelProviderDeepSeek, "test-unsaved"); err != nil {
		t.Fatal(err)
	}
	if repo.config != nil {
		t.Fatal("discovery persisted supplied key")
	}
	if client.key != "test-unsaved" {
		t.Fatal("supplied key not used")
	}
	config := ModelConfig{
		Provider: ModelProviderDeepSeek,
		Model:    "official-new-model",
		APIKey:   "test-stored",
	}
	if _, err := uc.Save(ctx, config); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.ListModels(ctx, ModelProviderDeepSeek, ""); err != nil {
		t.Fatal(err)
	}
	if client.key != "test-stored" {
		t.Fatal("stored key not used")
	}
	config.Model = "not-in-official-list"
	if _, err := uc.Save(ctx, config); !errors.Is(err, ErrChatInvalidArgument) {
		t.Fatal("unlisted model accepted")
	}
	client.fail = true
	config.APIKey = "test-replacement"
	if _, err := uc.Save(ctx, config); !errors.Is(err, ErrChatUnavailable) {
		t.Fatal("discovery failure ignored")
	}
	if repo.config.APIKey != "test-stored" || repo.config.Model != "official-new-model" {
		t.Fatal("failed save changed configuration")
	}
}
func TestModelConfigLifecycle(t *testing.T) {
	ctx := context.Background()
	uc := NewModelConfigUsecase(&fakeModelConfigRepo{}, fakeChatModelClient{})
	if _, err := uc.Save(ctx, ModelConfig{
		Provider: ModelProviderDeepSeek,
		Model:    ModelDeepSeekFlash,
	}); !errors.Is(err, ErrChatInvalidArgument) {
		t.Fatalf("initial save without key: %v", err)
	}
	first := ModelConfig{
		Provider: ModelProviderDeepSeek,
		Model:    ModelDeepSeekFlash,
		APIKey:   "test-first",
	}
	if _, err := uc.Save(ctx, first); err != nil {
		t.Fatal(err)
	}
	kept, err := uc.Save(ctx, ModelConfig{
		Provider: first.Provider,
		Model:    first.Model,
	})
	if err != nil || kept.APIKey != first.APIKey {
		t.Fatalf("retain key failed: %v", err)
	}
	first.APIKey = "test-replaced"
	if _, err := uc.Save(ctx, first); err != nil {
		t.Fatal(err)
	}
	got, err := uc.Get(ctx)
	if err != nil || got.APIKey != first.APIKey {
		t.Fatalf("replace key failed: %v", err)
	}
	if err := uc.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = uc.Get(ctx)
	if err != nil || got != nil {
		t.Fatalf("delete failed: %v", err)
	}
}

func TestModelConfigValidation(t *testing.T) {
	for _, config := range []ModelConfig{
		{
			Provider: "other",
			Model:    ModelDeepSeekFlash,
			APIKey:   "test",
		},
		{
			Provider: ModelProviderDeepSeek,
			Model:    "other",
			APIKey:   "test",
		},
		{
			Provider: ModelProviderDeepSeek,
			Model:    ModelDeepSeekFlash,
			APIKey:   " \n",
		},
		{
			Provider: ModelProviderDeepSeek,
			Model:    ModelDeepSeekFlash,
			APIKey:   "key\ninjected",
		},
	} {
		uc := NewModelConfigUsecase(&fakeModelConfigRepo{}, fakeChatModelClient{})
		if _, err := uc.Save(context.Background(), config); !errors.Is(err, ErrChatInvalidArgument) {
			t.Errorf("invalid config accepted")
		}
	}
}
