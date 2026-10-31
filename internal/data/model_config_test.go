package data

import (
	"context"
	"path/filepath"
	"testing"

	"suika/internal/biz"
	"suika/internal/conf"
)

func TestModelConfigPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	open := func() (*Data, func()) {
		t.Helper()
		data, cleanup, err := NewData(&conf.Data{
			Database: &conf.Data_Database{
				Source: path,
			},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return data, cleanup
	}
	ctx := context.Background()
	data, cleanup := open()
	repo := NewModelConfigRepo(data)
	config, err := repo.Get(ctx)
	if err != nil || config != nil {
		t.Fatalf("empty config: %v", err)
	}
	key := "test-placeholder-not-a-real-key"
	if err := repo.Save(ctx, &biz.ModelConfig{
		Provider: biz.ModelProviderDeepSeek,
		Model:    biz.ModelDeepSeekFlash,
		APIKey:   key,
	}); err != nil {
		t.Fatal(err)
	}
	cleanup()
	data, cleanup = open()
	defer cleanup()
	repo = NewModelConfigRepo(data)
	config, err = repo.Get(ctx)
	if err != nil || config == nil || config.APIKey != key {
		t.Fatalf("config not persisted: %v", err)
	}
	if err := repo.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	config, err = repo.Get(ctx)
	if err != nil || config != nil {
		t.Fatalf("config not deleted: %v", err)
	}
}
