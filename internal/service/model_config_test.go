package service

import (
	"encoding/json"
	"strings"
	"testing"

	"suika/internal/biz"
)

func TestModelConfigReplyNeverIncludesKey(t *testing.T) {
	config := &biz.ModelConfig{
		Provider: biz.ModelProviderDeepSeek,
		Model:    biz.ModelDeepSeekFlash,
		APIKey:   "test-private-key",
	}
	for _, value := range []any{
		config,
		toModelConfigDTO(config),
	} {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), config.APIKey) {
			t.Fatal("API key leaked in JSON")
		}
	}
	if !toModelConfigDTO(config).GetConfigured() {
		t.Fatal("configured flag missing")
	}
	if toModelConfigDTO(nil).GetConfigured() {
		t.Fatal("empty config marked configured")
	}
	if toModelConfigDTO(nil).GetModel() != biz.ModelDeepSeekFlash {
		t.Fatal("default model is not DeepSeek Flash")
	}
}
