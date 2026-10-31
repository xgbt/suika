package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"suika/internal/biz"
	"suika/internal/conf"
	"suika/internal/data"
	"suika/internal/server"
	"suika/internal/service"
)

type modelCatalogStub struct{}

func (modelCatalogStub) New(context.Context, *biz.ModelConfig) (biz.ChatModel, error) {
	return nil, biz.ErrChatUnavailable
}
func (modelCatalogStub) ListModels(context.Context, string) ([]biz.AvailableModel, error) {
	return []biz.AvailableModel{
		{
			ID:   biz.ModelDeepSeekFlash,
			Name: "DeepSeek Flash",
		},
	}, nil
}

func TestModelConfigHTTP(t *testing.T) {
	t.Setenv("DEEPSEEK_API_KEY", "test-environment-must-be-ignored")
	d, cleanup, err := data.NewData(&conf.Data{
		Database: &conf.Data_Database{
			Source: filepath.Join(t.TempDir(), "test.db"),
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	config := biz.NewModelConfigUsecase(data.NewModelConfigRepo(d), modelCatalogStub{})
	chat := service.NewChatService(biz.NewChatUsecase(config, data.NewChatModelClient(), nil))
	modelConfig := service.NewModelService(config)
	srv := httptest.NewServer(server.NewHTTPServer(&conf.Server{}, nil, nil, chat, modelConfig))
	defer srv.Close()
	key := "test-secret-placeholder"
	post := func(path, body string, status int) string {
		t.Helper()
		resp, err := srv.Client().Post(srv.URL+path, "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		result, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != status {
			t.Fatalf("%s: status %d, want %d", path, resp.StatusCode, status)
		}
		if strings.Contains(string(result), key) || strings.Contains(string(result), "api_key") {
			t.Fatal("configuration HTTP response leaked credential")
		}
		return string(result)
	}
	post("/v1/chat", `{"messages":[{"role":1,"content":"hi"}]}`, http.StatusServiceUnavailable)
	post("/v1/chat/config/get", `{}`, http.StatusOK)
	post("/v1/chat/models/list", `{"provider":"deepseek","api_key":"test"}`, http.StatusOK)
	post("/v1/chat/models/list", `{"provider":"deepseek"}`, http.StatusBadRequest)
	post("/v1/chat/config/save", `{"provider":"deepseek","model":"deepseek-flash"}`, http.StatusBadRequest)
	post("/v1/chat/config/save", `{"provider":"other","model":"deepseek-flash","api_key":"test"}`, http.StatusBadRequest)
	result := post("/v1/chat/config/save", `{"provider":"deepseek","model":"deepseek-flash","api_key":"`+key+`"}`, http.StatusOK)
	var reply struct {
		Config struct {
			Configured bool `json:"configured"`
		} `json:"config"`
	}
	if err := json.Unmarshal([]byte(result), &reply); err != nil {
		t.Fatal(err)
	}
	if !reply.Config.Configured {
		t.Fatal("saved config not reported configured")
	}
	post("/v1/chat/config/save", `{"provider":"deepseek","model":"deepseek-flash","api_key":""}`, http.StatusOK)
	stored, err := config.Get(context.Background())
	if err != nil || stored == nil || stored.APIKey != key {
		t.Fatal("blank update did not retain key")
	}
	post("/v1/chat/config/get", `{}`, http.StatusOK)
	post("/v1/chat/models/list", `{"provider":"deepseek"}`, http.StatusOK)
	post("/v1/chat/config/delete", `{}`, http.StatusOK)
	post("/v1/chat", `{"messages":[{"role":1,"content":"hi"}]}`, http.StatusServiceUnavailable)
	post("/v1/chat/config/delete", `{}`, http.StatusOK)
}
