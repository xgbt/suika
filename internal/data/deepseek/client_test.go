package deepseek

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-resty/resty/v2"
)

func TestDeepSeekModelDiscovery(t *testing.T) {
	status := http.StatusOK
	body := `{"object":"list","data":[{"id":"deepseek-flash","name":"DeepSeek-V4.1-Flash"},{"id":"new-model"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/models" {
			t.Error("invalid official endpoint request")
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Error("authorization missing")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	client := &Client{
		http: resty.New().SetBaseURL(server.URL),
	}
	models, err := client.ListModels(context.Background(), "test-key")
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 2 || models[0].Name != "DeepSeek-V4.1-Flash" || models[1].Name != "new-model" {
		t.Fatal("model IDs or display names not decoded")
	}
	for _, invalid := range []string{
		`{`,
	} {
		body = invalid
		if _, err := client.ListModels(context.Background(), "test-key"); err == nil {
			t.Error("invalid model list accepted")
		}
	}
	status = http.StatusUnauthorized
	body = `{"message":"test-key"}`
	if _, err := client.ListModels(context.Background(), "test-key"); err == nil {
		t.Error("unauthorized discovery succeeded")
	}
}
