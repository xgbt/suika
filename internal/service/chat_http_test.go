package service_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	chatv1 "suika/api/chat/v1"
	"suika/internal/biz"
	"suika/internal/conf"
	"suika/internal/data"
	"suika/internal/server"
	"suika/internal/service"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	kratoshttp "github.com/go-kratos/kratos/v3/transport/http"
)

type streamingChatModel struct {
	entered   chan struct{}
	cancelled chan struct{}
}

func (m *streamingChatModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	panic("test model only supports streaming")
}

func (m *streamingChatModel) WithTools([]*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	return m, nil
}

// Stream 持续输出文本直到上下文被取消，模拟一个长回答。
func (m *streamingChatModel) Stream(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	close(m.entered)
	reader, writer := schema.Pipe[*schema.Message](1)
	go func() {
		defer writer.Close()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				close(m.cancelled)
				return
			case <-time.After(3 * time.Second):
				return
			case <-ticker.C:
				writer.Send(schema.AssistantMessage("hello", nil), nil)
			}
		}
	}()
	return reader, nil
}

type streamingChatClient struct {
	modelCatalogStub
	model *streamingChatModel
}

func (c streamingChatClient) New(context.Context, *biz.ModelConfig) (biz.ChatModel, error) {
	return c.model, nil
}

func TestHTTPChatStopsAfterDisconnect(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
	}{
		{
			name: "during-stream",
		},
		{
			name:    "after-unary-timeout",
			timeout: 20 * time.Millisecond,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, cleanup, err := data.NewData(&conf.Data{
				Database: &conf.Data_Database{
					Source: filepath.Join(t.TempDir(), "test.db"),
				},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			cm := &streamingChatModel{
				entered:   make(chan struct{}),
				cancelled: make(chan struct{}),
			}
			client := streamingChatClient{
				model: cm,
			}
			config := biz.NewModelConfigUsecase(data.NewModelConfigRepo(d), client)
			if _, err := config.Save(context.Background(), biz.ModelConfig{
				Provider: biz.ModelProviderDeepSeek,
				Model:    biz.ModelDeepSeekFlash,
				APIKey:   "test-key",
			}); err != nil {
				t.Fatal(err)
			}
			chat := service.NewChatService(biz.NewChatUsecase(config, client, nil))
			httpServer := server.NewHTTPServer(&conf.Server{}, nil, nil, chat, nil)
			if tc.timeout > 0 {
				httpServer = kratoshttp.NewServer(kratoshttp.Timeout(tc.timeout))
				chatv1.RegisterChatServiceHTTPServer(httpServer, chat)
			}
			srv := httptest.NewServer(httpServer)
			defer srv.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/chat", bytes.NewBufferString(`{"messages":[{"role":1,"content":"hi"}]}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			response := make(chan *http.Response, 1)
			failed := make(chan error, 1)
			go func() {
				resp, err := srv.Client().Do(req)
				if err != nil {
					failed <- err
					return
				}
				response <- resp
			}()
			select {
			case <-cm.entered:
			case err := <-failed:
				t.Fatal(err)
			case <-time.After(time.Second):
				t.Fatal("model not started")
			}
			select {
			case resp := <-response:
				defer resp.Body.Close()
				if _, err := io.ReadFull(resp.Body, make([]byte, 5)); err != nil {
					t.Fatal(err)
				}
			case err := <-failed:
				t.Fatal(err)
			case <-time.After(time.Second):
				t.Fatal("stream did not start")
			}
			if tc.timeout > 0 {
				select {
				case <-cm.cancelled:
					t.Fatal("unary timeout cancelled SSE model")
				case <-time.After(3 * tc.timeout):
				}
			}
			cancel()
			select {
			case <-cm.cancelled:
			case <-time.After(time.Second):
				t.Fatal("HTTP disconnect did not stop the model")
			}
		})
	}
}
