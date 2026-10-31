package service_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	v1 "suika/api/chat/v1"
	"suika/internal/biz"
	"suika/internal/conf"
	"suika/internal/data"
	"suika/internal/server"
	"suika/internal/service"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestModelConfigGRPC(t *testing.T) {
	if len(v1.ChatService_ServiceDesc.Methods) != 0 || len(v1.ChatService_ServiceDesc.Streams) != 1 {
		t.Fatal("ChatService must contain only the Chat stream")
	}
	if len(v1.ModelService_ServiceDesc.Methods) != 4 || len(v1.ModelService_ServiceDesc.Streams) != 0 {
		t.Fatal("ModelService must contain only four unary methods")
	}
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
	chat := service.NewChatService(biz.NewChatUsecase(config, modelCatalogStub{}, nil))
	srv := server.NewGRPCServer(
		&conf.Server{},
		service.NewRoomService(nil),
		service.NewAccountService(nil),
		chat,
		service.NewModelService(config),
	)
	listener := bufconn.Listen(1024 * 1024)
	defer listener.Close()
	stopped := make(chan error, 1)
	go func() {
		stopped <- srv.Serve(listener)
	}()
	defer func() {
		srv.GracefulStop()
		if err := <-stopped; err != nil {
			t.Error(err)
		}
	}()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := v1.NewModelServiceClient(conn)
	if _, err := client.ListModels(ctx, &v1.ListModelsRequest{
		Provider: biz.ModelProviderDeepSeek,
		ApiKey:   "test-key",
	}); err != nil {
		t.Fatal(err)
	}
	saved, err := client.SaveModelConfig(ctx, &v1.SaveModelConfigRequest{
		Provider: biz.ModelProviderDeepSeek,
		Model:    biz.ModelDeepSeekFlash,
		ApiKey:   "test-key",
	})
	if err != nil || !saved.GetConfig().GetConfigured() {
		t.Fatalf("save failed: %v", err)
	}
	got, err := client.GetModelConfig(ctx, &v1.GetModelConfigRequest{})
	if err != nil || got.GetConfig().GetModel() != biz.ModelDeepSeekFlash {
		t.Fatalf("get failed: %v", err)
	}
	if _, err := client.DeleteModelConfig(ctx, &v1.DeleteModelConfigRequest{}); err != nil {
		t.Fatal(err)
	}
	got, err = client.GetModelConfig(ctx, &v1.GetModelConfigRequest{})
	if err != nil || got.GetConfig().GetConfigured() {
		t.Fatalf("delete failed: %v", err)
	}
}
