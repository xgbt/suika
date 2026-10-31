package server

import (
	"time"

	accountv1 "suika/api/account/v1"
	chatv1 "suika/api/chat/v1"
	v1 "suika/api/room/v1"
	"suika/internal/conf"
	"suika/internal/service"

	"github.com/go-kratos/kratos/v3/middleware/recovery"
	"github.com/go-kratos/kratos/v3/transport/grpc"
)

// NewGRPCServer 创建 gRPC 服务器。
func NewGRPCServer(c *conf.Server, room *service.RoomService, account *service.AccountService, chat *service.ChatService, model *service.ModelService) *grpc.Server {
	var opts = []grpc.ServerOption{
		grpc.Timeout(15 * time.Second),
		grpc.Middleware(
			recovery.Recovery(),
		),
	}
	if addr := c.GetGrpc().GetAddr(); addr != "" {
		opts = append(opts, grpc.Address(addr))
	}
	srv := grpc.NewServer(opts...)
	v1.RegisterRoomServiceServer(srv, room)
	accountv1.RegisterAccountServiceServer(srv, account)
	chatv1.RegisterChatServiceServer(srv, chat)
	chatv1.RegisterModelServiceServer(srv, model)
	return srv
}
