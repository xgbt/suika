package server

import (
	accountv1 "suika/api/account/v1"
	v1 "suika/api/room/v1"
	"suika/internal/conf"
	"suika/internal/service"

	"github.com/go-kratos/kratos/v3/middleware/recovery"
	"github.com/go-kratos/kratos/v3/transport/grpc"
)

// NewGRPCServer 创建 gRPC 服务器。
func NewGRPCServer(c *conf.Server, room *service.RoomService, account *service.AccountService) *grpc.Server {
	var opts = []grpc.ServerOption{
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
	return srv
}
