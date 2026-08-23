package server

import (
	v1 "suika/api/room/v1"
	"suika/internal/conf"
	"suika/internal/service"

	"github.com/go-kratos/kratos/v3/middleware/recovery"
	"github.com/go-kratos/kratos/v3/middleware/validate"
	"github.com/go-kratos/kratos/v3/transport/http"

	"go.einride.tech/aip/fieldbehavior"
	"google.golang.org/protobuf/proto"
)

// NewHTTPServer 创建 HTTP 服务器。
func NewHTTPServer(c *conf.Server, room *service.RoomService) *http.Server {
	var opts = []http.ServerOption{
		http.Middleware(
			recovery.Recovery(),
			validate.Validator(func(req any) error {
				if msg, ok := req.(proto.Message); ok {
					if err := fieldbehavior.ValidateRequiredFields(msg); err != nil {
						return err
					}
				}
				return nil
			}),
		),
	}
	if addr := c.GetHttp().GetAddr(); addr != "" {
		opts = append(opts, http.Address(addr))
	}
	srv := http.NewServer(opts...)
	v1.RegisterRoomServiceHTTPServer(srv, room)
	return srv
}
