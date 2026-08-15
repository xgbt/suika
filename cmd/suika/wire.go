//go:build wireinject
// +build wireinject

// 构建标签确保该桩文件不会进入最终构建。

package main

import (
	"log/slog"

	"suika/internal/biz"
	"suika/internal/conf"
	"suika/internal/data"
	"suika/internal/server"
	"suika/internal/service"

	"github.com/go-kratos/kratos/v3"
	"github.com/google/wire"
)

// wireApp 初始化 Kratos 应用。
func wireApp(*conf.Server, *conf.Data, *conf.Recorder, *slog.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, biz.ProviderSet, service.ProviderSet, newApp))
}
