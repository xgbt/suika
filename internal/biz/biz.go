package biz

import "github.com/google/wire"

// RepoOptions 聚合 biz 声明、data 实现的 repo 接口，由 Wire 按字段名填充。
// usecase 只要持有它就能使用当前所有 repo；新增 repo 时在这里加一个字段，
// 不必再改动每个 usecase 的构造签名。
type RepoOptions struct {
	RoomRepo         RoomRepo
	SessionStatsRepo SessionStatsRepo
	RecorderRepo     RecorderRepo
	CredentialRepo   CredentialRepo
	ModelRepo        ModelRepo
}

var ProviderSet = wire.NewSet(
	wire.Struct(new(RepoOptions), "*"),
	NewRoomRegistry,
	NewRecorderUsecase,
	NewRoomUsecase,
	NewAccountUsecase,
	NewChatUsecase,
	NewModelUsecase,
)
