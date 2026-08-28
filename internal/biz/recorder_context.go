package biz

import "context"

type recorderRoomIDCtxKey struct{}

func withRoomID(ctx context.Context, roomID int64) context.Context {
	return context.WithValue(ctx, recorderRoomIDCtxKey{}, roomID)
}

func roomIDFromCtx(ctx context.Context) int64 {
	roomID, ok := ctx.Value(recorderRoomIDCtxKey{}).(int64)
	if !ok {
		panic("recorder room id is missing in context")
	}
	return roomID
}
