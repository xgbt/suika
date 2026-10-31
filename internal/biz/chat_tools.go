package biz

import (
	"context"
	"math"

	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/components/tool/utils"
)

type listRoomsInput struct {
	StreamerName  string `json:"streamer_name,omitempty" jsonschema:"description=按主播名称精确匹配，不填不过滤"`
	RecordEnabled *bool  `json:"record_enabled,omitempty" jsonschema:"description=按是否启用录制过滤，不填不过滤"`
	Offset        int    `json:"offset,omitempty" jsonschema:"description=分页偏移量，首次不填，后续使用返回的 next_offset"`
}

type roomOutput struct {
	RoomID        int64  `json:"room_id"`
	StreamerName  string `json:"streamer_name"`
	RoomTitle     string `json:"room_title"`
	RecordEnabled bool   `json:"record_enabled"`
	LiveStatus    string `json:"live_status"`
	RecordStatus  string `json:"record_status"`
}

type listRoomsOutput struct {
	Rooms      []roomOutput `json:"rooms"`
	NextOffset *int         `json:"next_offset,omitempty"`
}

const chatRoomPageSize = 50

// newChatTools 返回对话可用的工具，全部只读。
func newChatTools(rooms *RoomUsecase) ([]tool.BaseTool, error) {
	listRooms, err := utils.InferTool("list_rooms",
		"分页列出 Suika 管理的直播间及其直播状态、录制状态，可按主播名称、是否启用录制过滤。每页最多 50 条；有 next_offset 时用相同筛选条件继续查询，不能把一页当成完整清单。",
		func(ctx context.Context, in listRoomsInput) (listRoomsOutput, error) {
			if in.Offset < 0 || in.Offset > math.MaxInt-chatRoomPageSize {
				return listRoomsOutput{}, ErrChatInvalidArgument
			}
			query := ListQuery{
				RecordEnabled: in.RecordEnabled,
				Offset:        in.Offset,
				Limit:         chatRoomPageSize + 1,
			}
			if in.StreamerName != "" {
				query.StreamerName = &in.StreamerName
			}
			runtimes, err := rooms.ListRoomRuntimes(ctx, query)
			if err != nil {
				return listRoomsOutput{}, err
			}
			out := listRoomsOutput{
				Rooms: make([]roomOutput, 0, len(runtimes)),
			}
			if len(runtimes) > chatRoomPageSize {
				next := in.Offset + chatRoomPageSize
				out.NextOffset = &next
				runtimes = runtimes[:chatRoomPageSize]
			}
			for _, rt := range runtimes {
				out.Rooms = append(out.Rooms, roomOutput{
					RoomID:        rt.Room.RoomID,
					StreamerName:  rt.Room.StreamerName,
					RoomTitle:     rt.Room.RoomTitle,
					RecordEnabled: rt.Room.RecordEnabled,
					LiveStatus:    liveStatusText(rt.LiveStatus),
					RecordStatus:  recordStatusText(rt.RecordStatus),
				})
			}
			return out, nil
		})
	if err != nil {
		return nil, err
	}
	return []tool.BaseTool{
		listRooms,
	}, nil
}

func liveStatusText(s LiveStatus) string {
	switch s {
	case LiveStatusOnAir:
		return "live"
	case LiveStatusPreparing:
		return "preparing"
	default:
		return "unknown"
	}
}

func recordStatusText(s RecordStatus) string {
	switch s {
	case RecordStatusRecording:
		return "recording"
	case RecordStatusMerging:
		return "merging"
	case RecordStatusError:
		return "error"
	default:
		return "idle"
	}
}
