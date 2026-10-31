package biz

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

// scriptedChatModel 先请求调用 list_rooms，拿到工具结果后分两个分片给出最终回答。
type scriptedChatModel struct {
	calls int
	seen  [][]*schema.Message
}

func (m *scriptedChatModel) Generate(ctx context.Context, input []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	sr, err := m.Stream(ctx, input, opts...)
	if err != nil {
		return nil, err
	}
	defer sr.Close()
	var chunks []*schema.Message
	for {
		c, err := sr.Recv()
		if err != nil {
			break
		}
		chunks = append(chunks, c)
	}
	return schema.ConcatMessages(chunks)
}

func (m *scriptedChatModel) Stream(_ context.Context, input []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m.seen = append(m.seen, input)
	m.calls++
	if m.calls == 1 {
		return schema.StreamReaderFromArray([]*schema.Message{
			schema.AssistantMessage("", []schema.ToolCall{
				{
					ID: "call-1",
					Function: schema.FunctionCall{
						Name:      "list_rooms",
						Arguments: `{}`,
					},
				},
			}),
		}), nil
	}
	toolResult := input[len(input)-1].Content
	return schema.StreamReaderFromArray([]*schema.Message{
		schema.AssistantMessage("房间：", nil),
		schema.AssistantMessage(toolResult, nil),
	}), nil
}

func (m *scriptedChatModel) WithTools(_ []*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	return m, nil
}

type fakeModelConfigRepo struct {
	config *ModelConfig
}

func (r *fakeModelConfigRepo) Get(context.Context) (*ModelConfig, error) {
	if r.config == nil {
		return nil, nil
	}
	config := *r.config
	return &config, nil
}
func (r *fakeModelConfigRepo) Save(_ context.Context, config *ModelConfig) error {
	copy := *config
	r.config = &copy
	return nil
}
func (r *fakeModelConfigRepo) Delete(context.Context) error {
	r.config = nil
	return nil
}

type fakeChatModelClient struct {
	model ChatModel
}

func (c fakeChatModelClient) New(context.Context, *ModelConfig) (ChatModel, error) {
	return c.model, nil
}

func (fakeChatModelClient) ListModels(context.Context, string) ([]AvailableModel, error) {
	return []AvailableModel{
		{
			ID: ModelDeepSeekFlash,
		},
		{
			ID: "official-new-model",
		},
	}, nil
}

type recordingChatModelClient struct {
	keys []string
}

func (c *recordingChatModelClient) New(_ context.Context, config *ModelConfig) (ChatModel, error) {
	c.keys = append(c.keys, config.APIKey)
	return &scriptedChatModel{}, nil
}

func (*recordingChatModelClient) ListModels(ctx context.Context, key string) ([]AvailableModel, error) {
	return (fakeChatModelClient{}).ListModels(ctx, key)
}

func TestChatUsesLatestSavedModelConfig(t *testing.T) {
	ctx := context.Background()
	uc := newChatTestUsecase(t, nil)
	client := &recordingChatModelClient{}
	uc.client = client
	for _, key := range []string{
		"test-first",
		"test-second",
	} {
		if _, err := uc.config.Save(ctx, ModelConfig{
			Provider: ModelProviderDeepSeek,
			Model:    ModelDeepSeekFlash,
			APIKey:   key,
		}); err != nil {
			t.Fatal(err)
		}
		if err := uc.Chat(ctx, userMessage("有哪些直播间？"), func(string) error {
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(client.keys) != 2 || client.keys[0] != "test-first" || client.keys[1] != "test-second" {
		t.Fatal("chat did not pick up the latest configuration")
	}
	if err := uc.config.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := uc.Chat(ctx, userMessage("hi"), func(string) error {
		return nil
	}); !stderrors.Is(err, ErrChatUnavailable) {
		t.Fatalf("chat still enabled after config deletion: %v", err)
	}
}

func newChatTestUsecase(t *testing.T, cm ChatModel) *ChatUsecase {
	t.Helper()
	repo := &fakeRoomRepo{
		rooms: map[int64]*Room{
			1: {
				RoomID:       1,
				StreamerName: "主播A",
				RoomTitle:    "标题A",
			},
		},
	}
	reg, err := NewRoomRegistry(repo)
	if err != nil {
		t.Fatalf("NewRoomRegistry: %v", err)
	}
	configRepo := &fakeModelConfigRepo{}
	if cm != nil {
		configRepo.config = &ModelConfig{
			Provider: ModelProviderDeepSeek,
			Model:    ModelDeepSeekFlash,
			APIKey:   "test-key",
		}
	}
	return NewChatUsecase(NewModelConfigUsecase(configRepo, fakeChatModelClient{}), fakeChatModelClient{
		model: cm,
	}, NewRoomUsecase(repo, reg, &fakeStatsRepo{}))
}

func userMessage(s string) []ChatMessage {
	return []ChatMessage{
		{
			Role:    ChatRoleUser,
			Content: s,
		},
	}
}

func TestChatStreamsAnswerBuiltFromToolResult(t *testing.T) {
	cm := &scriptedChatModel{}
	uc := newChatTestUsecase(t, cm)

	var deltas []string
	err := uc.Chat(context.Background(), userMessage("有哪些直播间？"), func(d string) error {
		deltas = append(deltas, d)
		return nil
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}

	if len(deltas) < 2 {
		t.Fatalf("deltas = %q, want the answer split across several events", deltas)
	}
	answer := strings.Join(deltas, "")
	if !strings.HasPrefix(answer, "房间：") || !strings.Contains(answer, "主播A") {
		t.Errorf("answer = %q, want text built from the list_rooms result", answer)
	}
	if first := cm.seen[0][0]; first.Role != schema.System {
		t.Errorf("first model input role = %v, want the instruction as system prompt", first.Role)
	}
}

func TestChatStopsWhenSendFails(t *testing.T) {
	uc := newChatTestUsecase(t, &scriptedChatModel{})

	errGone := stderrors.New("client gone")
	err := uc.Chat(context.Background(), userMessage("有哪些直播间？"), func(string) error {
		return errGone
	})
	if !stderrors.Is(err, errGone) {
		t.Errorf("err = %v, want the send error", err)
	}
}

func TestChatUnavailableWithoutModel(t *testing.T) {
	uc := newChatTestUsecase(t, nil)

	err := uc.Chat(context.Background(), userMessage("hi"), func(string) error {
		return nil
	})
	if !stderrors.Is(err, ErrChatUnavailable) {
		t.Errorf("err = %v, want ErrChatUnavailable", err)
	}
}

func TestChatRejectsInvalidMessages(t *testing.T) {
	uc := newChatTestUsecase(t, nil)
	cases := map[string][]ChatMessage{
		"empty":         nil,
		"blank content": userMessage("  "),
		"unknown role": {
			{
				Role:    "system",
				Content: "x",
			},
			{
				Role:    ChatRoleUser,
				Content: "y",
			},
		},
		"last not user": {
			{
				Role:    ChatRoleUser,
				Content: "a",
			},
			{
				Role:    ChatRoleAssistant,
				Content: "b",
			},
		},
	}
	for name, msgs := range cases {
		err := uc.Chat(context.Background(), msgs, func(string) error {
			return nil
		})
		if !stderrors.Is(err, ErrChatInvalidArgument) {
			t.Errorf("%s: err = %v, want ErrChatInvalidArgument", name, err)
		}
	}
}

type pagedChatRoomRepo struct {
	fakeRoomRepo
}

func (r *pagedChatRoomRepo) ListRooms(ctx context.Context, query ListQuery) ([]*Room, error) {
	rooms, err := r.fakeRoomRepo.ListRooms(ctx, query)
	if err != nil {
		return nil, err
	}
	filtered := make([]*Room, 0, len(rooms))
	for _, room := range rooms {
		if query.StreamerName != nil && room.StreamerName != *query.StreamerName {
			continue
		}
		if query.RecordEnabled != nil && room.RecordEnabled != *query.RecordEnabled {
			continue
		}
		filtered = append(filtered, room)
	}
	start := min(query.Offset, len(filtered))
	if query.Limit <= 0 {
		return filtered[start:], nil
	}
	return filtered[start:min(start+query.Limit, len(filtered))], nil
}

func TestChatRoomToolPagination(t *testing.T) {
	for _, count := range []int{
		0,
		50,
		51,
		101,
	} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			repo := &pagedChatRoomRepo{
				fakeRoomRepo{
					rooms: make(map[int64]*Room),
				},
			}
			for id := range count {
				repo.rooms[int64(id+1)] = &Room{
					RoomID:        int64(id + 1),
					StreamerName:  "selected",
					RecordEnabled: true,
				}
			}
			repo.rooms[999] = &Room{
				RoomID:       999,
				StreamerName: "excluded",
			}
			registry, err := NewRoomRegistry(repo)
			if err != nil {
				t.Fatal(err)
			}
			tools, err := newChatTools(NewRoomUsecase(repo, registry, &fakeStatsRepo{}))
			if err != nil {
				t.Fatal(err)
			}
			invoke := tools[0].(tool.InvokableTool).InvokableRun
			total, offset := 0, 0
			for {
				result, err := invoke(context.Background(), fmt.Sprintf(`{"streamer_name":"selected","record_enabled":true,"offset":%d}`, offset))
				if err != nil {
					t.Fatal(err)
				}
				var page listRoomsOutput
				if err := json.Unmarshal([]byte(result), &page); err != nil {
					t.Fatal(err)
				}
				if len(page.Rooms) > chatRoomPageSize {
					t.Fatal("page exceeded 50 rooms")
				}
				for i, room := range page.Rooms {
					if room.RoomID != int64(offset+i+1) {
						t.Fatal("room skipped, duplicated or filter lost")
					}
				}
				total += len(page.Rooms)
				if page.NextOffset == nil {
					break
				}
				if *page.NextOffset != offset+chatRoomPageSize || total >= count {
					t.Fatal("incorrect continuation offset")
				}
				offset = *page.NextOffset
			}
			if total != count {
				t.Fatalf("returned %d rooms, want %d", total, count)
			}
			for _, invalid := range []int{
				-1,
				math.MaxInt,
			} {
				if _, err := invoke(context.Background(), fmt.Sprintf(`{"offset":%d}`, invalid)); err == nil {
					t.Fatal("invalid offset accepted")
				}
			}
		})
	}
}
