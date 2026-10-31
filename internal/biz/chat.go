package biz

import (
	"context"
	stderrors "errors"
	"io"
	"strings"

	chatv1 "suika/api/chat/v1"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/go-kratos/kratos/v3/errors"
)

var (
	ErrChatInvalidArgument = errors.BadRequest(chatv1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT.String(), "invalid chat argument")
	ErrChatUnavailable     = errors.ServiceUnavailable(chatv1.ErrorReason_ERROR_REASON_UNAVAILABLE.String(), "chat model unavailable")
)

const chatInstruction = `你是 Suika 的素材助手。Suika 管理 B 站直播间并录制直播。
回答直播间相关的问题时，先调用工具查询，只依据工具返回的内容作答；查不到就如实说明。
回答简洁，使用中文。`

// ChatModel 是对话使用的大模型，由 data 层依据配置构建。
type ChatModel = model.ToolCallingChatModel

type ChatRole string

const (
	ChatRoleUser      ChatRole = "user"
	ChatRoleAssistant ChatRole = "assistant"
)

type ChatMessage struct {
	Role    ChatRole
	Content string
}

// ChatUsecase 用 Eino ADK 的 ChatModelAgent（ReAct）编排对话：模型按需调用只读工具，
// 再给出最终回答。
type ChatUsecase struct {
	config *ModelConfigUsecase
	client ChatModelClient
	rooms  *RoomUsecase
}

func NewChatUsecase(config *ModelConfigUsecase, client ChatModelClient, rooms *RoomUsecase) *ChatUsecase {
	return &ChatUsecase{
		config: config,
		client: client,
		rooms:  rooms,
	}
}

func (uc *ChatUsecase) newRunner(ctx context.Context) (*adk.Runner, error) {
	config, err := uc.config.Get(ctx)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return nil, ErrChatUnavailable
	}
	cm, err := uc.client.New(ctx, config)
	if err != nil {
		return nil, ErrChatUnavailable.WithCause(err)
	}
	tools, err := newChatTools(uc.rooms)
	if err != nil {
		return nil, err
	}
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:        "suika_assistant",
		Description: "Suika 素材助手",
		Instruction: chatInstruction,
		Model:       cm,
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: tools,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return adk.NewRunner(ctx, adk.RunnerConfig{
		Agent:           agent,
		EnableStreaming: true,
	}), nil
}

// Chat 流式回答：回答文本的增量片段依次交给 send。send 返回错误（如客户端断开）会终止对话。
func (uc *ChatUsecase) Chat(ctx context.Context, messages []ChatMessage, send func(textDelta string) error) error {
	if len(messages) == 0 || messages[len(messages)-1].Role != ChatRoleUser {
		return ErrChatInvalidArgument
	}
	input := make([]adk.Message, 0, len(messages))
	for _, m := range messages {
		switch m.Role {
		case ChatRoleUser:
			input = append(input, schema.UserMessage(m.Content))
		case ChatRoleAssistant:
			input = append(input, schema.AssistantMessage(m.Content, nil))
		default:
			return ErrChatInvalidArgument
		}
		if strings.TrimSpace(m.Content) == "" {
			return ErrChatInvalidArgument
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	runner, err := uc.newRunner(ctx)
	if err != nil {
		return err
	}
	events := runner.Run(ctx, input)
	for {
		event, ok := events.Next()
		if !ok {
			return nil
		}
		if event.Err != nil {
			return ErrChatUnavailable.WithCause(event.Err)
		}
		out := event.Output
		if out == nil || out.MessageOutput == nil || out.MessageOutput.Role != schema.Assistant {
			continue
		}
		if err := forwardText(out.MessageOutput, send); err != nil {
			return err
		}
	}
}

// forwardText 转发助手消息的文本；携带工具调用的消息没有文本，自然被跳过。
func forwardText(mo *adk.MessageVariant, send func(string) error) error {
	if !mo.IsStreaming {
		return sendNonEmpty(mo.Message.Content, send)
	}
	defer mo.MessageStream.Close()
	for {
		chunk, err := mo.MessageStream.Recv()
		if stderrors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return ErrChatUnavailable.WithCause(err)
		}
		if err := sendNonEmpty(chunk.Content, send); err != nil {
			return err
		}
	}
}

func sendNonEmpty(text string, send func(string) error) error {
	if text == "" {
		return nil
	}
	return send(text)
}
