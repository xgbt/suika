package service

import (
	v1 "suika/api/chat/v1"
	"suika/internal/biz"
)

type ChatService struct {
	v1.UnimplementedChatServiceServer

	uc *biz.ChatUsecase
}

func NewChatService(uc *biz.ChatUsecase) *ChatService {
	return &ChatService{
		uc: uc,
	}
}

func (s *ChatService) Chat(req *v1.ChatRequest, stream v1.ChatService_ChatServer) error {
	ctx := stream.Context()
	messages := make([]biz.ChatMessage, 0, len(req.GetMessages()))
	for _, m := range req.GetMessages() {
		messages = append(messages, biz.ChatMessage{
			Role:    toChatRoleDO(m.GetRole()),
			Content: m.GetContent(),
		})
	}

	return s.uc.Chat(ctx, messages, func(textDelta string) error {
		return stream.Send(&v1.ChatEvent{
			TextDelta: textDelta,
		})
	})
}

func toChatRoleDO(role v1.ChatRole) biz.ChatRole {
	switch role {
	case v1.ChatRole_CHAT_ROLE_USER:
		return biz.ChatRoleUser
	case v1.ChatRole_CHAT_ROLE_ASSISTANT:
		return biz.ChatRoleAssistant
	default:
		return ""
	}
}
