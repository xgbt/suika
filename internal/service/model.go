package service

import (
	"context"

	v1 "suika/api/chat/v1"
	"suika/internal/biz"

	"google.golang.org/protobuf/types/known/emptypb"
)

type ModelService struct {
	v1.UnimplementedModelServiceServer

	uc *biz.ModelConfigUsecase
}

func NewModelService(uc *biz.ModelConfigUsecase) *ModelService {
	return &ModelService{
		uc: uc,
	}
}

func (s *ModelService) ListModels(ctx context.Context, req *v1.ListModelsRequest) (*v1.ListModelsResponse, error) {
	models, err := s.uc.ListModels(ctx, req.GetProvider(), req.GetApiKey())
	if err != nil {
		return nil, err
	}
	response := &v1.ListModelsResponse{
		Models: make([]*v1.AvailableModel, 0, len(models)),
	}
	for _, model := range models {
		response.Models = append(response.Models, &v1.AvailableModel{
			Id:   model.ID,
			Name: model.Name,
		})
	}
	return response, nil
}

func (s *ModelService) GetModelConfig(ctx context.Context, _ *v1.GetModelConfigRequest) (*v1.GetModelConfigResponse, error) {
	config, err := s.uc.Get(ctx)
	if err != nil {
		return nil, err
	}
	return &v1.GetModelConfigResponse{
		Config: toModelConfigDTO(config),
	}, nil
}

func (s *ModelService) SaveModelConfig(ctx context.Context, req *v1.SaveModelConfigRequest) (*v1.SaveModelConfigResponse, error) {
	config, err := s.uc.Save(ctx, biz.ModelConfig{
		Provider: req.GetProvider(),
		Model:    req.GetModel(),
		APIKey:   req.GetApiKey(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.SaveModelConfigResponse{
		Config: toModelConfigDTO(config),
	}, nil
}

func (s *ModelService) DeleteModelConfig(ctx context.Context, _ *v1.DeleteModelConfigRequest) (*v1.DeleteModelConfigResponse, error) {
	if err := s.uc.Delete(ctx); err != nil {
		return nil, err
	}
	return &v1.DeleteModelConfigResponse{
		Empty: &emptypb.Empty{},
	}, nil
}

func toModelConfigDTO(config *biz.ModelConfig) *v1.ModelConfig {
	if config == nil {
		return &v1.ModelConfig{
			Provider: biz.ModelProviderDeepSeek,
			Model:    biz.ModelDeepSeekFlash,
		}
	}
	return &v1.ModelConfig{
		Provider:   config.Provider,
		Model:      config.Model,
		Configured: config.APIKey != "",
	}
}
