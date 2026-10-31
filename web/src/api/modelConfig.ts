import { request } from './request';

export interface ModelConfig {
  provider: string;
  model: string;
  configured?: boolean;
}

export interface AvailableModel {
  id: string;
  name: string;
}

export const modelConfigApi = {
  listModels(provider: string, api_key: string, signal?: AbortSignal): Promise<{
    models: AvailableModel[]
  }> {
    return request('/v1/chat/models/list', {
      provider,
      api_key
    }, signal);
  },
  get(): Promise<{
    config: ModelConfig
  }> {
    return request('/v1/chat/config/get', {});
  },
  save(config: {
    provider: string;
    model: string;
    api_key: string
  }): Promise<{
    config: ModelConfig
  }> {
    return request('/v1/chat/config/save', config);
  },
  delete(): Promise<unknown> {
    return request('/v1/chat/config/delete', {});
  },
};
