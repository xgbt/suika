import { useEffect, useState } from 'react';
import { Alert, Button, Form, Input, Modal, Popconfirm, Select, Typography } from 'antd';
import { modelConfigApi } from '../api/modelConfig';
import type { AvailableModel, ModelConfig } from '../api/modelConfig';

interface Props {
  config: ModelConfig;
  onClose: () => void;
  onSaved: (config: ModelConfig) => void;
}

export default function ModelConfigModal({ config, onClose, onSaved }: Props) {
  const [form] = Form.useForm<{
    provider: string;
    model: string;
    api_key: string
  }>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [models, setModels] = useState<AvailableModel[]>([]);
  const [loadingModels, setLoadingModels] = useState(Boolean(config.configured));
  const [modelRequest, setModelRequest] = useState<{
    key: string
  } | undefined>(
    config.configured ? {
      key: ''
    } : undefined,
  );

  useEffect(() => {
    if (!modelRequest) return;
    const controller = new AbortController();
    modelConfigApi.listModels(config.provider, modelRequest.key, controller.signal).then(({ models: available }) => {
      if (controller.signal.aborted) return;
      if (available.length === 0) throw new Error('官方模型列表为空');
      setModels(available);
      if (!available.some((model) => model.id === form.getFieldValue('model'))) {
        form.setFieldValue('model', available[0].id);
      }
    }).catch((cause: unknown) => {
      if (!controller.signal.aborted) setError(cause instanceof Error ? cause.message : '官方模型列表加载失败');
    }).finally(() => {
      if (!controller.signal.aborted) setLoadingModels(false);
    });
    return () => controller.abort();
  }, [modelRequest, config.provider, form]);

  async function loadModels() {
    try {
      await form.validateFields(['api_key']);
    } catch {
      return;
    }
    setLoadingModels(true);
    setError('');
    setModels([]);
    setModelRequest({
      key: form.getFieldValue('api_key') || ''
    });
  }

  async function save(values: {
    provider: string;
    model: string;
    api_key: string
  }) {
    setBusy(true);
    setError('');
    try {
      const response = await modelConfigApi.save({
        ...values,
        api_key: values.api_key || ''
      });
      form.resetFields();
      onSaved(response.config);
      onClose();
    } catch (cause: unknown) {
      setError(cause instanceof Error ? cause.message : '模型配置保存失败');
    } finally {
      setBusy(false);
    }
  }

  async function remove() {
    setBusy(true);
    setError('');
    try {
      await modelConfigApi.delete();
      form.resetFields();
      onSaved({
        provider: 'deepseek',
        model: 'deepseek-flash',
        configured: false
      });
      onClose();
    } catch (cause: unknown) {
      setError(cause instanceof Error ? cause.message : '模型配置删除失败');
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal
      open
      title="模型配置"
      onCancel={onClose}
      closable={!busy}
      mask={{
        closable: !busy
      }}
      keyboard={!busy}
      footer={[
        <Popconfirm key="delete" title="删除配置后，智能助手将不可用，确定删除？" onConfirm={() => void remove()} disabled={busy || !config.configured}>
          <Button danger disabled={busy || !config.configured}>删除配置</Button>
        </Popconfirm>,
        <Button key="cancel" onClick={onClose} disabled={busy}>取消</Button>,
        <Button key="save" type="primary" loading={busy} disabled={loadingModels || models.length === 0} onClick={() => form.submit()}>保存</Button>,
      ]}
    >
      <Form
        form={form}
        layout="vertical"
        disabled={busy || loadingModels}
        initialValues={{
          provider: config.provider,
          model: config.model,
          api_key: ''
        }}
        onFinish={(values) => void save(values)}
      >
        <Form.Item name="provider" label="厂商" rules={[{
          required: true
        }]}>
          <Select options={[{
            label: 'DeepSeek',
            value: 'deepseek'
          }]} />
        </Form.Item>
        <Form.Item name="model" label="模型" rules={[{
          required: true
        }]}>
          <Select
            loading={loadingModels}
            options={models.map((model) => ({
              label: `${model.name} (${model.id})`,
              value: model.id
            }))}
            placeholder="填写 API key 后获取官方模型列表"
          />
        </Form.Item>
        <Form.Item
          name="api_key"
          label="API key"
          extra={config.configured ? '已保存 API key，留空保留原值；填写新值将替换。' : '首次配置请填写 API key。'}
          rules={[
            {
              required: !config.configured,
              message: '请填写 API key'
            },
            {
              pattern: /^\S*$/,
              message: 'API key 不能包含空格或换行'
            },
            {
              max: 4096,
              message: 'API key 长度超过限制'
            },
          ]}
        >
          <Input.Password
            autoComplete="off"
            onChange={() => {
              setModelRequest(undefined);
              setModels([]);
              setLoadingModels(false);
              setError('');
            }}
            placeholder={config.configured ? '留空保留原 key' : '输入 DeepSeek API key'}
          />
        </Form.Item>
        <Form.Item>
          <Button loading={loadingModels} disabled={busy} onClick={() => void loadModels()}>获取官方模型列表</Button>
        </Form.Item>
      </Form>
      <Typography.Paragraph type="secondary">
        API key 仅保存在后端 SQLite，不会从接口读回浏览器。数据库未加密，请妥善保管，仅用于可信的本地单用户环境。
        后端使用 API key 调用 DeepSeek 官方模型列表；获取不会保存 key。保存时再次校验模型可用性，新对话立即生效。
      </Typography.Paragraph>
      {error && <Alert type="error" showIcon title={error} />}
    </Modal>
  );
}
