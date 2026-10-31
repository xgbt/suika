import { useEffect, useRef, useState } from 'react';
import { Alert, Button, Empty, Input, Space, Typography } from 'antd';
import { ClearOutlined, RobotOutlined, SendOutlined, SettingOutlined, StopOutlined } from '@ant-design/icons';
import { chat, ChatRole } from '../api/chat';
import type { ChatMessage } from '../api/chat';
import { modelConfigApi } from '../api/modelConfig';
import type { ModelConfig } from '../api/modelConfig';
import ModelConfigModal from './ModelConfigModal';
import './Assistant.css';

interface DisplayMessage extends ChatMessage {
  status?: 'streaming' | 'complete' | 'stopped' | 'error';
}

export default function Assistant() {
  const [messages, setMessages] = useState<DisplayMessage[]>([]);
  const [input, setInput] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [config, setConfig] = useState<ModelConfig>();
  const [configError, setConfigError] = useState('');
  const [configOpen, setConfigOpen] = useState(false);
  const history = useRef<ChatMessage[]>([]);
  const controller = useRef<AbortController | null>(null);
  const end = useRef<HTMLDivElement>(null);

  useEffect(() => () => controller.current?.abort(), []);
  useEffect(() => {
    let cancelled = false;
    modelConfigApi.get().then(({ config: current }) => {
      if (!cancelled) setConfig(current);
    }).catch((cause: unknown) => {
      if (!cancelled) setConfigError(cause instanceof Error ? cause.message : '模型配置加载失败');
    });
    return () => {
      cancelled = true;
    };
  }, []);
  useEffect(() => {
    end.current?.scrollIntoView({
      block: 'nearest'
    });
  }, [messages]);

  async function send() {
    const content = input.trim();
    if (!content || controller.current || !config?.configured) return;
    const request = new AbortController();
    controller.current = request;
    setBusy(true);
    setInput('');
    setError('');
    const user: ChatMessage = {
      role: ChatRole.USER,
      content
    };
    const requestMessages = [...history.current, user];
    setMessages((previous) => [
      ...previous,
      user,
      {
        role: ChatRole.ASSISTANT,
        content: '',
        status: 'streaming'
      },
    ]);

    function updateReply(reply: string, status: DisplayMessage['status']) {
      setMessages((previous) => [
        ...previous.slice(0, -1),
        {
          role: ChatRole.ASSISTANT,
          content: reply,
          status
        },
      ]);
    }

    let reply = '';
    try {
      await chat(requestMessages, request.signal, (delta) => {
        reply += delta;
        updateReply(reply, 'streaming');
      });
      if (request.signal.aborted) {
        updateReply(reply, 'stopped');
      } else {
        if (!reply.trim()) throw new Error('智能助手没有返回回答，请重试');
        history.current = [...requestMessages, {
          role: ChatRole.ASSISTANT,
          content: reply
        }];
        updateReply(reply, 'complete');
      }
    } catch (cause: unknown) {
      if (request.signal.aborted) {
        updateReply(reply, 'stopped');
      } else {
        setError(cause instanceof Error ? cause.message : '智能助手请求失败');
        updateReply(reply, 'error');
        setInput(content);
      }
    } finally {
      controller.current = null;
      setBusy(false);
    }
  }

  return (
    <section className="assistant">
      <div className="assistant-toolbar">
        <Typography.Text type="secondary">
          DeepSeek · {config?.configured ? config.model : '尚未配置'} · 只读查询直播间信息
        </Typography.Text>
        <Space>
          <Button
            icon={<SettingOutlined />}
            onClick={() => {
              setConfigError('');
              modelConfigApi.get().then(({ config: current }) => {
                setConfig(current);
                setConfigOpen(true);
              }).catch((cause: unknown) => setConfigError(cause instanceof Error ? cause.message : '模型配置加载失败'));
            }}
          >模型配置</Button>
          <Button
            icon={<ClearOutlined />}
            disabled={busy || messages.length === 0}
            onClick={() => {
              history.current = [];
              setMessages([]);
              setError('');
              setInput('');
            }}
          >
            清空对话
          </Button>
        </Space>
      </div>
      <div className="assistant-messages" role="log" aria-label="对话记录" aria-live="polite">
        {messages.length === 0 ? (
          <div className="assistant-empty">
            <Empty image={<RobotOutlined />} description="向智能助手查询 Suika 管理的直播间">
              <Button onClick={() => setInput('有哪些直播间正在录制？')}>有哪些直播间正在录制？</Button>
            </Empty>
          </div>
        ) : messages.map((entry, index) => (
          <article className={`assistant-message assistant-message-${entry.role}`} key={index}>
            <Typography.Text strong>{entry.role === ChatRole.USER ? '你' : '智能助手'}</Typography.Text>
            <div className="assistant-message-text">
              {entry.content || (entry.status === 'streaming' ? '正在思考…' : '未收到回答')}
            </div>
            {entry.status === 'streaming' && entry.content && <Typography.Text type="secondary">正在回答…</Typography.Text>}
            {entry.status === 'stopped' && <Typography.Text type="secondary">已停止，此轮不会加入后续对话上下文</Typography.Text>}
            {entry.status === 'error' && <Typography.Text type="danger">回答失败，此轮不会加入后续对话上下文</Typography.Text>}
          </article>
        ))}
        <div ref={end} />
      </div>
      {error && <Alert type="error" showIcon title={error} />}
      {configError && <Alert type="error" showIcon title={configError} />}
      {config && !config.configured && <Alert type="info" showIcon title="请先通过「模型配置」保存 DeepSeek API key。" />}
      <form className="assistant-composer" onSubmit={(event) => {
        event.preventDefault();
        void send();
      }}>
        <Input.TextArea
          aria-label="输入消息"
          placeholder="输入问题，Enter 发送，Shift + Enter 换行"
          value={input}
          disabled={busy}
          autoSize={{
            minRows: 2,
            maxRows: 6
          }}
          onChange={(event) => setInput(event.target.value)}
          onPressEnter={(event) => {
            if (!event.shiftKey && !event.nativeEvent.isComposing) {
              event.preventDefault();
              void send();
            }
          }}
        />
        <div className="assistant-composer-footer">
          <Typography.Text type="secondary">刷新页面将清除对话；模型配置会保留。</Typography.Text>
          <Space>
            {busy ? (
              <Button icon={<StopOutlined />} onClick={() => controller.current?.abort()}>停止生成</Button>
            ) : (
              <Button type="primary" htmlType="submit" icon={<SendOutlined />} disabled={!input.trim() || !config?.configured}>发送</Button>
            )}
          </Space>
        </div>
      </form>
      {configOpen && config && <ModelConfigModal config={config} onClose={() => setConfigOpen(false)} onSaved={setConfig} />}
    </section>
  );
}
