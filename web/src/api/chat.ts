export const ChatRole = {
  USER: 1,
  ASSISTANT: 2,
} as const;

export interface ChatMessage {
  role: (typeof ChatRole)[keyof typeof ChatRole];
  content: string;
}

function errorMessage(value: unknown, fallback: string): string {
  if (typeof value === 'object' && value !== null && 'message' in value &&
    typeof value.message === 'string') {
    return value.message;
  }
  return fallback;
}

function receiveEvent(frame: string, onDelta: (text: string) => void): void {
  let event = 'message';
  const data: string[] = [];
  for (const line of frame.split(/\r\n|\r|\n/)) {
    if (line.startsWith(':')) continue;
    const colon = line.indexOf(':');
    const field = colon < 0 ? line : line.slice(0, colon);
    const value = colon < 0 ? '' : line.slice(colon + 1).replace(/^ /, '');
    if (field === 'event') event = value;
    if (field === 'data') data.push(value);
  }
  if (data.length === 0) return;
  const payload: unknown = JSON.parse(data.join('\n'));
  if (event === 'error') {
    throw new Error(errorMessage(payload, '智能助手请求失败'));
  }
  if (event !== 'message') return;
  if (typeof payload !== 'object' || payload === null || !('text_delta' in payload) ||
    typeof payload.text_delta !== 'string') {
    throw new Error('智能助手返回了无效的文本事件');
  }
  onDelta(payload.text_delta);
}

export async function chat(
  messages: ChatMessage[],
  signal: AbortSignal,
  onDelta: (text: string) => void,
): Promise<void> {
  let response: Response;
  try {
    response = await fetch('/v1/chat', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'text/event-stream'
      },
      body: JSON.stringify({
        messages
      }),
      signal,
    });
  } catch (error: unknown) {
    if (!signal.aborted && error instanceof TypeError) {
      throw new Error('无法连接服务端，请确认 Suika 后端已启动（localhost:8000）', {
        cause: error
      });
    }
    throw error;
  }
  if (!response.ok) {
    const body = await response.text();
    let message = `智能助手请求失败（HTTP ${response.status}）`;
    if (response.headers.get('Content-Type')?.includes('application/json')) {
      message = errorMessage(JSON.parse(body), message);
    }
    throw new Error(message);
  }
  if (!response.headers.get('Content-Type')?.includes('text/event-stream') || !response.body) {
    throw new Error('智能助手未返回 SSE 数据流');
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  try {
    for (; ;) {
      const { value, done } = await reader.read();
      buffer += decoder.decode(value, {
        stream: !done
      });
      let separator: RegExpExecArray | null;
      while ((separator = /\r\n\r\n|\n\n|\r\r/.exec(buffer)) !== null) {
        const frame = buffer.slice(0, separator.index);
        buffer = buffer.slice(separator.index + separator[0].length);
        receiveEvent(frame, onDelta);
      }
      if (done) {
        if (buffer.trim() !== '') throw new Error('智能助手数据流意外结束，请重试');
        return;
      }
    }
  } finally {
    try {
      await reader.cancel();
    } finally {
      reader.releaseLock();
    }
  }
}
