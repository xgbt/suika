// 共享的 HTTP POST 请求助手。所有 API 均为 POST + JSON。

export async function request<T>(path: string, body: unknown): Promise<T> {
  let res: Response;
  try {
    res = await fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch (error: unknown) {
    if (error instanceof TypeError) {
      throw new Error('无法连接服务端，请确认 Suika 后端已启动（localhost:8000）');
    }
    throw error;
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ message: res.statusText }));
    throw new Error(err.message ?? res.statusText);
  }
  return res.json();
}
