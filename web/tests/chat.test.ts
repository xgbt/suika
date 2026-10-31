import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { TestContext } from 'node:test';
import { chat, ChatRole } from '../src/api/chat.ts';

const messages = [{
  role: ChatRole.USER,
  content: '有哪些直播间？'
}];
const encoder = new TextEncoder();

function mockResponse(context: TestContext, response: Response) {
  return context.mock.method(globalThis, 'fetch', async () => response);
}

function streamResponse(chunks: Uint8Array[]) {
  return new Response(new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  }), {
    headers: {
      'Content-Type': 'text/event-stream'
    }
  });
}

test('POSTs history and decodes split UTF-8, CRLF, comments and multiple events', async (context) => {
  const bytes = encoder.encode(
    ': heartbeat\r\n\r\nevent: message\r\ndata: {"text_delta":"直播"}\r\n\r\n' +
    'data: {"text_delta":"间"}\n\n',
  );
  const fetch = mockResponse(context, streamResponse(Array.from(bytes, (byte) => Uint8Array.of(byte))));
  const deltas: string[] = [];
  const signal = new AbortController().signal;
  await chat(messages, signal, (delta) => deltas.push(delta));
  assert.equal(deltas.join(''), '直播间');
  assert.equal(fetch.mock.calls[0].arguments[0], '/v1/chat');
  const options = fetch.mock.calls[0].arguments[1];
  assert.equal(options?.method, 'POST');
  assert.equal(options?.signal, signal);
  assert.deepEqual(JSON.parse(String(options?.body)), {
    messages
  });
});

test('surfaces an SSE error even after partial text', async (context) => {
  mockResponse(context, streamResponse([encoder.encode(
    'data: {"text_delta":"部分"}\n\nevent: error\ndata: {"message":"模型不可用"}\n\n',
  )]));
  const deltas: string[] = [];
  await assert.rejects(chat(messages, new AbortController().signal, (delta) => deltas.push(delta)), /模型不可用/);
  assert.deepEqual(deltas, ['部分']);
});

test('surfaces HTTP errors before streaming starts', async (context) => {
  mockResponse(context, new Response('{"message":"chat model unavailable"}', {
    status: 503,
    headers: {
      'Content-Type': 'application/json'
    },
  }));
  await assert.rejects(chat(messages, new AbortController().signal, () => { }), /chat model unavailable/);
});

test('rejects invalid payloads and incomplete events', async (context) => {
  const fetch = mockResponse(context, streamResponse([encoder.encode('data: {}\n\n')]));
  await assert.rejects(chat(messages, new AbortController().signal, () => { }), /无效的文本事件/);
  fetch.mock.mockImplementation(async () => streamResponse([encoder.encode('data: {"text_delta":"未完成"}')]));
  await assert.rejects(chat(messages, new AbortController().signal, () => { }), /意外结束/);
});

test('rejects a non-SSE success response', async (context) => {
  mockResponse(context, new Response('{}', {
    headers: {
      'Content-Type': 'application/json'
    }
  }));
  await assert.rejects(chat(messages, new AbortController().signal, () => { }), /未返回 SSE/);
});

test('preserves cancellation instead of displaying a network error', async (context) => {
  const controller = new AbortController();
  context.mock.method(globalThis, 'fetch', async () => {
    controller.abort();
    throw controller.signal.reason;
  });
  await assert.rejects(chat(messages, controller.signal, () => { }), {
    name: 'AbortError'
  });
});
