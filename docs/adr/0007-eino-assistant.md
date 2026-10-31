# A minimal Eino ADK assistant with database-backed model configuration

Suika adds conversational room queries without implementing the planned
cutting/publishing workflow. Use Eino's official `adk.ChatModelAgent` and
streaming `adk.Runner`, with one read-only `list_rooms` tool. The tool reuses
`RoomUsecase`, returns 50 rooms per page and exposes `next_offset`; the agent
continues with the same filters rather than treating a page as the full list.
Do not add retrieval stores, workflow engines, tracing wrappers or tool-event
protocols before a concrete use case requires them.

`ChatService` accepts user/assistant history on every request and streams only
`ChatEvent.text_delta`; `api/chat/v1/chat.proto` defines only this streaming RPC.
Unary model discovery and configuration live in `ModelService`, defined
in `api/chat/v1/model.proto` and handled by `service/model.go`.
The HTTP paths and DTO shapes stay unchanged; gRPC configuration methods move
from `chat.v1.ChatService` to `chat.v1.ModelService`. Both services are
registered on both transports and wired independently.
Kratos v3 provides native SSE over HTTP and a gRPC
server stream; browsers use POST `fetch` with a `ReadableStream`. Normal EOF
completes the answer, and post-start errors use `event: error`. Kratos's HTTP
stream context drops cancellation as well as transport deadlines, so the
service uses the stream context as-is: the stream ends when `Send` fails after
the client disconnects. A disconnect while the model is still silent (e.g.
between tool calls) is only noticed at the next text write; this is accepted
for a single-user tool in exchange for not wrapping the service, changing
generated code or coupling biz to HTTP.
`internal/server` only constructs transports, installs middleware and registers
services (plus the existing recorder daemon lifecycle).
gRPC retains its native cancellation. HTTP/gRPC unary RPCs uniformly use
Kratos's native 15-second timeout; chat is not limited by that unary timeout.
The DeepSeek discovery HTTP client also has a 15-second timeout. There is no
custom timeout middleware or per-operation budget policy.

Support DeepSeek only for now through the official Eino DeepSeek component.
`ModelConfigRepo` and `ChatModelClient` are biz seams implemented in data.
Database configuration stays in `internal/data/model_config.go`; DeepSeek
SDK/model-discovery IO and its tests live in `internal/data/deepseek/`.
`data.NewChatModelClient` is a thin Wire constructor, following the Bilibili
client organization. HTTP contract/cancellation tests live in service's
external test package, including SSE cancellation after the unary deadline.
Persist a single provider/model/API-key row (`id=1`) in SQLite `model_configs`;
environment-variable API keys are no longer read. Each chat builds a model and
agent from a configuration snapshot, so saves/deletes affect new requests
immediately while existing requests retain their snapshot. No configuration
means chat is unavailable (503), not that recording or room management fails.
The provider and model fields leave room for future adapters without adding
multi-provider registries or multiple configurations now.

Discover IDs and names using authenticated official `GET
https://api.deepseek.com/models` through Suika, never directly from the browser.
A supplied key is used without persisting it; a blank key uses the stored key.
Saving revalidates the selected ID against the effective key's official list,
then persists. A blank key on update retains the old key; delete is idempotent.
`deepseek-flash` is only a default selection, not an allowlist. This deliberately
makes discovery and saving dependent on DeepSeek availability; a failed save
leaves the previous configuration unchanged.

Get/Save responses never include the API key. Credential SQL logging is silent,
and storage/platform errors must not expose keys, SQL parameters or response
bodies. SQLite stores the key unencrypted: the unauthenticated application is
for trusted local single-user use; protect the database and use HTTPS remotely.
The assistant sends conversation history and queried room data to DeepSeek.
The web conversation is kept in memory across tab switches, not reloads;
failed/stopped turns are excluded from subsequent context.
