# Responses API Support Plan

This document outlines the phased plan for adding OpenAI Responses API support to `pi-mixlayer`, with the eventual goal of supporting Responses over WebSocket with delta updates. The extension's underlying transport should be configurable so we can swap between Chat Completions, Responses HTTP/SSE, and Responses WebSocket.

## Current State

- The extension registers all Mixlayer models with `api: "openai-completions"`.
- Pi has a built-in `openai-responses` API provider that handles Responses API over HTTP/SSE.
- Pi has a built-in `openai-codex-responses` API provider that handles Responses over WebSocket with delta updates, but that implementation is Codex-specific and not exposed as a reusable public API type.
- There is no public Pi API type such as `"openai-responses-websocket"` that an extension can opt into.

## Server Capabilities (as confirmed)

- **All Mixlayer models support the Responses API over both SSE and WebSocket.**
- **Delta updates are not yet implemented server-side.** The extension should not send `previous_response_id` or attempt delta payloads until the server supports it.
- **`previous_response_id` is not supported by any endpoint currently.** It will be added shortly; the extension should be structured so this is easy to enable.
- The base URL for Responses API calls is the same as Chat Completions: `https://models.mixlayer.ai/v1`.
- The Responses HTTP/SSE endpoint is `POST /v1/responses`.
- The Responses WebSocket endpoint is `GET /v1/responses` (upgrade to WebSocket).
- The WebSocket transport expects JSON messages with `type: "response.create"` and the same request body as the HTTP endpoint (minus `type`, `stream`, `stream_options`, and `background`).
- The server rejects `previous_response_id` on both HTTP and WebSocket with code `previous_response_not_found`.
- No special beta headers are required for WebSocket access.

## Goal

- Support Responses API for all Mixlayer models.
- Support Responses over WebSocket.
- Make the transport configurable per model or globally.
- Lay the groundwork for delta updates over WebSocket so they can be enabled quickly once the server supports them.

## Transport Options

The extension should support at least these transports:

1. `chat-completions` — current behavior, `api: "openai-completions"`.
2. `responses-sse` — Responses API over HTTP/SSE via Pi's built-in `openai-responses` provider.
3. `responses-websocket` — Responses API over WebSocket with full context per turn.
4. `responses-websocket-delta` — Responses API over WebSocket with delta updates and connection caching (server support pending).

## Phases

### Phase 0: Discovery

- ~~Confirm whether Mixlayer exposes Responses API support in the model list from `https://models.mixlayer.ai/_openrouter/models`.~~
  - **All models support Responses API over SSE and WebSocket.** Discovery via model list is not required.
- Identify any Mixlayer-specific headers, beta headers, or WebSocket endpoint requirements.
  - **No special beta headers required.** WebSocket endpoint is `GET /v1/responses`.
- ~~Determine whether Mixlayer supports `previous_response_id` for delta updates over WebSocket.~~
  - **Not supported yet.** Will be enabled in Phase 4 once server support lands.
- Decide how the extension will know which models support Responses API:
  - **All registered Mixlayer models support Responses API.** No hardcoded subset or separate provider needed.

**Deliverable:** A short discovery note added to this document or a linked issue.

### Phase 1: Configurable Transport Plumbing

- Introduce a transport configuration layer in the extension.
- Allow transport to be specified via:
  - Per-model config from the Mixlayer model list (e.g. `api: "responses"`, `transport: "websocket"`).
  - Environment variable (e.g. `MIXLAYER_TRANSPORT=responses-websocket`).
  - Extension-level default.
- Define an internal `MixlayerTransport` union type:
  ```ts
  type MixlayerTransport =
    | "chat-completions"
    | "responses-sse"
    | "responses-websocket"
    | "responses-websocket-delta";
  ```
- Update `toProviderModel()` to set `api` and per-model `compat` based on the resolved transport.
- Keep the default behavior unchanged (`chat-completions`) so existing users are not broken.

**Deliverable:** Extension builds and passes type checks; no functional change for the default transport.

**Status:** Implemented. Transport is read from `MIXLAYER_TRANSPORT` env var, then `~/.pi/agent/mixlayer-settings.json`, then defaults to `chat-completions`. The `/mixlayer-transport` slash command writes the settings file. The resolved transport is wired into `registerProvider`:
- `chat-completions` → `api: "openai-completions"`
- `responses-sse` → custom `api: "mixlayer-responses-sse"` that delegates to Pi's `openai-responses` implementation with Mixlayer payload sanitization
- `responses-websocket` / `responses-websocket-delta` → custom API with a placeholder `streamSimple` (to be implemented in Phases 3 and 4).

### Phase 2: Responses over HTTP/SSE

- For models resolved to `responses-sse`, register them with:
  ```ts
  api: "openai-responses"
  ```
- Set `OpenAIResponsesCompat` appropriately:
  ```ts
  compat: {
    supportsDeveloperRole: false, // or true once confirmed
    sendSessionIdHeader: true,    // or false once confirmed
    supportsLongCacheRetention: true,
  }
  ```
- Verify that Pi's built-in `openai-responses` provider routes requests to `https://models.mixlayer.ai/v1` and calls `client.responses.create(...)`.
- Test streaming text, tool calls, and reasoning if supported.

**Deliverable:** Mixlayer models can be used with Responses API over HTTP/SSE.

**Status:** Implemented via a custom `mixlayer-responses-sse` API alias. The handler delegates to Pi's `streamSimpleOpenAIResponses()`, preserves Pi's normal `before_provider_request` hook, then strips Mixlayer-incompatible payload fields immediately before the OpenAI SDK sends the request:
- top-level `include`
- top-level `prompt_cache_retention`
- `reasoning.summary`
- `reasoning.generate_summary`

Live testing with the Mixlayer endpoint also showed that `session_id` request headers return HTTP 400, while `x-client-request-id` is accepted. Responses transports therefore set `compat.sendSessionIdHeader: false`.

### Phase 3: Responses over WebSocket

- Implement a custom `streamSimple` handler in `pi.registerProvider()` for models using `responses-websocket`.
- The handler should:
  - Open a WebSocket connection to `wss://models.mixlayer.ai/v1/responses`.
  - Send a `response.create` message with the full request body.
  - Parse incoming WebSocket events and forward them through Pi's `AssistantMessageEventStream` protocol.
  - Handle connection errors, abort signals, and close events.
- Reuse Pi's event types where possible (`text_start/delta/end`, `thinking_start/delta/end`, `toolcall_start/delta/end`, `done`, `error`).
- Consider whether to use the `openai` SDK's WebSocket helpers or raw `WebSocket`.
- **Do not use `previous_response_id` in this phase.** Send full context on every turn.

**Deliverable:** Mixlayer models can stream Responses over WebSocket with full context per turn.

**Status:** Implemented for `responses-websocket` via a custom `streamSimple` handler. The handler:
- Builds a full Responses request body from Pi context on every turn.
- Opens `wss://models.mixlayer.ai/v1/responses`.
- Sends `{ type: "response.create", ...payload }`.
- Omits the HTTP/SSE-only `stream` request field, which Mixlayer rejects on WebSocket requests.
- Applies the same Mixlayer payload sanitizer used by `responses-sse`.
- Parses Responses WebSocket events into Pi's `AssistantMessageEventStream` protocol.
- Handles connection errors, aborts, early closes, and nested provider error frames.

Live testing confirmed `responses-websocket` returns the expected output for a basic Pi print-mode request. It does not use `previous_response_id`; delta support remains Phase 4.

### Phase 4: Delta Updates over WebSocket

- Enable once the server supports `previous_response_id` and delta updates.
- Extend the Phase 3 WebSocket handler to support `responses-websocket-delta`.
- Cache WebSocket connections per `sessionId`.
- On each turn, compute the delta between:
  - the current full `input`, and
  - the cached previous request body plus the assistant's response items from the previous turn.
- If the delta can be computed and `previous_response_id` is available, send:
  ```ts
  {
    type: "response.create",
    previous_response_id: "...",
    input: delta,
    // ...other unchanged params
  }
  ```
- Otherwise, fall back to sending the full context.
- Store the assistant's response items after each successful turn for use in the next delta computation.
- Add session cleanup (close idle connections, clear cache on shutdown).

**Deliverable:** Mixlayer models can stream Responses over WebSocket with delta updates, reducing per-turn payload size.

### Phase 5: Polish and Defaults

- Add tests or manual test scripts for each transport.
- Update `README.md` and `AGENTS.md` with:
  - how to select a transport,
  - which models support Responses API,
  - environment variables,
  - known limitations.
- Decide on a default transport for new installations once Responses API support is stable.
- Consider exposing transport selection via a Pi command or setting.

**Deliverable:** Responses API support is documented, tested, and ready for general use.

## Open Questions

- Does Mixlayer's Responses API use the same base URL (`https://models.mixlayer.ai/v1`) or a different one for WebSocket?
  - **Same base URL confirmed for HTTP/SSE and WebSocket.**
- Does Mixlayer require any beta headers (e.g. `OpenAI-Beta: responses_websockets=...`) for WebSocket access?
  - **No beta headers required.**
- Does Mixlayer support `previous_response_id` for delta updates?
  - **Not yet.** Will be enabled in Phase 4.
- How should the extension discover which models support Responses API?
  - **All Mixlayer models support it.** No per-model discovery needed.
- Should the extension register one provider (`mixlayer`) with mixed transports, or separate providers (`mixlayer`, `mixlayer-responses`)?
