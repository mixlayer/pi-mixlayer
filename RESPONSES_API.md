# Responses API Support Plan

This document outlines the phased plan for adding OpenAI Responses API support to `pi-mixlayer`, with the eventual goal of supporting Responses over WebSocket with delta updates. The extension's underlying transport should be configurable so we can swap between Chat Completions, Responses HTTP/SSE, and Responses WebSocket.

## Current State

- The extension registers all Mixlayer models with `api: "openai-completions"`.
- Pi has a built-in `openai-responses` API provider that handles Responses API over HTTP/SSE.
- Pi has a built-in `openai-codex-responses` API provider that handles Responses over WebSocket with delta updates, but that implementation is Codex-specific and not exposed as a reusable public API type.
- There is no public Pi API type such as `"openai-responses-websocket"` that an extension can opt into.

## Goal

- Support Responses API for Mixlayer models.
- Support Responses over WebSocket with delta updates (reusing `previous_response_id` and connection caching).
- Make the transport configurable per model or globally.

## Transport Options

The extension should support at least these transports:

1. `chat-completions` — current behavior, `api: "openai-completions"`.
2. `responses-sse` — Responses API over HTTP/SSE via Pi's built-in `openai-responses` provider.
3. `responses-websocket` — Responses API over WebSocket with full context per turn.
4. `responses-websocket-delta` — Responses API over WebSocket with delta updates and connection caching.

## Phases

### Phase 0: Discovery

- Confirm whether Mixlayer exposes Responses API support in the model list from `https://models.mixlayer.ai/_openrouter/models`.
- Identify any Mixlayer-specific headers, beta headers, or WebSocket endpoint requirements.
- Determine whether Mixlayer supports `previous_response_id` for delta updates over WebSocket.
- Decide how the extension will know which models support Responses API:
  - A new field in the Mixlayer model list (preferred).
  - A hardcoded set of model IDs in the extension.
  - A separate provider registration (e.g. `mixlayer-responses`).

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

### Phase 3: Responses over WebSocket

- Implement a custom `streamSimple` handler in `pi.registerProvider()` for models using `responses-websocket`.
- The handler should:
  - Open a WebSocket connection to the Mixlayer Responses WebSocket endpoint.
  - Send a `response.create` message with the full request body.
  - Parse incoming WebSocket events and forward them through Pi's `AssistantMessageEventStream` protocol.
  - Handle connection errors, abort signals, and close events.
- Reuse Pi's event types where possible (`text_start/delta/end`, `thinking_start/delta/end`, `toolcall_start/delta/end`, `done`, `error`).
- Consider whether to use the `openai` SDK's WebSocket helpers or raw `WebSocket`.

**Deliverable:** Mixlayer models can stream Responses over WebSocket with full context per turn.

### Phase 4: Delta Updates over WebSocket

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
- Does Mixlayer require any beta headers (e.g. `OpenAI-Beta: responses_websockets=...`) for WebSocket access?
- Does Mixlayer support `previous_response_id` for delta updates?
- How should the extension discover which models support Responses API?
- Should the extension register one provider (`mixlayer`) with mixed transports, or separate providers (`mixlayer`, `mixlayer-responses`)?
