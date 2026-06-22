# pi Mixlayer Extension

Registers Mixlayer as a pi provider using Mixlayer's OpenAI-compatible APIs.

## Usage

```bash
pi install npm:pi-mixlayer
pi
```

Inside pi, run:

```text
/login
```

Select **Use an API key**, then choose **Mixlayer** from the provider list. Enter your Mixlayer API key when prompted. Pi stores the credential in `~/.pi/agent/auth.json`.

The extension fetches the current model list from:

```text
https://models.mixlayer.ai/_openrouter/models
```

It registers the models under the `mixlayer` provider with `baseUrl: "https://models.mixlayer.ai/v1"`. By default, models use Pi's OpenAI-compatible Chat Completions transport.

The model list is cached in `~/.pi/agent/mixlayer-models-cache.json` for one hour to avoid repeated API requests. If the cache has expired and the network request fails, the extension falls back to the stale cache so pi still starts with available models.

Run `pi --list-models` to verify the registered models after installation.

When Mixlayer auth is configured and no model is selected yet, the extension selects `qwen/qwen3.5-397b-a17b` as the preferred default if that model is present in the remote model list.

## Transport Selection

The extension supports configurable transport selection for the `mixlayer` provider:

| Transport | Status | Behavior |
| --- | --- | --- |
| `chat-completions` | Default | Uses Pi's `openai-completions` transport. |
| `responses-sse` | Supported | Uses Mixlayer's Responses API over HTTP/SSE through a Mixlayer-specific wrapper around Pi's Responses transport. |
| `responses-websocket` | Supported | Uses Mixlayer's Responses API over WebSocket with full context sent on every turn. |
| `responses-websocket-delta` | Supported | Uses Mixlayer's Responses API over WebSocket with session-cached `previous_response_id` deltas when safe, falling back to full context otherwise. |

Transport resolution order is:

1. `MIXLAYER_TRANSPORT` environment variable
2. `~/.pi/agent/mixlayer-settings.json`, written by `/mixlayer-transport`
3. `chat-completions`

For a one-off run:

```bash
MIXLAYER_TRANSPORT=responses-sse pi
```

For a persistent setting inside Pi:

```text
/mixlayer-transport responses-sse
```

Then reload extensions or restart Pi for the new transport to take effect. Run `/mixlayer-transport` with no argument to show the current configured transport.

The Responses transports strip Mixlayer-incompatible request fields before sending and disable Pi's `session_id` request header, which Mixlayer rejects. They still send `x-client-request-id` for request affinity. The `responses-websocket` transport sends the full conversation context each turn. The `responses-websocket-delta` transport first builds the full request, then sends a delta only when the current request is an exact append-only continuation of the cached previous request and response.

## Stats Counters

Run this inside Pi to print in-memory Mixlayer counters for the current extension process:

```text
/mixlayer-stats
```

The output includes Mixlayer turn counts, logical provider request counts, HTTP response status counts, and WebSocket full/delta request attempts. `delta_requests` and `delta_hit_rate` show whether the WebSocket delta path is being used.

Reset counters with:

```text
/mixlayer-stats reset
```

## Debug Logging

Set `MIXLAYER_DEBUG_LOGS=1` to write provider request/response debug logs to:

- `/tmp/mixlayer-debug.log`
- `/tmp/mixlayer-response.log`

Logging is disabled by default.
