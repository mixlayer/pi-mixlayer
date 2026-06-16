# pi Mixlayer Extension

Registers Mixlayer as a pi provider using Mixlayer's OpenAI-compatible Chat Completions API.

## Usage

```bash
pi install npm:pi-mixlayer
pi
```

Inside pi, run:

```text
/login mixlayer
```

Enter your Mixlayer API key when prompted. Pi stores the credential in `~/.pi/agent/auth.json`.

The extension fetches the current model list from:

```text
https://models.mixlayer.ai/_openrouter/models
```

It registers the models under the `mixlayer` provider with `api: "openai-completions"` and `baseUrl: "https://models.mixlayer.ai/v1"`.

The model list is cached in `~/.pi/agent/mixlayer-models-cache.json` for one hour to avoid repeated API requests. If the cache has expired and the network request fails, the extension falls back to the stale cache so pi still starts with available models.

Run `pi --list-models` to verify the registered models after installation.

When Mixlayer auth is configured and no model is selected yet, the extension selects `qwen/qwen3.5-397b-a17b` as the preferred default if that model is present in the remote model list.
