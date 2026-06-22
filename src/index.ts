import type { ExtensionAPI, ProviderModelConfig } from "@earendil-works/pi-coding-agent";
import {
	createAssistantMessageEventStream,
	type Api,
	type AssistantMessage,
	type AssistantMessageEventStream,
	type Context,
	type Model,
	type SimpleStreamOptions,
	streamSimpleOpenAIResponses,
} from "@earendil-works/pi-ai";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { createResponsesWebSocketStreamSimple, type ResponsesWebSocketStatsEvent } from "./responses-websocket.js";

const MIXLAYER_PROVIDER_ID = "mixlayer";
const MIXLAYER_PROVIDER_NAME = "Mixlayer";
const MIXLAYER_BASE_URL = "https://models.mixlayer.ai/v1";
const MIXLAYER_MODELS_URL = "https://models.mixlayer.ai/_openrouter/models";
const MIXLAYER_RESPONSES_SSE_API = "mixlayer-responses-sse";
const MIXLAYER_RESPONSES_WEBSOCKET_API = "mixlayer-responses-websocket";
const MIXLAYER_RESPONSES_WEBSOCKET_DELTA_API = "mixlayer-responses-websocket-delta";
const MIXLAYER_DEBUG_LOGS_ENV = "MIXLAYER_DEBUG_LOGS";
const MIXLAYER_REQUEST_LOG_PATH = "/tmp/mixlayer-debug.log";
const MIXLAYER_RESPONSE_LOG_PATH = "/tmp/mixlayer-response.log";
const MIXLAYER_DEFAULT_MODEL_IDS = ["qwen/qwen3.5-397b-a17b", "qwen/qwen3.6-35b-a3b"];
const DEFAULT_CONTEXT_WINDOW = 128000;
const DEFAULT_MAX_TOKENS = 4096;
const MODELS_FETCH_TIMEOUT_MS = 10_000;
const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour

const MIXLAYER_TRANSPORTS = [
	"chat-completions",
	"responses-sse",
	"responses-websocket",
	"responses-websocket-delta",
] as const;

type MixlayerTransport = (typeof MIXLAYER_TRANSPORTS)[number];

interface CachedModels {
	fetchedAt: string;
	models: MixlayerModel[];
}

interface MixlayerModelsResponse {
	data: MixlayerModel[];
}

interface MixlayerModel {
	id: string;
	name?: string;
	input_modalities?: string[];
	context_length?: number;
	max_output_length?: number;
	pricing?: {
		prompt?: string;
		completion?: string;
		input_cache_read?: string;
		input_cache_write?: string;
	};
	supported_features?: string[];
}

interface PiSettings {
	mixlayer?: {
		transport?: MixlayerTransport;
	};
}

interface MixlayerStats {
	startedAt: string;
	providerRequests: {
		total: number;
		byApi: Record<string, number>;
		byModel: Record<string, number>;
		httpResponses: number;
		httpResponseErrors: number;
		httpStatuses: Record<string, number>;
	};
	turns: {
		started: number;
		ended: number;
		completed: number;
		errored: number;
		aborted: number;
		toolUse: number;
		byModel: Record<string, number>;
	};
	websocket: {
		streams: number;
		deltaEnabledStreams: number;
		cacheableStreams: number;
		deltaDisabledStreams: number;
		requests: number;
		successfulRequests: number;
		failedRequests: number;
		fullRequests: number;
		deltaRequests: number;
		retryRequests: number;
		deltaFailures: number;
		deltaRecoverableFailures: number;
		deltaDisabled: number;
	};
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
}

function createMixlayerStats(): MixlayerStats {
	return {
		startedAt: new Date().toISOString(),
		providerRequests: {
			total: 0,
			byApi: {},
			byModel: {},
			httpResponses: 0,
			httpResponseErrors: 0,
			httpStatuses: {},
		},
		turns: {
			started: 0,
			ended: 0,
			completed: 0,
			errored: 0,
			aborted: 0,
			toolUse: 0,
			byModel: {},
		},
		websocket: {
			streams: 0,
			deltaEnabledStreams: 0,
			cacheableStreams: 0,
			deltaDisabledStreams: 0,
			requests: 0,
			successfulRequests: 0,
			failedRequests: 0,
			fullRequests: 0,
			deltaRequests: 0,
			retryRequests: 0,
			deltaFailures: 0,
			deltaRecoverableFailures: 0,
			deltaDisabled: 0,
		},
	};
}

function resetMixlayerStats(stats: MixlayerStats): void {
	Object.assign(stats, createMixlayerStats());
}

function incrementCounter(map: Record<string, number>, key: string | undefined): void {
	map[key && key.length > 0 ? key : "unknown"] = (map[key && key.length > 0 ? key : "unknown"] ?? 0) + 1;
}

function formatCounterMap(map: Record<string, number>): string {
	const entries = Object.entries(map).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
	if (entries.length === 0) {
		return "(none)";
	}
	return entries.map(([key, value]) => `${key}=${value}`).join(", ");
}

function formatPercent(numerator: number, denominator: number): string {
	if (denominator <= 0) {
		return "0.0%";
	}
	return `${((numerator / denominator) * 100).toFixed(1)}%`;
}

function formatMixlayerStats(stats: MixlayerStats, transport: MixlayerTransport): string {
	const websocket = stats.websocket;
	return [
		"Mixlayer stats",
		`Since: ${stats.startedAt}`,
		`Transport: ${transport}`,
		"",
		"Turns:",
		`  started: ${stats.turns.started}`,
		`  ended: ${stats.turns.ended}`,
		`  completed: ${stats.turns.completed}`,
		`  tool_use: ${stats.turns.toolUse}`,
		`  errored: ${stats.turns.errored}`,
		`  aborted: ${stats.turns.aborted}`,
		`  by_model: ${formatCounterMap(stats.turns.byModel)}`,
		"",
		"Provider requests:",
		`  total: ${stats.providerRequests.total}`,
		`  by_api: ${formatCounterMap(stats.providerRequests.byApi)}`,
		`  by_model: ${formatCounterMap(stats.providerRequests.byModel)}`,
		`  http_responses: ${stats.providerRequests.httpResponses}`,
		`  http_response_errors: ${stats.providerRequests.httpResponseErrors}`,
		`  http_statuses: ${formatCounterMap(stats.providerRequests.httpStatuses)}`,
		"",
		"WebSocket:",
		`  streams: ${websocket.streams}`,
		`  delta_enabled_streams: ${websocket.deltaEnabledStreams}`,
		`  cacheable_streams: ${websocket.cacheableStreams}`,
		`  delta_disabled_streams: ${websocket.deltaDisabledStreams}`,
		`  requests: ${websocket.requests}`,
		`  successful_requests: ${websocket.successfulRequests}`,
		`  failed_requests: ${websocket.failedRequests}`,
		`  full_requests: ${websocket.fullRequests}`,
		`  delta_requests: ${websocket.deltaRequests}`,
		`  delta_hit_rate: ${formatPercent(websocket.deltaRequests, websocket.requests)}`,
		`  retry_requests: ${websocket.retryRequests}`,
		`  delta_failures: ${websocket.deltaFailures}`,
		`  delta_recoverable_failures: ${websocket.deltaRecoverableFailures}`,
		`  delta_disabled: ${websocket.deltaDisabled}`,
	].join("\n");
}

function recordWebSocketStats(stats: MixlayerStats, event: ResponsesWebSocketStatsEvent): void {
	switch (event.type) {
		case "stream_started":
			stats.websocket.streams++;
			if (event.deltaEnabled) {
				stats.websocket.deltaEnabledStreams++;
			}
			if (event.cacheable) {
				stats.websocket.cacheableStreams++;
			}
			if (event.deltaDisabled) {
				stats.websocket.deltaDisabledStreams++;
			}
			break;
		case "request_started":
			stats.websocket.requests++;
			if (event.requestKind === "delta") {
				stats.websocket.deltaRequests++;
			} else {
				stats.websocket.fullRequests++;
			}
			if (event.retry) {
				stats.websocket.retryRequests++;
			}
			break;
		case "request_finished":
			if (event.ok) {
				stats.websocket.successfulRequests++;
			} else {
				stats.websocket.failedRequests++;
				if (event.requestKind === "delta") {
					stats.websocket.deltaFailures++;
					if (event.recoverable) {
						stats.websocket.deltaRecoverableFailures++;
					}
				}
			}
			break;
		case "delta_disabled":
			stats.websocket.deltaDisabled++;
			break;
	}
}

function getStopReason(message: unknown): string | undefined {
	if (!isRecord(message)) {
		return undefined;
	}
	return typeof message.stopReason === "string" ? message.stopReason : undefined;
}

function parseNumber(value: unknown): number | undefined {
	if (typeof value === "number" && Number.isFinite(value)) {
		return value;
	}
	if (typeof value === "string") {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : undefined;
	}
	return undefined;
}

function parseStringArray(value: unknown): string[] | undefined {
	if (!Array.isArray(value)) {
		return undefined;
	}
	return value.filter((item): item is string => typeof item === "string");
}

function parsePricing(value: unknown): MixlayerModel["pricing"] {
	if (!isRecord(value)) {
		return undefined;
	}
	return {
		prompt: typeof value.prompt === "string" ? value.prompt : undefined,
		completion: typeof value.completion === "string" ? value.completion : undefined,
		input_cache_read: typeof value.input_cache_read === "string" ? value.input_cache_read : undefined,
		input_cache_write: typeof value.input_cache_write === "string" ? value.input_cache_write : undefined,
	};
}

function parseModel(value: unknown): MixlayerModel | undefined {
	if (!isRecord(value) || typeof value.id !== "string") {
		return undefined;
	}

	return {
		id: value.id,
		name: typeof value.name === "string" ? value.name : undefined,
		input_modalities: parseStringArray(value.input_modalities),
		context_length: parseNumber(value.context_length),
		max_output_length: parseNumber(value.max_output_length),
		pricing: parsePricing(value.pricing),
		supported_features: parseStringArray(value.supported_features),
	};
}

function parseModelsResponse(payload: unknown): MixlayerModelsResponse {
	if (!isRecord(payload) || !Array.isArray(payload.data)) {
		throw new Error("Mixlayer models response did not contain a data array.");
	}

	const models = payload.data.map(parseModel).filter((model): model is MixlayerModel => model !== undefined);
	if (models.length === 0) {
		throw new Error("Mixlayer models response did not contain any valid models.");
	}

	return { data: models };
}

function getAgentDir(): string {
	return join(homedir(), ".pi", "agent");
}

function getCachePath(): string {
	return join(getAgentDir(), "mixlayer-models-cache.json");
}

function getSettingsPath(): string {
	return join(getAgentDir(), "mixlayer-settings.json");
}

async function readCache(skipTTL = false): Promise<MixlayerModel[] | undefined> {
	try {
		const raw = await readFile(getCachePath(), "utf8");
		const parsed: unknown = JSON.parse(raw);
		if (!isRecord(parsed) || typeof parsed.fetchedAt !== "string" || !Array.isArray(parsed.models)) {
			return undefined;
		}

		const fetchedAt = new Date(parsed.fetchedAt).getTime();
		if (!skipTTL && (Number.isNaN(fetchedAt) || Date.now() - fetchedAt > CACHE_TTL_MS)) {
			return undefined;
		}

		return parsed.models.map(parseModel).filter((model): model is MixlayerModel => model !== undefined);
	} catch {
		return undefined;
	}
}

async function writeCache(models: MixlayerModel[]): Promise<void> {
	try {
		const cachePath = getCachePath();
		await mkdir(join(cachePath, ".."), { recursive: true });
		const cache: CachedModels = { fetchedAt: new Date().toISOString(), models };
		await writeFile(cachePath, JSON.stringify(cache), "utf8");
	} catch {
		// Cache write failures are non-fatal.
	}
}

async function fetchModels(): Promise<MixlayerModel[]> {
	const controller = new AbortController();
	const timeout = setTimeout(() => {
		controller.abort();
	}, MODELS_FETCH_TIMEOUT_MS);
	if (typeof timeout === "object" && "unref" in timeout && typeof timeout.unref === "function") {
		timeout.unref();
	}

	try {
		const response = await fetch(MIXLAYER_MODELS_URL, { signal: controller.signal });
		if (!response.ok) {
			throw new Error(`Failed to fetch Mixlayer models: ${response.status} ${response.statusText}`);
		}

		const payload = parseModelsResponse(await response.json());
		return payload.data;
	} catch (error) {
		if (controller.signal.aborted) {
			throw new Error(`Timed out fetching Mixlayer models after ${MODELS_FETCH_TIMEOUT_MS}ms`);
		}
		throw error;
	} finally {
		clearTimeout(timeout);
	}
}

async function fetchModelsWithCache(): Promise<MixlayerModel[]> {
	const cached = await readCache();
	if (cached && cached.length > 0) {
		return cached;
	}

	try {
		const models = await fetchModels();
		await writeCache(models);
		return models;
	} catch (error) {
		const stale = await readCache(true);
		if (stale && stale.length > 0) {
			return stale;
		}
		throw error;
	}
}

async function readSettings(): Promise<PiSettings> {
	try {
		const raw = await readFile(getSettingsPath(), "utf8");
		const parsed: unknown = JSON.parse(raw);
		if (!isRecord(parsed)) {
			return {};
		}

		const mixlayer = isRecord(parsed.mixlayer) ? parsed.mixlayer : {};
		const transport = parseTransport(mixlayer.transport);
		return { mixlayer: { transport } };
	} catch {
		return {};
	}
}

async function writeSettings(settings: PiSettings): Promise<void> {
	const settingsPath = getSettingsPath();
	let parsed: Record<string, unknown> = {};
	try {
		const raw = await readFile(settingsPath, "utf8");
		const existing: unknown = JSON.parse(raw);
		if (isRecord(existing)) {
			parsed = existing;
		}
	} catch {
		// If the file doesn't exist or is invalid, start fresh.
	}

	if (settings.mixlayer?.transport) {
		parsed.mixlayer = { ...(isRecord(parsed.mixlayer) ? parsed.mixlayer : {}), transport: settings.mixlayer.transport };
	} else if (settings.mixlayer) {
		parsed.mixlayer = { ...(isRecord(parsed.mixlayer) ? parsed.mixlayer : {}) };
		delete (parsed.mixlayer as Record<string, unknown>).transport;
	}

	await mkdir(join(settingsPath, ".."), { recursive: true });
	await writeFile(settingsPath, `${JSON.stringify(parsed, null, 2)}\n`, "utf8");
}

function parseTransport(value: unknown): MixlayerTransport | undefined {
	if (typeof value !== "string") {
		return undefined;
	}
	return MIXLAYER_TRANSPORTS.includes(value as MixlayerTransport) ? (value as MixlayerTransport) : undefined;
}

function resolveTransport(settings: PiSettings): MixlayerTransport {
	const envTransport = parseTransport(process.env.MIXLAYER_TRANSPORT);
	if (envTransport) {
		return envTransport;
	}

	if (settings.mixlayer?.transport) {
		return settings.mixlayer.transport;
	}

	return "chat-completions";
}

function isDebugLoggingEnabled(): boolean {
	const value = process.env[MIXLAYER_DEBUG_LOGS_ENV]?.toLowerCase();
	return value === "1" || value === "true" || value === "yes";
}

function pricePerMillion(value: string | undefined): number {
	const parsed = parseNumber(value);
	return parsed === undefined ? 0 : parsed * 1_000_000;
}

function mapInputModalities(inputModalities: string[] | undefined): ("text" | "image")[] {
	const input = new Set<"text" | "image">();
	for (const modality of inputModalities ?? []) {
		if (modality === "text" || modality === "image") {
			input.add(modality);
		}
	}
	if (input.size === 0) {
		input.add("text");
	}
	return Array.from(input);
}

function toProviderApi(transport: MixlayerTransport): Api {
	switch (transport) {
		case "chat-completions":
			return "openai-completions";
		case "responses-sse":
			return MIXLAYER_RESPONSES_SSE_API;
		case "responses-websocket":
			return MIXLAYER_RESPONSES_WEBSOCKET_API;
		case "responses-websocket-delta":
			return MIXLAYER_RESPONSES_WEBSOCKET_DELTA_API;
	}
}

function toProviderModel(model: MixlayerModel, transport: MixlayerTransport): ProviderModelConfig {
	const base = {
		id: model.id,
		name: model.name ?? model.id,
		reasoning: model.supported_features?.includes("reasoning") ?? false,
		input: mapInputModalities(model.input_modalities),
		cost: {
			input: pricePerMillion(model.pricing?.prompt),
			output: pricePerMillion(model.pricing?.completion),
			cacheRead: pricePerMillion(model.pricing?.input_cache_read),
			cacheWrite: pricePerMillion(model.pricing?.input_cache_write),
		},
		contextWindow: model.context_length ?? DEFAULT_CONTEXT_WINDOW,
		maxTokens: model.max_output_length ?? DEFAULT_MAX_TOKENS,
	};

	if (transport === "chat-completions") {
		return {
			...base,
			compat: {
				supportsDeveloperRole: false,
				supportsUsageInStreaming: true,
				maxTokensField: "max_tokens",
			},
		};
	}

	return {
		...base,
		compat: {
			supportsDeveloperRole: false,
			// Mixlayer rejects the underscore-containing `session_id` header.
			sendSessionIdHeader: false,
			// Mixlayer rejects `prompt_cache_retention` as an unknown parameter.
			supportsLongCacheRetention: false,
		},
	};
}

function sanitizeMixlayerResponsesPayload(payload: unknown): unknown {
	if (!isRecord(payload)) {
		return payload;
	}

	const sanitized: Record<string, unknown> = {};
	for (const [key, value] of Object.entries(payload)) {
		if (key === "include") {
			// Mixlayer rejects non-empty `include` arrays.
			continue;
		}
		if (key === "prompt_cache_retention") {
			// Mixlayer rejects `prompt_cache_retention` as an unknown parameter.
			continue;
		}
		if (key === "reasoning" && isRecord(value)) {
			// Mixlayer rejects `reasoning.summary` and `reasoning.generate_summary`.
			const { summary: _summary, generate_summary: _generateSummary, ...rest } = value;
			if (Object.keys(rest).length > 0) {
				sanitized[key] = rest;
			}
			continue;
		}
		sanitized[key] = value;
	}

	return sanitized;
}

function isMixlayerResponsesApi(api: Api | undefined): boolean {
	return (
		api === MIXLAYER_RESPONSES_SSE_API ||
		api === MIXLAYER_RESPONSES_WEBSOCKET_API ||
		api === MIXLAYER_RESPONSES_WEBSOCKET_DELTA_API ||
		api === "openai-responses"
	);
}

function selectDefaultModel(models: ProviderModelConfig[]): ProviderModelConfig {
	for (const modelId of MIXLAYER_DEFAULT_MODEL_IDS) {
		const model = models.find((candidate) => candidate.id === modelId);
		if (model) {
			return model;
		}
	}
	return models[0];
}

function createResponsesSseStreamSimple() {
	return (model: Model<Api>, context: Context, options?: SimpleStreamOptions): AssistantMessageEventStream => {
		return streamSimpleOpenAIResponses(model as Model<"openai-responses">, context, {
			...options,
			async onPayload(payload, payloadModel) {
				const nextPayload = await options?.onPayload?.(payload, payloadModel);
				return sanitizeMixlayerResponsesPayload(nextPayload ?? payload);
			},
		});
	};
}

function createNotImplementedStreamSimple(transport: MixlayerTransport) {
	return (_model: Model<Api>, _context: Context, _options?: SimpleStreamOptions): AssistantMessageEventStream => {
		const stream = createAssistantMessageEventStream();
		(async () => {
			const output: AssistantMessage = {
				role: "assistant",
				content: [],
				api: _model.api,
				provider: _model.provider,
				model: _model.id,
				usage: {
					input: 0,
					output: 0,
					cacheRead: 0,
					cacheWrite: 0,
					totalTokens: 0,
					cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
				},
				stopReason: "error",
				timestamp: Date.now(),
				errorMessage: `Mixlayer transport "${transport}" is not implemented yet.`,
			};
			stream.push({ type: "error", reason: "error", error: output });
			stream.end();
		})();
		return stream;
	};
}

export default async function mixlayerExtension(pi: ExtensionAPI): Promise<void> {
	const stats = createMixlayerStats();
	const onWebSocketStats = (event: ResponsesWebSocketStatsEvent) => {
		recordWebSocketStats(stats, event);
	};
	const mixlayerModels = await fetchModelsWithCache();

	const settings = await readSettings();
	const transport = resolveTransport(settings);

	const models = mixlayerModels.map((model) => toProviderModel(model, transport));
	const defaultModel = selectDefaultModel(models);

	const providerConfig: Parameters<ExtensionAPI["registerProvider"]>[1] = {
		name: MIXLAYER_PROVIDER_NAME,
		baseUrl: MIXLAYER_BASE_URL,
		apiKey: "$MIXLAYER_API_KEY",
		api: toProviderApi(transport),
		models,
	};

	if (transport === "responses-sse") {
		providerConfig.streamSimple = createResponsesSseStreamSimple();
	} else if (transport === "responses-websocket") {
		providerConfig.streamSimple = createResponsesWebSocketStreamSimple(sanitizeMixlayerResponsesPayload, { onStats: onWebSocketStats });
	} else if (transport === "responses-websocket-delta") {
		providerConfig.streamSimple = createResponsesWebSocketStreamSimple(sanitizeMixlayerResponsesPayload, {
			delta: true,
			onStats: onWebSocketStats,
		});
	}

	pi.registerProvider(MIXLAYER_PROVIDER_ID, providerConfig);

	pi.on("before_provider_request", (_event, ctx) => {
		if (ctx.model?.provider !== MIXLAYER_PROVIDER_ID) {
			return;
		}

		stats.providerRequests.total++;
		incrementCounter(stats.providerRequests.byApi, ctx.model.api);
		incrementCounter(stats.providerRequests.byModel, ctx.model.id);

		if (isMixlayerResponsesApi(ctx.model?.api)) {
			const sanitized = sanitizeMixlayerResponsesPayload(_event.payload);
			if (isDebugLoggingEnabled()) {
				void writeFile(
					MIXLAYER_REQUEST_LOG_PATH,
					JSON.stringify({ original: _event.payload, sanitized }, null, 2),
					"utf8",
				).catch(() => {});
			}
			return sanitized;
		}

		return;
	});

	pi.on("after_provider_response", (_event, ctx) => {
		if (ctx.model?.provider !== MIXLAYER_PROVIDER_ID) {
			return;
		}
		stats.providerRequests.httpResponses++;
		incrementCounter(stats.providerRequests.httpStatuses, String(_event.status));
		if (_event.status >= 400) {
			stats.providerRequests.httpResponseErrors++;
		}
		if (!isDebugLoggingEnabled()) {
			return;
		}
		void writeFile(
			MIXLAYER_RESPONSE_LOG_PATH,
			JSON.stringify({ status: _event.status, headers: _event.headers }, null, 2),
			"utf8",
		).catch(() => {});
	});

	pi.on("turn_start", (_event, ctx) => {
		if (ctx.model?.provider !== MIXLAYER_PROVIDER_ID) {
			return;
		}
		stats.turns.started++;
		incrementCounter(stats.turns.byModel, ctx.model.id);
	});

	pi.on("turn_end", (_event, ctx) => {
		if (ctx.model?.provider !== MIXLAYER_PROVIDER_ID) {
			return;
		}
		stats.turns.ended++;
		const stopReason = getStopReason(_event.message);
		if (stopReason === "error") {
			stats.turns.errored++;
		} else if (stopReason === "aborted") {
			stats.turns.aborted++;
		} else if (stopReason === "toolUse") {
			stats.turns.toolUse++;
		} else {
			stats.turns.completed++;
		}
	});

	pi.registerCommand("mixlayer-transport", {
		description: "Set the Mixlayer transport (chat-completions, responses-sse, responses-websocket, responses-websocket-delta).",
		getArgumentCompletions(prefix) {
			return MIXLAYER_TRANSPORTS.filter((t) => t.startsWith(prefix)).map((value) => ({ value, label: value, description: value }));
		},
		async handler(args, ctx) {
			const trimmed = args.trim();
			if (!trimmed) {
				const current = resolveTransport(await readSettings());
				ctx.ui.notify(`Current Mixlayer transport: ${current}`, "info");
				return;
			}

			const transport = parseTransport(trimmed);
			if (!transport) {
				ctx.ui.notify(`Invalid Mixlayer transport: ${trimmed}. Valid options: ${MIXLAYER_TRANSPORTS.join(", ")}`, "error");
				return;
			}

			await writeSettings({ mixlayer: { transport } });
			ctx.ui.notify(`Mixlayer transport set to ${transport}. Reload extensions to apply.`, "info");
		},
	});

	pi.registerCommand("mixlayer-stats", {
		description: "Show Mixlayer request, turn, and WebSocket delta counters. Use /mixlayer-stats reset to clear.",
		getArgumentCompletions(prefix) {
			return "reset".startsWith(prefix) ? [{ value: "reset", label: "reset", description: "Clear Mixlayer stats counters" }] : [];
		},
		async handler(args, ctx) {
			const trimmed = args.trim();
			if (trimmed === "reset") {
				resetMixlayerStats(stats);
				ctx.ui.notify("Mixlayer stats reset.", "info");
				return;
			}
			if (trimmed) {
				ctx.ui.notify("Usage: /mixlayer-stats [reset]", "error");
				return;
			}

			ctx.ui.notify(formatMixlayerStats(stats, transport), "info");
		},
	});

	pi.on("session_start", async (_event, ctx) => {
		if (ctx.model) {
			return;
		}

		const model = ctx.modelRegistry.find(MIXLAYER_PROVIDER_ID, defaultModel.id);
		if (model) {
			await pi.setModel(model);
		}
	});
}
