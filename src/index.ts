import type { ExtensionAPI, ProviderModelConfig } from "@earendil-works/pi-coding-agent";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";

const MIXLAYER_PROVIDER_ID = "mixlayer";
const MIXLAYER_PROVIDER_NAME = "Mixlayer";
const MIXLAYER_BASE_URL = "https://models.mixlayer.ai/v1";
const MIXLAYER_MODELS_URL = "https://models.mixlayer.ai/_openrouter/models";
const MIXLAYER_DEFAULT_MODEL_IDS = ["qwen/qwen3.5-397b-a17b", "qwen/qwen3.6-35b-a3b"];
const DEFAULT_CONTEXT_WINDOW = 128000;
const DEFAULT_MAX_TOKENS = 4096;
const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour

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

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null;
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

function getCachePath(): string {
	return join(homedir(), ".pi", "agent", "mixlayer-models-cache.json");
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
	const response = await fetch(MIXLAYER_MODELS_URL);
	if (!response.ok) {
		throw new Error(`Failed to fetch Mixlayer models: ${response.status} ${response.statusText}`);
	}

	const payload = parseModelsResponse(await response.json());
	return payload.data;
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

function toProviderModel(model: MixlayerModel): ProviderModelConfig {
	return {
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
		compat: {
			supportsDeveloperRole: false,
			supportsUsageInStreaming: true,
			maxTokensField: "max_tokens",
		},
	};
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

export default async function mixlayerExtension(pi: ExtensionAPI): Promise<void> {
	const mixlayerModels = await fetchModelsWithCache();
	const models = mixlayerModels.map(toProviderModel);
	const defaultModel = selectDefaultModel(models);

	pi.registerProvider(MIXLAYER_PROVIDER_ID, {
		name: MIXLAYER_PROVIDER_NAME,
		baseUrl: MIXLAYER_BASE_URL,
		apiKey: "$MIXLAYER_API_KEY",
		api: "openai-completions",
		models,
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
