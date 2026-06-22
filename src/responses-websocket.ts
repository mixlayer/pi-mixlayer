import {
	calculateCost,
	clampThinkingLevel,
	createAssistantMessageEventStream,
	parseStreamingJson,
	type Api,
	type AssistantMessage,
	type AssistantMessageEventStream,
	type Context,
	type ImageContent,
	type Message,
	type Model,
	type SimpleStreamOptions,
	type TextContent,
	type Tool,
	type ToolCall,
} from "@earendil-works/pi-ai";

const OPENAI_PROMPT_CACHE_KEY_MAX_LENGTH = 64;
const DEFAULT_WEBSOCKET_CONNECT_TIMEOUT_MS = 15_000;
const SESSION_WEBSOCKET_CACHE_TTL_MS = 5 * 60 * 1000;
const WEBSOCKET_NORMAL_CLOSE_CODE = 1000;

const MIXLAYER_TOOL_CALL_PROVIDERS = new Set(["openai", "openai-codex", "opencode"]);

type ResponsesStreamEvent = Record<string, any>;
type ResponsesPayload = Record<string, unknown>;
type PayloadSanitizer = (payload: unknown) => unknown;

interface ResponsesWebSocketStreamOptions {
	delta?: boolean;
	onStats?: (event: ResponsesWebSocketStatsEvent) => void;
}

type ResponsesWebSocketRequestKind = "full" | "delta";

export type ResponsesWebSocketStatsEvent =
	| { type: "stream_started"; deltaEnabled: boolean; cacheable: boolean; deltaDisabled: boolean; sessionId?: string }
	| { type: "request_started"; requestKind: ResponsesWebSocketRequestKind; retry: boolean }
	| { type: "request_finished"; requestKind: ResponsesWebSocketRequestKind; retry: boolean; ok: boolean; recoverable: boolean }
	| { type: "delta_disabled"; sessionId: string };

interface WebSocketLike {
	readonly readyState?: number;
	addEventListener(type: "open", listener: (event: Event) => void, options?: unknown): void;
	addEventListener(type: "message", listener: (event: MessageEvent) => void, options?: unknown): void;
	addEventListener(type: "error", listener: (event: Event) => void, options?: unknown): void;
	addEventListener(type: "close", listener: (event: CloseEvent) => void, options?: unknown): void;
	removeEventListener(type: "open", listener: (event: Event) => void): void;
	removeEventListener(type: "message", listener: (event: MessageEvent) => void): void;
	removeEventListener(type: "error", listener: (event: Event) => void): void;
	removeEventListener(type: "close", listener: (event: CloseEvent) => void): void;
	send(data: string): void;
	close(code?: number, reason?: string): void;
}

interface WebSocketConstructorLike {
	new (url: string | URL, options?: { headers?: Record<string, string> }): WebSocketLike;
}

interface WebSocketContinuation {
	lastRequestBody: ResponsesPayload;
	lastResponseId: string;
	lastResponseItems: unknown[];
}

interface WebSocketSessionEntry {
	socket: WebSocketLike;
	busy: boolean;
	idleTimer?: ReturnType<typeof setTimeout>;
	continuation?: WebSocketContinuation;
}

interface WebSocketLease {
	socket: WebSocketLike;
	entry?: WebSocketSessionEntry;
	release: (options?: { keep?: boolean }) => void;
}

const websocketSessionCache = new Map<string, WebSocketSessionEntry>();
const deltaDisabledSessions = new Set<string>();

function sanitizeSurrogates(text: string): string {
	return text.replace(/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, "");
}

function shortHash(str: string): string {
	let h1 = 0xdeadbeef;
	let h2 = 0x41c6ce57;
	for (let i = 0; i < str.length; i++) {
		const ch = str.charCodeAt(i);
		h1 = Math.imul(h1 ^ ch, 2654435761);
		h2 = Math.imul(h2 ^ ch, 1597334677);
	}
	h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
	h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
	return (h2 >>> 0).toString(36) + (h1 >>> 0).toString(36);
}

function clampOpenAIPromptCacheKey(key: string | undefined): string | undefined {
	if (key === undefined) {
		return undefined;
	}
	const chars = Array.from(key);
	if (chars.length <= OPENAI_PROMPT_CACHE_KEY_MAX_LENGTH) {
		return key;
	}
	return chars.slice(0, OPENAI_PROMPT_CACHE_KEY_MAX_LENGTH).join("");
}

function resolveCacheRetention(cacheRetention: SimpleStreamOptions["cacheRetention"]): SimpleStreamOptions["cacheRetention"] {
	if (cacheRetention) {
		return cacheRetention;
	}
	if (typeof process !== "undefined" && process.env.PI_CACHE_RETENTION === "long") {
		return "long";
	}
	return "short";
}

function normalizeIdPart(part: string): string {
	const sanitized = part.replace(/[^a-zA-Z0-9_-]/g, "_");
	const normalized = sanitized.length > 64 ? sanitized.slice(0, 64) : sanitized;
	return normalized.replace(/_+$/, "");
}

function buildForeignResponsesItemId(itemId: string): string {
	const normalized = `fc_${shortHash(itemId)}`;
	return normalized.length > 64 ? normalized.slice(0, 64) : normalized;
}

function normalizeToolCallId<TApi extends Api>(id: string, model: Model<TApi>, source: AssistantMessage): string {
	if (!MIXLAYER_TOOL_CALL_PROVIDERS.has(model.provider)) {
		return normalizeIdPart(id);
	}
	if (!id.includes("|")) {
		return normalizeIdPart(id);
	}
	const [callId, itemId] = id.split("|");
	const normalizedCallId = normalizeIdPart(callId);
	const isForeignToolCall = source.provider !== model.provider || source.api !== model.api;
	let normalizedItemId = isForeignToolCall ? buildForeignResponsesItemId(itemId) : normalizeIdPart(itemId);
	if (!normalizedItemId.startsWith("fc_")) {
		normalizedItemId = normalizeIdPart(`fc_${normalizedItemId}`);
	}
	return `${normalizedCallId}|${normalizedItemId}`;
}

function replaceImagesWithPlaceholder(content: (TextContent | ImageContent)[], placeholder: string): (TextContent | ImageContent)[] {
	const result: (TextContent | ImageContent)[] = [];
	let previousWasPlaceholder = false;
	for (const block of content) {
		if (block.type === "image") {
			if (!previousWasPlaceholder) {
				result.push({ type: "text", text: placeholder });
			}
			previousWasPlaceholder = true;
			continue;
		}
		result.push(block);
		previousWasPlaceholder = block.text === placeholder;
	}
	return result;
}

function downgradeUnsupportedImages<TApi extends Api>(messages: Message[], model: Model<TApi>): Message[] {
	if (model.input.includes("image")) {
		return messages;
	}
	return messages.map((msg) => {
		if (msg.role === "user" && Array.isArray(msg.content)) {
			return {
				...msg,
				content: replaceImagesWithPlaceholder(msg.content, "(image omitted: model does not support images)"),
			};
		}
		if (msg.role === "toolResult") {
			return {
				...msg,
				content: replaceImagesWithPlaceholder(msg.content, "(tool image omitted: model does not support images)"),
			};
		}
		return msg;
	});
}

function transformMessages<TApi extends Api>(messages: Message[], model: Model<TApi>): Message[] {
	const toolCallIdMap = new Map<string, string>();
	const imageAwareMessages = downgradeUnsupportedImages(messages, model);
	const transformed = imageAwareMessages.map((msg) => {
		if (msg.role === "user") {
			return msg;
		}
		if (msg.role === "toolResult") {
			const normalizedId = toolCallIdMap.get(msg.toolCallId);
			if (normalizedId && normalizedId !== msg.toolCallId) {
				return { ...msg, toolCallId: normalizedId };
			}
			return msg;
		}
		if (msg.role === "assistant") {
			const isSameModel = msg.provider === model.provider && msg.api === model.api && msg.model === model.id;
			const transformedContent = msg.content.flatMap((block) => {
				if (block.type === "thinking") {
					if (block.redacted) {
						return isSameModel ? block : [];
					}
					if (isSameModel && block.thinkingSignature) {
						return block;
					}
					if (!block.thinking || block.thinking.trim() === "") {
						return [];
					}
					if (isSameModel) {
						return block;
					}
					return { type: "text" as const, text: block.thinking };
				}
				if (block.type === "text") {
					if (isSameModel) {
						return block;
					}
					return { type: "text" as const, text: block.text };
				}
				if (block.type === "toolCall") {
					let normalizedToolCall = block;
					if (!isSameModel && block.thoughtSignature) {
						normalizedToolCall = { ...block };
						delete normalizedToolCall.thoughtSignature;
					}
					if (!isSameModel) {
						const normalizedId = normalizeToolCallId(block.id, model, msg);
						if (normalizedId !== block.id) {
							toolCallIdMap.set(block.id, normalizedId);
							normalizedToolCall = { ...normalizedToolCall, id: normalizedId };
						}
					}
					return normalizedToolCall;
				}
				return block;
			});
			return { ...msg, content: transformedContent };
		}
		return msg;
	});

	const result: Message[] = [];
	let pendingToolCalls: ToolCall[] = [];
	let existingToolResultIds = new Set<string>();
	const insertSyntheticToolResults = () => {
		if (pendingToolCalls.length === 0) {
			return;
		}
		for (const toolCall of pendingToolCalls) {
			if (!existingToolResultIds.has(toolCall.id)) {
				result.push({
					role: "toolResult",
					toolCallId: toolCall.id,
					toolName: toolCall.name,
					content: [{ type: "text", text: "No result provided" }],
					isError: true,
					timestamp: Date.now(),
				});
			}
		}
		pendingToolCalls = [];
		existingToolResultIds = new Set();
	};

	for (const msg of transformed) {
		if (msg.role === "assistant") {
			insertSyntheticToolResults();
			if (msg.stopReason === "error" || msg.stopReason === "aborted") {
				continue;
			}
			const toolCalls = msg.content.filter((block): block is ToolCall => block.type === "toolCall");
			if (toolCalls.length > 0) {
				pendingToolCalls = toolCalls;
				existingToolResultIds = new Set();
			}
			result.push(msg);
		} else if (msg.role === "toolResult") {
			existingToolResultIds.add(msg.toolCallId);
			result.push(msg);
		} else if (msg.role === "user") {
			insertSyntheticToolResults();
			result.push(msg);
		} else {
			result.push(msg);
		}
	}
	insertSyntheticToolResults();
	return result;
}

function encodeTextSignatureV1(id: string, phase?: "commentary" | "final_answer"): string {
	const payload: { v: 1; id: string; phase?: "commentary" | "final_answer" } = { v: 1, id };
	if (phase) {
		payload.phase = phase;
	}
	return JSON.stringify(payload);
}

function parseTextSignature(signature: string | undefined): { id: string; phase?: "commentary" | "final_answer" } | undefined {
	if (!signature) {
		return undefined;
	}
	if (signature.startsWith("{")) {
		try {
			const parsed: unknown = JSON.parse(signature);
			if (parsed && typeof parsed === "object" && "v" in parsed && "id" in parsed) {
				const candidate = parsed as { v?: unknown; id?: unknown; phase?: unknown };
				if (candidate.v === 1 && typeof candidate.id === "string") {
					if (candidate.phase === "commentary" || candidate.phase === "final_answer") {
						return { id: candidate.id, phase: candidate.phase };
					}
					return { id: candidate.id };
				}
			}
		} catch {
			// Fall through to legacy plain-string handling.
		}
	}
	return { id: signature };
}

function convertResponsesMessages<TApi extends Api>(model: Model<TApi>, context: Context): unknown[] {
	const messages: unknown[] = [];
	if (context.systemPrompt) {
		const role = model.reasoning ? "developer" : "system";
		messages.push({ role, content: sanitizeSurrogates(context.systemPrompt) });
	}

	let msgIndex = 0;
	for (const msg of transformMessages(context.messages, model)) {
		if (msg.role === "user") {
			if (typeof msg.content === "string") {
				messages.push({
					role: "user",
					content: [{ type: "input_text", text: sanitizeSurrogates(msg.content) }],
				});
			} else {
				const content = msg.content.map((item) => {
					if (item.type === "text") {
						return { type: "input_text", text: sanitizeSurrogates(item.text) };
					}
					return {
						type: "input_image",
						detail: "auto",
						image_url: `data:${item.mimeType};base64,${item.data}`,
					};
				});
				if (content.length > 0) {
					messages.push({ role: "user", content });
				}
			}
		} else if (msg.role === "assistant") {
			const output: unknown[] = [];
			const isDifferentModel = msg.model !== model.id && msg.provider === model.provider && msg.api === model.api;
			let textBlockIndex = 0;
			for (const block of msg.content) {
				if (block.type === "thinking") {
					if (block.thinkingSignature) {
						output.push(JSON.parse(block.thinkingSignature));
					}
				} else if (block.type === "text") {
					const parsedSignature = parseTextSignature(block.textSignature);
					const fallbackMessageId = textBlockIndex === 0 ? `msg_pi_${msgIndex}` : `msg_pi_${msgIndex}_${textBlockIndex}`;
					textBlockIndex++;
					let msgId = parsedSignature?.id;
					if (!msgId) {
						msgId = fallbackMessageId;
					} else if (msgId.length > 64) {
						msgId = `msg_${shortHash(msgId)}`;
					}
					output.push({
						type: "message",
						role: "assistant",
						content: [{ type: "output_text", text: sanitizeSurrogates(block.text), annotations: [] }],
						status: "completed",
						id: msgId,
						phase: parsedSignature?.phase,
					});
				} else if (block.type === "toolCall") {
					const [callId, itemIdRaw] = block.id.split("|");
					let itemId: string | undefined = itemIdRaw;
					if (isDifferentModel && itemId?.startsWith("fc_")) {
						itemId = undefined;
					}
					output.push({
						type: "function_call",
						id: itemId,
						call_id: callId,
						name: block.name,
						arguments: JSON.stringify(block.arguments),
					});
				}
			}
			messages.push(...output);
		} else if (msg.role === "toolResult") {
			const textResult = msg.content
				.filter((block): block is TextContent => block.type === "text")
				.map((block) => block.text)
				.join("\n");
			const hasImages = msg.content.some((block) => block.type === "image");
			const hasText = textResult.length > 0;
			const [callId] = msg.toolCallId.split("|");
			let output: unknown;
			if (hasImages && model.input.includes("image")) {
				const contentParts: unknown[] = [];
				if (hasText) {
					contentParts.push({ type: "input_text", text: sanitizeSurrogates(textResult) });
				}
				for (const block of msg.content) {
					if (block.type === "image") {
						contentParts.push({
							type: "input_image",
							detail: "auto",
							image_url: `data:${block.mimeType};base64,${block.data}`,
						});
					}
				}
				output = contentParts;
			} else {
				output = sanitizeSurrogates(hasText ? textResult : "(see attached image)");
			}
			messages.push({ type: "function_call_output", call_id: callId, output });
		}
		msgIndex++;
	}
	return messages;
}

function convertResponsesTools(tools: Tool[]): unknown[] {
	return tools.map((tool) => ({
		type: "function",
		name: tool.name,
		description: tool.description,
		parameters: tool.parameters,
		strict: false,
	}));
}

function buildResponsesWebSocketPayload<TApi extends Api>(model: Model<TApi>, context: Context, options?: SimpleStreamOptions): ResponsesPayload {
	const cacheRetention = resolveCacheRetention(options?.cacheRetention);
	const payload: ResponsesPayload = {
		model: model.id,
		input: convertResponsesMessages(model, context),
		prompt_cache_key: cacheRetention === "none" ? undefined : clampOpenAIPromptCacheKey(options?.sessionId),
	};

	if (options?.maxTokens) {
		payload.max_output_tokens = options.maxTokens;
	}
	if (options?.temperature !== undefined) {
		payload.temperature = options.temperature;
	}
	const serviceTier = (options as { serviceTier?: unknown } | undefined)?.serviceTier;
	if (serviceTier !== undefined) {
		payload.service_tier = serviceTier;
	}
	if (context.tools && context.tools.length > 0) {
		payload.tools = convertResponsesTools(context.tools);
	}
	if (model.reasoning) {
		const clampedReasoning = options?.reasoning ? clampThinkingLevel(model, options.reasoning) : undefined;
		const reasoningEffort = clampedReasoning === "off" ? undefined : clampedReasoning;
		if (reasoningEffort) {
			payload.reasoning = {
				effort: model.thinkingLevelMap?.[reasoningEffort] ?? reasoningEffort,
				summary: "auto",
			};
			payload.include = ["reasoning.encrypted_content"];
		} else if (model.thinkingLevelMap?.off !== null) {
			payload.reasoning = {
				effort: model.thinkingLevelMap?.off ?? "none",
			};
		}
	}

	return payload;
}

function createEmptyAssistantMessage<TApi extends Api>(model: Model<TApi>): AssistantMessage {
	return {
		role: "assistant",
		content: [],
		api: model.api,
		provider: model.provider,
		model: model.id,
		usage: {
			input: 0,
			output: 0,
			cacheRead: 0,
			cacheWrite: 0,
			totalTokens: 0,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason: "stop",
		timestamp: Date.now(),
	};
}

function resolveResponsesWebSocketUrl(baseUrl: string): string {
	const raw = baseUrl && baseUrl.trim().length > 0 ? baseUrl : "https://models.mixlayer.ai/v1";
	const normalized = raw.replace(/\/+$/, "");
	const url = new URL(normalized.endsWith("/responses") ? normalized : `${normalized}/responses`);
	if (url.protocol === "https:") {
		url.protocol = "wss:";
	} else if (url.protocol === "http:") {
		url.protocol = "ws:";
	}
	return url.toString();
}

function buildWebSocketHeaders<TApi extends Api>(model: Model<TApi>, apiKey: string, options?: SimpleStreamOptions): Record<string, string> {
	const headers: Record<string, string> = {
		...model.headers,
		Authorization: `Bearer ${apiKey}`,
	};
	if (options?.sessionId) {
		headers["x-client-request-id"] = options.sessionId;
	}
	if (options?.headers) {
		Object.assign(headers, options.headers);
	}
	delete headers.accept;
	delete headers.Accept;
	delete headers["content-type"];
	delete headers["Content-Type"];
	return headers;
}

async function getWebSocketConstructor(): Promise<WebSocketConstructorLike> {
	const ctor = globalThis.WebSocket as unknown as WebSocketConstructorLike | undefined;
	if (typeof ctor !== "function") {
		throw new Error("WebSocket transport is not available in this runtime");
	}
	return ctor;
}

function extractWebSocketError(event: Event): Error {
	const candidate = event as Event & { message?: unknown; error?: unknown };
	if (typeof candidate.message === "string" && candidate.message.length > 0) {
		return new Error(candidate.message);
	}
	if (candidate.error instanceof Error && candidate.error.message.length > 0) {
		return candidate.error;
	}
	if (candidate.error && typeof candidate.error === "object" && "message" in candidate.error) {
		const message = (candidate.error as { message?: unknown }).message;
		if (typeof message === "string" && message.length > 0) {
			return new Error(message);
		}
	}
	return new Error("WebSocket error");
}

function extractWebSocketCloseError(event: CloseEvent): Error {
	const codeText = typeof event.code === "number" ? ` ${event.code}` : "";
	const reasonText = typeof event.reason === "string" && event.reason.length > 0 ? ` ${event.reason}` : "";
	return new Error(`WebSocket closed${codeText}${reasonText}`.trim());
}

function closeWebSocketSilently(socket: WebSocketLike, reason = "done"): void {
	try {
		socket.close(WEBSOCKET_NORMAL_CLOSE_CODE, reason);
	} catch {
		// Closing is best-effort.
	}
}

function unrefTimer(timer: ReturnType<typeof setTimeout>): void {
	if (typeof timer === "object" && "unref" in timer && typeof timer.unref === "function") {
		timer.unref();
	}
}

function getWebSocketReadyState(socket: WebSocketLike): number | undefined {
	return typeof socket.readyState === "number" ? socket.readyState : undefined;
}

function isWebSocketReusable(socket: WebSocketLike): boolean {
	const readyState = getWebSocketReadyState(socket);
	return readyState === undefined || readyState === 1;
}

function closeSessionEntry(sessionId: string, entry: WebSocketSessionEntry): void {
	if (entry.idleTimer) {
		clearTimeout(entry.idleTimer);
	}
	closeWebSocketSilently(entry.socket);
	if (websocketSessionCache.get(sessionId) === entry) {
		websocketSessionCache.delete(sessionId);
	}
}

function scheduleSessionWebSocketExpiry(sessionId: string, entry: WebSocketSessionEntry): void {
	if (entry.idleTimer) {
		clearTimeout(entry.idleTimer);
	}
	entry.idleTimer = setTimeout(() => {
		if (entry.busy) {
			return;
		}
		closeSessionEntry(sessionId, entry);
	}, SESSION_WEBSOCKET_CACHE_TTL_MS);
	unrefTimer(entry.idleTimer);
}

async function connectWebSocket(url: string, headers: Record<string, string>, signal: AbortSignal | undefined, connectTimeoutMs: number): Promise<WebSocketLike> {
	const WebSocketCtor = await getWebSocketConstructor();
	return new Promise((resolve, reject) => {
		let settled = false;
		let timeout: ReturnType<typeof setTimeout> | undefined;
		let socket: WebSocketLike;
		try {
			socket = new WebSocketCtor(url, { headers });
		} catch (error) {
			reject(error instanceof Error ? error : new Error(String(error)));
			return;
		}

		const cleanup = () => {
			if (timeout) {
				clearTimeout(timeout);
			}
			socket.removeEventListener("open", onOpen);
			socket.removeEventListener("error", onError);
			socket.removeEventListener("close", onClose);
			signal?.removeEventListener("abort", onAbort);
		};
		const fail = (error: Error, closeReason?: string) => {
			if (settled) {
				return;
			}
			settled = true;
			cleanup();
			if (closeReason) {
				closeWebSocketSilently(socket, closeReason);
			}
			reject(error);
		};
		const onOpen = () => {
			if (settled) {
				return;
			}
			settled = true;
			cleanup();
			resolve(socket);
		};
		const onError = (event: Event) => fail(extractWebSocketError(event));
		const onClose = (event: CloseEvent) => fail(extractWebSocketCloseError(event));
		const onAbort = () => fail(new Error("Request was aborted"), "aborted");

		socket.addEventListener("open", onOpen);
		socket.addEventListener("error", onError);
		socket.addEventListener("close", onClose);
		signal?.addEventListener("abort", onAbort);
		if (connectTimeoutMs > 0) {
			timeout = setTimeout(() => {
				fail(new Error(`WebSocket connect timeout after ${connectTimeoutMs}ms`), "connect_timeout");
			}, connectTimeoutMs);
			unrefTimer(timeout);
		}
		if (signal?.aborted) {
			onAbort();
		}
	});
}

async function acquireWebSocket(
	url: string,
	headers: Record<string, string>,
	sessionId: string | undefined,
	signal: AbortSignal | undefined,
	connectTimeoutMs: number,
	cacheable: boolean,
): Promise<WebSocketLease> {
	if (!cacheable || !sessionId) {
		const socket = await connectWebSocket(url, headers, signal, connectTimeoutMs);
		return {
			socket,
			release: () => closeWebSocketSilently(socket),
		};
	}

	const cached = websocketSessionCache.get(sessionId);
	if (cached) {
		if (cached.idleTimer) {
			clearTimeout(cached.idleTimer);
			cached.idleTimer = undefined;
		}
		if (!cached.busy && isWebSocketReusable(cached.socket)) {
			cached.busy = true;
			return {
				socket: cached.socket,
				entry: cached,
				release: ({ keep } = {}) => {
					if (!keep || !isWebSocketReusable(cached.socket)) {
						closeSessionEntry(sessionId, cached);
						return;
					}
					cached.busy = false;
					scheduleSessionWebSocketExpiry(sessionId, cached);
				},
			};
		}
		if (cached.busy) {
			const socket = await connectWebSocket(url, headers, signal, connectTimeoutMs);
			return {
				socket,
				release: () => closeWebSocketSilently(socket),
			};
		}
		closeSessionEntry(sessionId, cached);
	}

	const socket = await connectWebSocket(url, headers, signal, connectTimeoutMs);
	const entry: WebSocketSessionEntry = { socket, busy: true };
	websocketSessionCache.set(sessionId, entry);
	return {
		socket,
		entry,
		release: ({ keep } = {}) => {
			if (!keep || !isWebSocketReusable(entry.socket)) {
				closeSessionEntry(sessionId, entry);
				return;
			}
			entry.busy = false;
			scheduleSessionWebSocketExpiry(sessionId, entry);
		},
	};
}

async function decodeWebSocketData(data: unknown): Promise<string | null> {
	if (typeof data === "string") {
		return data;
	}
	if (data instanceof ArrayBuffer) {
		return new TextDecoder().decode(new Uint8Array(data));
	}
	if (ArrayBuffer.isView(data)) {
		return new TextDecoder().decode(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
	}
	if (data && typeof data === "object" && "arrayBuffer" in data) {
		const arrayBuffer = await (data as { arrayBuffer: () => Promise<ArrayBuffer> }).arrayBuffer();
		return new TextDecoder().decode(new Uint8Array(arrayBuffer));
	}
	return null;
}

async function* parseWebSocket(socket: WebSocketLike, signal: AbortSignal | undefined, idleTimeoutMs: number | undefined): AsyncIterable<ResponsesStreamEvent> {
	const queue: ResponsesStreamEvent[] = [];
	let pending: (() => void) | null = null;
	let done = false;
	let failed: Error | null = null;
	let sawCompletion = false;
	const wake = () => {
		if (!pending) {
			return;
		}
		const resolve = pending;
		pending = null;
		resolve();
	};

	const onMessage = (event: MessageEvent) => {
		void (async () => {
			let text: string | null = null;
			try {
				text = await decodeWebSocketData(event.data);
				if (!text) {
					return;
				}
				const parsed: ResponsesStreamEvent = JSON.parse(text);
				const type = typeof parsed.type === "string" ? parsed.type : "";
				if (type === "response.completed" || type === "response.done" || type === "response.incomplete") {
					sawCompletion = true;
					done = true;
				}
				queue.push(parsed);
				wake();
			} catch (cause) {
				failed = new Error(`Invalid Responses WebSocket JSON: ${cause instanceof Error ? cause.message : String(cause)}`);
				done = true;
				wake();
			}
		})();
	};
	const onError = (event: Event) => {
		failed = extractWebSocketError(event);
		done = true;
		wake();
	};
	const onClose = (event: CloseEvent) => {
		if (!sawCompletion && !failed) {
			failed = extractWebSocketCloseError(event);
		}
		done = true;
		wake();
	};
	const onAbort = () => {
		failed = new Error("Request was aborted");
		done = true;
		wake();
	};

	socket.addEventListener("message", onMessage);
	socket.addEventListener("error", onError);
	socket.addEventListener("close", onClose);
	signal?.addEventListener("abort", onAbort);
	try {
		while (true) {
			if (signal?.aborted) {
				throw new Error("Request was aborted");
			}
			if (queue.length > 0) {
				yield queue.shift()!;
				continue;
			}
			if (done) {
				break;
			}
			let timeout: ReturnType<typeof setTimeout> | undefined;
			await new Promise<void>((resolve, reject) => {
				pending = resolve;
				if (idleTimeoutMs !== undefined && idleTimeoutMs > 0) {
					timeout = setTimeout(() => {
						const error = new Error(`WebSocket idle timeout after ${idleTimeoutMs}ms`);
						failed = error;
						done = true;
						pending = null;
						closeWebSocketSilently(socket, "idle_timeout");
						reject(error);
					}, idleTimeoutMs);
				}
			}).finally(() => {
				if (timeout) {
					clearTimeout(timeout);
				}
			});
		}
		if (failed) {
			throw failed;
		}
		if (!sawCompletion) {
			throw new Error("WebSocket stream closed before response.completed");
		}
	} finally {
		socket.removeEventListener("message", onMessage);
		socket.removeEventListener("error", onError);
		socket.removeEventListener("close", onClose);
		signal?.removeEventListener("abort", onAbort);
	}
}

async function* normalizeResponsesWebSocketEvents(events: AsyncIterable<ResponsesStreamEvent>): AsyncIterable<ResponsesStreamEvent> {
	for await (const event of events) {
		const type = typeof event.type === "string" ? event.type : undefined;
		if (!type) {
			continue;
		}
		if (type === "response.done" || type === "response.incomplete") {
			const response = event.response;
			yield { ...event, type: "response.completed", response };
			return;
		}
		yield event;
		if (type === "response.completed") {
			return;
		}
	}
}

function mapStopReason(status: unknown): AssistantMessage["stopReason"] {
	switch (status) {
		case undefined:
		case "completed":
		case "in_progress":
		case "queued":
			return "stop";
		case "incomplete":
			return "length";
		case "failed":
		case "cancelled":
			return "error";
		default:
			throw new Error(`Unhandled stop reason: ${String(status)}`);
	}
}

function cloneJson<T>(value: T): T {
	return JSON.parse(JSON.stringify(value)) as T;
}

function requestBodyWithoutInput(body: ResponsesPayload): ResponsesPayload {
	const { input: _input, previous_response_id: _previousResponseId, ...rest } = body;
	return rest;
}

function jsonEqual(a: unknown, b: unknown): boolean {
	return JSON.stringify(a) === JSON.stringify(b);
}

function getInputItems(body: ResponsesPayload): unknown[] | undefined {
	return Array.isArray(body.input) ? body.input : undefined;
}

function inputStartsWith(input: unknown[], prefix: unknown[]): boolean {
	if (input.length < prefix.length) {
		return false;
	}
	for (let index = 0; index < prefix.length; index++) {
		if (!jsonEqual(input[index], prefix[index])) {
			return false;
		}
	}
	return true;
}

function buildDeltaRequestBody(currentBody: ResponsesPayload, continuation: WebSocketContinuation | undefined): ResponsesPayload | undefined {
	if (!continuation) {
		return undefined;
	}
	if (!continuation.lastResponseId) {
		return undefined;
	}
	if (!jsonEqual(requestBodyWithoutInput(currentBody), requestBodyWithoutInput(continuation.lastRequestBody))) {
		return undefined;
	}

	const currentInput = getInputItems(currentBody);
	const previousInput = getInputItems(continuation.lastRequestBody);
	if (!currentInput || !previousInput) {
		return undefined;
	}

	const expectedPrefix = [...previousInput, ...continuation.lastResponseItems];
	if (!inputStartsWith(currentInput, expectedPrefix)) {
		return undefined;
	}

	const deltaInput = currentInput.slice(expectedPrefix.length);
	if (deltaInput.length === 0) {
		return undefined;
	}

	return {
		...currentBody,
		previous_response_id: continuation.lastResponseId,
		input: deltaInput,
	};
}

function buildAssistantResponseItems<TApi extends Api>(model: Model<TApi>, output: AssistantMessage): unknown[] {
	return convertResponsesMessages(model, { messages: [output] }).filter((item) => {
		return !(item && typeof item === "object" && "type" in item && item.type === "function_call_output");
	});
}

function updateContinuation<TApi extends Api>(
	entry: WebSocketSessionEntry | undefined,
	model: Model<TApi>,
	fullRequestBody: ResponsesPayload,
	output: AssistantMessage,
): void {
	if (!entry) {
		return;
	}
	if (!output.responseId || output.stopReason === "error" || output.stopReason === "aborted") {
		entry.continuation = undefined;
		return;
	}
	entry.continuation = {
		lastRequestBody: cloneJson(fullRequestBody),
		lastResponseId: output.responseId,
		lastResponseItems: cloneJson(buildAssistantResponseItems(model, output)),
	};
}

function isRecoverableDeltaError(error: unknown): boolean {
	const message = error instanceof Error ? error.message : String(error);
	return /previous_response|previous_response_id|unsupported_parameter/i.test(message);
}

function emitWebSocketStats(streamOptions: ResponsesWebSocketStreamOptions, event: ResponsesWebSocketStatsEvent): void {
	try {
		streamOptions.onStats?.(event);
	} catch {
		// Stats must never affect request handling.
	}
}

async function processResponsesStream<TApi extends Api>(
	openaiStream: AsyncIterable<ResponsesStreamEvent>,
	output: AssistantMessage,
	stream: AssistantMessageEventStream,
	model: Model<TApi>,
): Promise<void> {
	let currentItem: ResponsesStreamEvent | null = null;
	let currentBlock: (AssistantMessage["content"][number] & { partialJson?: string }) | null = null;
	const blockIndex = () => output.content.length - 1;

	for await (const event of openaiStream) {
		if (event.type === "response.created") {
			output.responseId = event.response?.id;
		} else if (event.type === "response.output_item.added") {
			const item = event.item;
			if (item?.type === "reasoning") {
				currentItem = item;
				currentBlock = { type: "thinking", thinking: "" };
				output.content.push(currentBlock);
				stream.push({ type: "thinking_start", contentIndex: blockIndex(), partial: output });
			} else if (item?.type === "message") {
				currentItem = item;
				currentBlock = { type: "text", text: "" };
				output.content.push(currentBlock);
				stream.push({ type: "text_start", contentIndex: blockIndex(), partial: output });
			} else if (item?.type === "function_call") {
				currentItem = item;
				currentBlock = {
					type: "toolCall",
					id: `${item.call_id}|${item.id}`,
					name: item.name,
					arguments: {},
					partialJson: item.arguments || "",
				};
				output.content.push(currentBlock);
				stream.push({ type: "toolcall_start", contentIndex: blockIndex(), partial: output });
			}
		} else if (event.type === "response.reasoning_summary_part.added") {
			if (currentItem?.type === "reasoning") {
				currentItem.summary = currentItem.summary || [];
				currentItem.summary.push(event.part);
			}
		} else if (event.type === "response.reasoning_summary_text.delta") {
			if (currentItem?.type === "reasoning" && currentBlock?.type === "thinking") {
				currentItem.summary = currentItem.summary || [];
				const lastPart = currentItem.summary[currentItem.summary.length - 1];
				if (lastPart) {
					currentBlock.thinking += event.delta;
					lastPart.text += event.delta;
					stream.push({ type: "thinking_delta", contentIndex: blockIndex(), delta: event.delta, partial: output });
				}
			}
		} else if (event.type === "response.reasoning_summary_part.done") {
			if (currentItem?.type === "reasoning" && currentBlock?.type === "thinking") {
				currentItem.summary = currentItem.summary || [];
				const lastPart = currentItem.summary[currentItem.summary.length - 1];
				if (lastPart) {
					currentBlock.thinking += "\n\n";
					lastPart.text += "\n\n";
					stream.push({ type: "thinking_delta", contentIndex: blockIndex(), delta: "\n\n", partial: output });
				}
			}
		} else if (event.type === "response.reasoning_text.delta") {
			if (currentItem?.type === "reasoning" && currentBlock?.type === "thinking") {
				currentBlock.thinking += event.delta;
				stream.push({ type: "thinking_delta", contentIndex: blockIndex(), delta: event.delta, partial: output });
			}
		} else if (event.type === "response.content_part.added") {
			if (currentItem?.type === "message") {
				currentItem.content = currentItem.content || [];
				if (event.part?.type === "output_text" || event.part?.type === "refusal") {
					currentItem.content.push(event.part);
				}
			}
		} else if (event.type === "response.output_text.delta") {
			if (currentItem?.type === "message" && currentBlock?.type === "text") {
				const lastPart = currentItem.content?.[currentItem.content.length - 1];
				if (lastPart?.type === "output_text") {
					currentBlock.text += event.delta;
					lastPart.text += event.delta;
					stream.push({ type: "text_delta", contentIndex: blockIndex(), delta: event.delta, partial: output });
				}
			}
		} else if (event.type === "response.refusal.delta") {
			if (currentItem?.type === "message" && currentBlock?.type === "text") {
				const lastPart = currentItem.content?.[currentItem.content.length - 1];
				if (lastPart?.type === "refusal") {
					currentBlock.text += event.delta;
					lastPart.refusal += event.delta;
					stream.push({ type: "text_delta", contentIndex: blockIndex(), delta: event.delta, partial: output });
				}
			}
		} else if (event.type === "response.function_call_arguments.delta") {
			if (currentItem?.type === "function_call" && currentBlock?.type === "toolCall") {
				currentBlock.partialJson = `${currentBlock.partialJson ?? ""}${event.delta}`;
				currentBlock.arguments = parseStreamingJson(currentBlock.partialJson);
				stream.push({ type: "toolcall_delta", contentIndex: blockIndex(), delta: event.delta, partial: output });
			}
		} else if (event.type === "response.function_call_arguments.done") {
			if (currentItem?.type === "function_call" && currentBlock?.type === "toolCall") {
				const previousPartialJson = currentBlock.partialJson ?? "";
				currentBlock.partialJson = event.arguments;
				currentBlock.arguments = parseStreamingJson(currentBlock.partialJson);
				if (typeof event.arguments === "string" && event.arguments.startsWith(previousPartialJson)) {
					const delta = event.arguments.slice(previousPartialJson.length);
					if (delta.length > 0) {
						stream.push({ type: "toolcall_delta", contentIndex: blockIndex(), delta, partial: output });
					}
				}
			}
		} else if (event.type === "response.output_item.done") {
			const item = event.item;
			if (item?.type === "reasoning" && currentBlock?.type === "thinking") {
				const summaryText = item.summary?.map((summary: { text?: string }) => summary.text).join("\n\n") || "";
				const contentText = item.content?.map((content: { text?: string }) => content.text).join("\n\n") || "";
				currentBlock.thinking = summaryText || contentText || currentBlock.thinking;
				currentBlock.thinkingSignature = JSON.stringify(item);
				stream.push({ type: "thinking_end", contentIndex: blockIndex(), content: currentBlock.thinking, partial: output });
				currentBlock = null;
			} else if (item?.type === "message" && currentBlock?.type === "text") {
				currentBlock.text = item.content.map((content: { type: string; text?: string; refusal?: string }) => (content.type === "output_text" ? content.text : content.refusal)).join("");
				currentBlock.textSignature = encodeTextSignatureV1(item.id, item.phase);
				stream.push({ type: "text_end", contentIndex: blockIndex(), content: currentBlock.text, partial: output });
				currentBlock = null;
			} else if (item?.type === "function_call") {
				const args =
					currentBlock?.type === "toolCall" && currentBlock.partialJson
						? parseStreamingJson(currentBlock.partialJson)
						: parseStreamingJson(item.arguments || "{}");
				let toolCall: ToolCall;
				if (currentBlock?.type === "toolCall") {
					currentBlock.arguments = args;
					delete currentBlock.partialJson;
					toolCall = currentBlock;
				} else {
					toolCall = {
						type: "toolCall",
						id: `${item.call_id}|${item.id}`,
						name: item.name,
						arguments: args,
					};
				}
				currentBlock = null;
				stream.push({ type: "toolcall_end", contentIndex: blockIndex(), toolCall, partial: output });
			}
		} else if (event.type === "response.completed") {
			const response = event.response;
			if (response?.id) {
				output.responseId = response.id;
			}
			if (response?.usage) {
				const cachedTokens = response.usage.input_tokens_details?.cached_tokens || 0;
				output.usage = {
					input: (response.usage.input_tokens || 0) - cachedTokens,
					output: response.usage.output_tokens || 0,
					cacheRead: cachedTokens,
					cacheWrite: 0,
					totalTokens: response.usage.total_tokens || 0,
					cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
				};
			}
			calculateCost(model, output.usage);
			output.stopReason = mapStopReason(response?.status);
			if (output.content.some((block) => block.type === "toolCall") && output.stopReason === "stop") {
				output.stopReason = "toolUse";
			}
		} else if (event.type === "error") {
			const error = event.error && typeof event.error === "object" ? event.error : undefined;
			const code = event.code ?? error?.code ?? error?.type;
			const message = event.message ?? error?.message;
			throw new Error(message ? `${code ? `${code}: ` : ""}${message}` : JSON.stringify(event));
		} else if (event.type === "response.failed") {
			const error = event.response?.error;
			const details = event.response?.incomplete_details;
			const message = error
				? `${error.code || "unknown"}: ${error.message || "no message"}`
				: details?.reason
					? `incomplete: ${details.reason}`
					: "Unknown error (no error details in response)";
			throw new Error(message);
		}
	}
}

function stripStreamingScratch(output: AssistantMessage): void {
	for (const block of output.content) {
		delete (block as { partialJson?: string }).partialJson;
	}
}

function normalizeTimeoutMs(value: number | undefined): number | undefined {
	if (value === undefined) {
		return undefined;
	}
	if (!Number.isFinite(value) || value < 0) {
		throw new Error(`Invalid timeoutMs: ${String(value)}`);
	}
	return Math.floor(value);
}

async function sendResponsesWebSocketRequest<TApi extends Api>(
	socket: WebSocketLike,
	requestBody: ResponsesPayload,
	output: AssistantMessage,
	stream: AssistantMessageEventStream,
	model: Model<TApi>,
	options: SimpleStreamOptions | undefined,
	idleTimeoutMs: number | undefined,
): Promise<void> {
	socket.send(JSON.stringify({ type: "response.create", ...requestBody }));
	await processResponsesStream(normalizeResponsesWebSocketEvents(parseWebSocket(socket, options?.signal, idleTimeoutMs)), output, stream, model);
}

export function createResponsesWebSocketStreamSimple(sanitizePayload: PayloadSanitizer, streamOptions: ResponsesWebSocketStreamOptions = {}) {
	return (model: Model<Api>, context: Context, options?: SimpleStreamOptions): AssistantMessageEventStream => {
		const stream = createAssistantMessageEventStream();
		void (async () => {
			const output = createEmptyAssistantMessage(model);
			let lease: WebSocketLease | undefined;
			let keepLease = false;
			try {
				const apiKey = options?.apiKey;
				if (!apiKey) {
					throw new Error(`No API key for provider: ${model.provider}`);
				}

				let payload: unknown = buildResponsesWebSocketPayload(model, context, options);
				const nextPayload = await options?.onPayload?.(payload, model);
				payload = sanitizePayload(nextPayload ?? payload);
				if (!payload || typeof payload !== "object") {
					throw new Error("Mixlayer Responses WebSocket payload must be an object.");
				}
				const fullRequestBody = payload as ResponsesPayload;

				const connectTimeoutMs = normalizeTimeoutMs(options?.websocketConnectTimeoutMs) ?? DEFAULT_WEBSOCKET_CONNECT_TIMEOUT_MS;
				const idleTimeoutMs = normalizeTimeoutMs(options?.timeoutMs);
				const sessionId = options?.sessionId;
				const deltaEnabled = streamOptions.delta === true;
				const deltaDisabled = !!sessionId && deltaDisabledSessions.has(sessionId);
				const cacheable = deltaEnabled && !!sessionId && !deltaDisabled;
				emitWebSocketStats(streamOptions, { type: "stream_started", deltaEnabled, cacheable, deltaDisabled, sessionId });
				lease = await acquireWebSocket(
					resolveResponsesWebSocketUrl(model.baseUrl),
					buildWebSocketHeaders(model, apiKey, options),
					sessionId,
					options?.signal,
					connectTimeoutMs,
					cacheable,
				);

				let requestBody = cacheable ? buildDeltaRequestBody(fullRequestBody, lease.entry?.continuation) : undefined;
				let requestKind: ResponsesWebSocketRequestKind = requestBody !== undefined ? "delta" : "full";
				requestBody ??= fullRequestBody;

				const sendTrackedRequest = async (body: ResponsesPayload, kind: ResponsesWebSocketRequestKind, retry: boolean) => {
					emitWebSocketStats(streamOptions, { type: "request_started", requestKind: kind, retry });
					try {
						await sendResponsesWebSocketRequest(lease!.socket, body, output, stream, model, options, idleTimeoutMs);
						emitWebSocketStats(streamOptions, { type: "request_finished", requestKind: kind, retry, ok: true, recoverable: false });
					} catch (error) {
						const recoverable = kind === "delta" && output.content.length === 0 && isRecoverableDeltaError(error);
						emitWebSocketStats(streamOptions, { type: "request_finished", requestKind: kind, retry, ok: false, recoverable });
						throw error;
					}
				};

				stream.push({ type: "start", partial: output });
				try {
					await sendTrackedRequest(requestBody, requestKind, false);
				} catch (error) {
					if (requestKind !== "delta" || output.content.length > 0 || !isRecoverableDeltaError(error)) {
						throw error;
					}

					if (sessionId) {
						deltaDisabledSessions.add(sessionId);
						emitWebSocketStats(streamOptions, { type: "delta_disabled", sessionId });
					}
					if (lease.entry) {
						lease.entry.continuation = undefined;
					}
					lease.release({ keep: false });
					lease = undefined;
					lease = await acquireWebSocket(
						resolveResponsesWebSocketUrl(model.baseUrl),
						buildWebSocketHeaders(model, apiKey, options),
						sessionId,
						options?.signal,
						connectTimeoutMs,
						false,
					);
					requestKind = "full";
					requestBody = fullRequestBody;
					await sendTrackedRequest(requestBody, requestKind, true);
				}

				if (options?.signal?.aborted) {
					throw new Error("Request was aborted");
				}
				if (output.stopReason === "error" || output.stopReason === "aborted") {
					throw new Error(output.errorMessage || "An unknown error occurred");
				}
				if (cacheable && lease.entry) {
					updateContinuation(lease.entry, model, fullRequestBody, output);
				}
				keepLease = cacheable && !!lease.entry && !options?.signal?.aborted;
				stream.push({ type: "done", reason: output.stopReason, message: output });
				stream.end();
			} catch (error) {
				if (lease?.entry) {
					lease.entry.continuation = undefined;
				}
				stripStreamingScratch(output);
				output.stopReason = options?.signal?.aborted ? "aborted" : "error";
				output.errorMessage = error instanceof Error ? error.message : String(error);
				stream.push({ type: "error", reason: output.stopReason, error: output });
				stream.end();
			} finally {
				lease?.release({ keep: keepLease });
			}
		})();
		return stream;
	};
}
