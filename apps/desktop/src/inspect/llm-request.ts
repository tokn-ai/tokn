import { numberField, stringField } from "./format.js";
import { readRequestClassification } from "./request-classification.js";
import type { RequestClassification } from "./types.js";

const LLM_ENDPOINTS = new Set([
  "chat",
  "chat_completions",
  "messages",
  "responses",
]);
const LLM_PATH_SUFFIXES = [
  "/chat/completions",
  "/messages",
  "/responses",
  "/responses/compact",
];

export interface LlmUsage {
  kind: string | undefined;
  input_tokens: number | undefined;
  output_tokens: number | undefined;
  total_tokens: number | undefined;
  cache_read_tokens: number | undefined;
  cache_write_tokens: number | undefined;
  reasoning_tokens: number | undefined;
}

export interface LlmRequestOverviewModel {
  client_method: string | undefined;
  client_url: string | undefined;
  provider_method: string | undefined;
  provider_url: string | undefined;
  endpoint: string | undefined;
  provider_id: string | undefined;
  model: string | undefined;
  account_id: string | undefined;
  pipeline: string | undefined;
  mode: string | undefined;
  request_classification: RequestClassification | undefined;
  stream: boolean | undefined;
  client_status: number | undefined;
  provider_status: number | undefined;
  latency_ms: number | undefined;
  first_response_ms: number | undefined;
  streamed_ms: number | undefined;
  usage: LlmUsage;
}

function jsonRecord(value: unknown): Record<string, unknown> | undefined {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  if (typeof value !== "string") {
    return undefined;
  }
  try {
    const parsed = JSON.parse(value) as unknown;
    return parsed !== null &&
      typeof parsed === "object" &&
      !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : undefined;
  } catch {
    return undefined;
  }
}

function nonNegativeNumber(
  record: Record<string, unknown> | undefined,
  field: string,
): number | undefined {
  const value = record?.[field];
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? value
    : undefined;
}

function endpointPath(value: unknown): string | undefined {
  if (typeof value !== "string" || value.length === 0) {
    return undefined;
  }
  try {
    return new URL(value, "http://localhost").pathname
      .toLowerCase()
      .replace(/\/$/, "");
  } catch {
    return value.split(/[?#]/, 1)[0]?.toLowerCase().replace(/\/$/, "");
  }
}

export function isLlmRequest(request: Record<string, unknown>): boolean {
  const usage = jsonRecord(request.usage_json);
  const kind = stringField(usage ?? {}, "kind")?.toLowerCase();
  if (kind && LLM_ENDPOINTS.has(kind)) {
    return true;
  }

  const endpoint = stringField(request, "endpoint")?.toLowerCase();
  if (endpoint && LLM_ENDPOINTS.has(endpoint)) {
    return true;
  }

  const has_route_identity =
    stringField(request, "model") !== undefined ||
    stringField(request, "provider_id") !== undefined;
  if (!has_route_identity) {
    return false;
  }

  return [request.inbound_req_url, request.outbound_req_url]
    .map(endpointPath)
    .some(
      (path) =>
        path !== undefined &&
        LLM_PATH_SUFFIXES.some((suffix) => path.endsWith(suffix)),
    );
}

export function buildLlmRequestOverview(
  request: Record<string, unknown>,
): LlmRequestOverviewModel | undefined {
  if (!isLlmRequest(request)) {
    return undefined;
  }

  const usage = jsonRecord(request.usage_json);
  const context = jsonRecord(request.ctx_json);
  const parameters = jsonRecord(request.params_json);
  const latency_ms = nonNegativeNumber(context, "latency_ms");
  const first_response_ms = nonNegativeNumber(context, "latency_header_ms");

  return {
    client_method: stringField(request, "inbound_req_method"),
    client_url:
      stringField(request, "inbound_req_url") ??
      stringField(request, "endpoint"),
    provider_method: stringField(request, "outbound_req_method"),
    provider_url: stringField(request, "outbound_req_url"),
    endpoint: stringField(request, "endpoint"),
    provider_id: stringField(request, "provider_id"),
    model: stringField(request, "model"),
    account_id: stringField(request, "account_id"),
    pipeline: stringField(context ?? {}, "pipeline_id"),
    mode: stringField(context ?? {}, "mode"),
    request_classification: readRequestClassification(request),
    stream:
      typeof parameters?.stream === "boolean" ? parameters.stream : undefined,
    client_status:
      numberField(request, "inbound_resp_status") ??
      numberField(request, "status"),
    provider_status: numberField(request, "outbound_resp_status"),
    latency_ms,
    first_response_ms,
    streamed_ms:
      latency_ms !== undefined && first_response_ms !== undefined
        ? Math.max(0, latency_ms - first_response_ms)
        : undefined,
    usage: {
      kind: stringField(usage ?? {}, "kind"),
      input_tokens: nonNegativeNumber(usage, "input"),
      output_tokens: nonNegativeNumber(usage, "output"),
      total_tokens: nonNegativeNumber(usage, "total"),
      cache_read_tokens: nonNegativeNumber(usage, "cache_read"),
      cache_write_tokens: nonNegativeNumber(usage, "cache_write"),
      reasoning_tokens: nonNegativeNumber(usage, "reasoning"),
    },
  };
}

export function cacheReadPercent(usage: LlmUsage): number | undefined {
  if (
    usage.input_tokens === undefined ||
    usage.input_tokens === 0 ||
    usage.cache_read_tokens === undefined
  ) {
    return undefined;
  }
  return Math.min(100, (usage.cache_read_tokens / usage.input_tokens) * 100);
}
