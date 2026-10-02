import {
  buildLlmRequestOverview,
  cacheReadPercent,
  isLlmRequest,
} from "./llm-request.js";
import {
  classificationSourceLabel,
  readRequestClassification,
} from "./request-classification.js";

function assertEqual(actual: unknown, expected: unknown, message: string) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `${message}\nexpected ${JSON.stringify(expected)}\nreceived ${JSON.stringify(actual)}`,
    );
  }
}

function assertDefined<T>(
  value: T | undefined,
  message: string,
): asserts value is T {
  if (value === undefined) {
    throw new Error(message);
  }
}

const request = {
  endpoint: "responses",
  inbound_req_method: "POST",
  inbound_req_url: "https://client.example/backend-api/codex/responses",
  inbound_resp_status: 200,
  outbound_req_method: "POST",
  outbound_req_url: "https://provider.example/v1/responses",
  outbound_resp_status: 200,
  provider_id: "codex",
  model: "gpt-5.6-sol",
  account_id: "account-1",
  params_json: { stream: true },
  ctx_json: {
    latency_ms: 5_036,
    latency_header_ms: 1_707,
    pipeline_id: "proxy",
    mode: "passthrough",
  },
  usage_json: {
    kind: "responses",
    input: 97_678,
    output: 166,
    total: 97_844,
    cache_read: 91_904,
    reasoning: 7,
  },
};

assertEqual(
  isLlmRequest(request),
  true,
  "known endpoint is classified as LLM traffic",
);
assertEqual(
  isLlmRequest({
    outbound_req_url: "https://example.test/v1/chat/completions",
    model: "gpt-test",
  }),
  true,
  "known model route is classified without usage",
);
assertEqual(
  isLlmRequest({ outbound_req_url: "https://example.test/messages" }),
  false,
  "an HTTP path alone does not turn arbitrary traffic into an LLM request",
);
assertEqual(
  isLlmRequest({ endpoint: "search", usage_json: null }),
  false,
  "ordinary HTTP traffic keeps the generic overview",
);
assertEqual(
  isLlmRequest({
    endpoint: "/backend-api/codex/responses/compact",
    inbound_req_url:
      "https://example.test/backend-api/codex/responses/compact?source=test",
    model: "gpt-test",
  }),
  true,
  "a routed compaction endpoint uses the LLM overview",
);
assertEqual(
  isLlmRequest({ inbound_req_url: "/v1/responses/compact" }),
  false,
  "a compaction path alone does not classify arbitrary HTTP traffic",
);

for (const source of [
  "endpoint",
  "request_field",
  "codex_prompt",
  "claude_code_prompt",
  "opencode_prompt",
] as const) {
  const classification = { purpose: "compaction", source };
  assertEqual(
    readRequestClassification({
      params_json: { request_classification: classification },
    }),
    classification,
    `${source} classification is read from decoded metadata`,
  );
  assertEqual(
    readRequestClassification({
      params_json: JSON.stringify({ request_classification: classification }),
    }),
    classification,
    `${source} classification is read from serialized metadata`,
  );
  assertEqual(
    readRequestClassification({ request_classification: classification }),
    classification,
    `${source} classification is read from a summary`,
  );
}
for (const params_json of [
  undefined,
  null,
  "invalid json",
  { stream: false },
  { request_classification: null },
  { request_classification: { purpose: "unknown", source: "endpoint" } },
  { request_classification: { purpose: "compaction", source: "unknown" } },
  { request_classification: { purpose: "compaction", source: "toString" } },
]) {
  assertEqual(
    readRequestClassification({ params_json }),
    undefined,
    "absent, malformed, or future classification remains unclassified",
  );
}
assertEqual(
  classificationSourceLabel("claude_code_prompt"),
  "Claude Code prompt",
  "the detection source label identifies the client",
);

const overview = buildLlmRequestOverview(request);
assertDefined(overview, "LLM overview is built");
assertEqual(
  overview.usage,
  {
    kind: "responses",
    input_tokens: 97_678,
    output_tokens: 166,
    total_tokens: 97_844,
    cache_read_tokens: 91_904,
    cache_write_tokens: undefined,
    reasoning_tokens: 7,
  },
  "normalized usage fields are preserved",
);
assertEqual(
  overview.streamed_ms,
  3_329,
  "streaming duration excludes first-response latency",
);
assertEqual(
  cacheReadPercent(overview.usage)?.toFixed(1),
  "94.1",
  "cache reuse is relative to normalized input",
);

const stringOverview = buildLlmRequestOverview({
  endpoint: "custom",
  usage_json: JSON.stringify({
    kind: "messages",
    input: 12,
    output: 3,
    cache_write: 4,
  }),
  params_json: JSON.stringify({ stream: false }),
  ctx_json: JSON.stringify({ latency_ms: 250 }),
});
assertDefined(stringOverview, "serialized JSON columns are accepted");
assertEqual(stringOverview.stream, false, "serialized stream mode is parsed");
const compactionOverview = buildLlmRequestOverview({
  endpoint: "responses",
  params_json: {
    stream: false,
    request_classification: { purpose: "compaction", source: "endpoint" },
  },
});
assertDefined(compactionOverview, "compaction overview is built");
assertEqual(
  compactionOverview.request_classification,
  { purpose: "compaction", source: "endpoint" },
  "the overview preserves purpose and detection source",
);
assertEqual(
  stringOverview.usage.cache_write_tokens,
  4,
  "serialized cache writes are parsed",
);
assertEqual(
  stringOverview.first_response_ms,
  undefined,
  "missing first-response latency stays absent",
);

assertEqual(
  cacheReadPercent({
    kind: undefined,
    input_tokens: 0,
    output_tokens: undefined,
    total_tokens: undefined,
    cache_read_tokens: 0,
    cache_write_tokens: undefined,
    reasoning_tokens: undefined,
  }),
  undefined,
  "zero input does not produce an invalid percentage",
);

console.log("llm-request-overview tests passed");
