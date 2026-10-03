export type ThemePreference = "system" | "light" | "dark";

export interface GatewayStatus {
  state: string;
  ownership: string;
  address: string;
  pid: number | null;
  last_error: string | null;
}
export interface RoutingDocument {
  config_path: string;
  revision: string;
  routing_toml: string;
  schema: string;
  overlay_paths: string[];
}
export interface AccountSummary {
  id: string;
  provider: string;
  label: string | null;
  enabled: boolean;
  tier: "active" | "fallback";
  username: string | null;
  credential_kind: string;
  credential_status: string;
  expires_at: number | null;
  last_refresh: number | null;
  can_refresh: boolean;
}
export interface UsageSummary {
  account: string | null;
  provider: string | null;
  model: string;
  requests: number;
  input_tokens: number;
  output_tokens: number;
  cached_tokens: number;
}
export interface RequestClassification {
  purpose: "compaction";
  source:
    | "endpoint"
    | "request_field"
    | "codex_prompt"
    | "claude_code_prompt"
    | "opencode_prompt";
}

export interface RequestSummary {
  row_id: string;
  day: string;
  request_id: string;
  ts: number;
  endpoint: string | null;
  request_classification: RequestClassification | null;
  status: number | null;
  request_error: string | null;
  session_id: string | null;
  account_id: string | null;
  provider_id: string | null;
  model: string | null;
  inbound_req_method: string | null;
  inbound_req_url: string | null;
  outbound_resp_status: number | null;
  inbound_resp_status: number | null;
}

export interface RequestDetail {
  row_id: string;
  day: string;
  request: Record<string, unknown>;
}

export interface RequestPayload {
  field: string;
  value: unknown;
}

export interface LlmMessageSummary {
  index: number;
  role: string;
  phase: string;
  kind: string;
  name: string | null;
  call_id: string | null;
  preview: string | null;
  truncated: boolean;
  content_bytes: number;
}

export interface LlmToolDefinitionSummary {
  index: number;
  name: string;
  kind: string;
  description: string | null;
  truncated: boolean;
  schema_bytes: number;
}

export interface LlmRequestContentSummary {
  messages: LlmMessageSummary[];
  tool_definitions: LlmToolDefinitionSummary[];
  warning: string | null;
}

export interface LlmItemDetail {
  index: number;
  value: unknown;
}

export interface RequestPage {
  requests: RequestSummary[];
  next_cursor: string | null;
}

export interface LatestRequests extends RequestPage {
  day: string | null;
}

export interface SessionSummary {
  session_id: string;
  source: string | null;
  first_ts: number;
  last_ts: number;
  request_count: number;
  last_request_day: string;
  last_request_id: string;
  endpoint: string | null;
  status: number | null;
  account_id: string | null;
  provider_id: string | null;
  model: string | null;
}

export interface SessionNodeSummary {
  node_id: string;
  parent_node_id: string | null;
  request_id: string;
  ts: number;
  endpoint: string;
  status: number | null;
  account_id: string | null;
  provider_id: string | null;
  model: string | null;
  reduction_kind: string;
  parent_source: string;
  common_prefix_messages: number;
  request_message_count: number;
  response_message_count: number;
  message_id: string | null;
  input_message_count: number;
  output_message_count: number;
  is_head: boolean;
}

export type SessionPartContent =
  | { encoding: "text"; value: string; truncated: boolean }
  | { encoding: "json"; value: unknown }
  | { encoding: "encrypted"; byte_length: number }
  | { encoding: "binary"; byte_length: number }
  | {
      encoding: "omitted";
      original_encoding: "text" | "json" | "binary" | "unknown";
      reason: "part_limit" | "aggregate_limit";
    };

export interface SessionPart {
  part_type: string;
  byte_length: number;
  content: SessionPartContent;
}

export interface SessionMessage {
  role: string;
  status: number | null;
  parts: SessionPart[];
  parts_total: number;
}

export interface SessionMessageTruncation {
  messages_total: number;
  messages_returned: number;
  messages_omitted_before: number;
  messages_omitted_after: number;
}

export interface SessionNodeTruncation {
  request_messages: SessionMessageTruncation;
  response_messages: SessionMessageTruncation;
  parts_total: number;
  parts_returned: number;
  parts_omitted: number;
  content_bytes_total: number;
  content_bytes_returned: number;
  content_parts_truncated: number;
  binary_parts_elided: number;
}

export interface SessionDetail {
  session: SessionSummary;
  head_node_id: string | null;
  nodes: SessionNodeSummary[];
  nodes_truncated: boolean;
}

export interface SessionUsage {
  session_id: string;
  request_count: number;
  requests_with_usage: number;
  input_tokens: number | null;
  output_tokens: number | null;
  total_tokens: number | null;
  cache_read_tokens: number | null;
  cache_write_tokens: number | null;
  reasoning_tokens: number | null;
  requests: SessionRequestUsage[];
}

export interface SessionRequestUsage {
  request_id: string;
  context_tokens: number | null;
  input_delta_tokens: number | null;
  output_tokens: number | null;
}

export interface SessionNodeDetail {
  node: SessionNodeSummary;
  request_messages: SessionMessage[];
  response_messages: SessionMessage[];
  truncation: SessionNodeTruncation;
}

export interface ViewerInfo {
  requests_dir: string;
  sessions_db: string;
  usage_db: string;
}

export type RequestDayState = "available" | "empty" | "unavailable";

export interface RequestDay {
  day: string;
  state: RequestDayState;
}

export interface RequestUrlPath {
  url_path: string;
  request_count: number;
}

export type ViewName = "requests" | "sessions";
export type LoadState = "idle" | "loading" | "ready" | "error";
export type DetailTab = "overview" | "client" | "provider" | "raw";
export type TimezoneMode = "local" | "utc";

export interface RequestFilters {
  query: string;
  provider_id: string;
  url_path: string;
  status: string;
  errors_only: boolean;
}

export interface RequestIdentity {
  day: string;
  request_id: string;
  row_id?: string;
}
export type InspectQuery =
  | { kind: "info" | "request_days" }
  | { kind: "request_url_paths"; day: string }
  | {
      kind: "requests";
      day?: string;
      limit?: number;
      cursor?: string;
      session_id?: string;
      provider_id?: string;
      url_path?: string;
      status?: number;
      errors_only?: boolean;
      query?: string;
    }
  | { kind: "latest_requests"; limit?: number; cursor?: string }
  | ({ kind: "request" | "request_llm_summary" } & RequestIdentity)
  | ({ kind: "request_payload"; field: string } & RequestIdentity)
  | ({
      kind: "request_llm_message" | "request_llm_tool_definition";
      index: number;
    } & RequestIdentity)
  | { kind: "sessions"; limit?: number }
  | { kind: "session" | "session_usage"; session_id: string; limit?: number }
  | { kind: "session_node"; session_id: string; node_id: string };

export type AccountActivation = "active" | "fallback" | "disabled";
export interface AccountProvider {
  id: string;
  device_login: boolean;
  api_key: boolean;
  refresh_token: boolean;
  sources: string[];
  default_flavor: "api_key" | "refresh_token";
}
export interface AccountProbe {
  checked_at: number;
  authentication: string;
  quota_status: string;
  plan: string | null;
  headline: string | null;
  reset_date: string | null;
  metered: { label: string; remaining: number; entitlement?: number } | null;
  secondary: {
    label: string;
    used?: number;
    total?: number;
    percent_used?: number;
    reset_at_ms?: number;
  }[];
  message: string | null;
}
export interface AccountImport {
  id: string;
  provider: string;
  source: string;
  value: string;
  flavor: "api_key" | "refresh_token";
}
export interface LoginTicket {
  login_id: string;
  user_code: string;
  verification_uri: string;
  expires_in: number;
}
export interface LoginProgress {
  login_id: string;
  phase: "waiting" | "saving" | "complete" | "ended";
}
export type AccountEdit =
  | {
      action: "update";
      id: string;
      label: string | null;
      activation: AccountActivation;
    }
  | { action: "remove"; id: string };
