import { LitElement, html, nothing } from "lit";
import type { PropertyValues } from "lit";
import { readInspector, isAbortError } from "./api";
import { displayPath, formatTimestamp, numberField } from "./format";
import { buildLlmRequestOverview, cacheReadPercent } from "./llm-request";
import { classificationSourceLabel } from "./request-classification";
import "./llm-expandable-item";
import type {
  LlmMessageSummary,
  LlmRequestContentSummary,
  LoadState,
  TimezoneMode,
} from "./types";

const TOOL_CALL_KINDS = new Set([
  "custom_tool_call",
  "function_call",
  "tool_call",
]);
const TOOL_OUTPUT_KINDS = new Set([
  "custom_tool_call_output",
  "function_call_output",
  "tool_result",
]);

function displayValue(value: string | undefined): string {
  return value && value.length > 0 ? value : "—";
}

function formatTokens(value: number | undefined): string {
  return value === undefined ? "—" : value.toLocaleString();
}

function formatMilliseconds(value: number | undefined): string {
  if (value === undefined) {
    return "—";
  }
  if (value < 1_000) {
    return `${Math.round(value).toLocaleString()} ms`;
  }
  const seconds = value / 1_000;
  return `${seconds >= 10 ? seconds.toFixed(1) : seconds.toFixed(2)} s`;
}

function formatBytes(value: number): string {
  if (value < 1_000) {
    return `${value} B`;
  }
  if (value < 1_000_000) {
    return `${(value / 1_000).toFixed(1)} KB`;
  }
  return `${(value / 1_000_000).toFixed(1)} MB`;
}

function statusTone(status: number | undefined): string {
  if (status === undefined) {
    return "neutral";
  }
  if (status >= 400) {
    return "error";
  }
  if (status >= 300) {
    return "warning";
  }
  return "success";
}

function messageTitle(message: LlmMessageSummary): string {
  if (!message.name) {
    return message.role;
  }
  if (TOOL_OUTPUT_KINDS.has(message.kind)) {
    return `tool ← ${message.name}`;
  }
  if (TOOL_CALL_KINDS.has(message.kind)) {
    return `assistant → ${message.name}`;
  }
  return message.role;
}

function messageMeta(message: LlmMessageSummary): string {
  const call_id = message.call_id
    ? ` · call …${message.call_id.slice(-8)}`
    : "";
  return `${message.phase} · ${message.kind}${call_id}`;
}

function statusLabel(status: number | undefined): string {
  return status === undefined ? "—" : String(status);
}

export class LlmRequestOverview extends LitElement {
  static properties = {
    request: { attribute: false },
    day: { type: String },
    request_id: { type: String },
    row_id: { type: String },
    timezone: { type: String },
    content_state: { type: String, state: true },
    content_summary: { attribute: false, state: true },
    content_error: { type: String, state: true },
  };

  declare request: Record<string, unknown>;
  declare day: string;
  declare request_id: string;
  declare row_id: string;
  declare timezone: TimezoneMode;
  declare content_state: LoadState;
  declare content_summary: LlmRequestContentSummary | undefined;
  declare content_error: string | undefined;

  private content_controller: AbortController | undefined;

  constructor() {
    super();
    this.day = "";
    this.request_id = "";
    this.row_id = "";
    this.content_state = "idle";
  }

  createRenderRoot() {
    return this;
  }

  disconnectedCallback() {
    this.content_controller?.abort();
    super.disconnectedCallback();
  }

  protected updated(changed_properties: PropertyValues<this>) {
    if (
      changed_properties.has("day") ||
      changed_properties.has("request_id") ||
      changed_properties.has("row_id")
    ) {
      void this.loadContentSummary();
    }
  }

  private async loadContentSummary() {
    if (!this.day || !this.request_id || !this.row_id) {
      return;
    }
    const identity = `${this.day}:${this.request_id}:${this.row_id}`;
    this.content_controller?.abort();
    const controller = new AbortController();
    this.content_controller = controller;
    this.content_state = "loading";
    this.content_error = undefined;
    try {
      const params = new URLSearchParams({
        day: this.day,
        request_id: this.request_id,
        row_id: this.row_id,
      });
      const summary = await readInspector<LlmRequestContentSummary>(
        `/api/request-llm-summary?${params}`,
        controller.signal,
      );
      if (
        this.content_controller !== controller ||
        identity !== `${this.day}:${this.request_id}:${this.row_id}`
      ) {
        return;
      }
      this.content_summary = summary;
      this.content_state = "ready";
      await this.updateComplete;
      const message_list = this.querySelector<HTMLElement>(".llm-message-list");
      if (message_list) {
        message_list.scrollTop = message_list.scrollHeight;
      }
    } catch (error) {
      if (this.content_controller !== controller || isAbortError(error)) {
        return;
      }
      this.content_state = "error";
      this.content_error =
        error instanceof Error
          ? error.message
          : "Unable to load messages and tools";
    } finally {
      if (this.content_controller === controller) {
        this.content_controller = undefined;
      }
    }
  }

  private itemUrl(endpoint: string, index: number): string {
    const params = new URLSearchParams({
      day: this.day,
      request_id: this.request_id,
      row_id: this.row_id,
      index: String(index),
    });
    return `${endpoint}?${params}`;
  }

  private renderContentSummary() {
    if (this.content_state === "idle" || this.content_state === "loading") {
      return html`
        <section class="llm-content-state" aria-live="polite">
          <span class="spinner" aria-hidden="true"></span>
          <span>Loading messages and tools…</span>
        </section>
      `;
    }
    if (this.content_state === "error") {
      return html`
        <section class="llm-content-state error-state" role="alert">
          <div>
            <strong>Messages and tools could not be loaded</strong
            ><span>${this.content_error}</span>
          </div>
          <button type="button" @click=${() => void this.loadContentSummary()}>
            Retry
          </button>
        </section>
      `;
    }
    const summary = this.content_summary;
    if (!summary) {
      return nothing;
    }
    return html`
      ${summary.warning ? html`<p class="llm-content-warning">${summary.warning}</p>` : nothing}
      <div class="llm-content-grid">
        <section
          class="llm-content-panel"
          aria-labelledby="llm-messages-heading"
        >
          <header>
            <div>
              <p class="eyebrow">Conversation</p>
              <h3 id="llm-messages-heading">Messages</h3>
            </div>
            <span>${summary.messages.length} items · newest below</span>
          </header>
          <div
            class="llm-message-list"
            tabindex="0"
            aria-label="All conversation items in chronological order"
          >
            ${
              summary.messages.length === 0
                ? html`<p class="llm-content-empty">
                    No conversational messages recorded.
                  </p>`
                : summary.messages.map(
                    (message) => html`
                      <llm-expandable-item
                        .title=${messageTitle(message)}
                        .meta=${messageMeta(message)}
                        .preview=${message.preview ?? undefined}
                        .size_label=${formatBytes(message.content_bytes)}
                        .load_url=${this.itemUrl("/api/request-llm-message", message.index)}
                      ></llm-expandable-item>
                    `,
                  )
            }
          </div>
        </section>

        <section class="llm-content-panel" aria-labelledby="llm-tools-heading">
          <header>
            <div>
              <p class="eyebrow">Request capabilities</p>
              <h3 id="llm-tools-heading">Tool definitions</h3>
            </div>
            <span>${summary.tool_definitions.length} definitions</span>
          </header>
          <div class="llm-tool-definition-list">
            ${
              summary.tool_definitions.length === 0
                ? html`<p class="llm-content-empty">
                    No structured tool definitions were persisted.
                  </p>`
                : summary.tool_definitions.map(
                    (tool) => html`
                      <llm-expandable-item
                        .title=${tool.name}
                        .meta=${tool.kind}
                        .preview=${tool.description ?? undefined}
                        .size_label=${tool.schema_bytes > 0 ? `${formatBytes(tool.schema_bytes)} schema` : "No schema"}
                        .load_url=${this.itemUrl("/api/request-llm-tool-definition", tool.index)}
                      ></llm-expandable-item>
                    `,
                  )
            }
          </div>
        </section>
      </div>
    `;
  }

  render() {
    const overview = buildLlmRequestOverview(this.request);
    if (!overview) {
      return nothing;
    }

    const ts = numberField(this.request, "ts");
    const cache_percent = cacheReadPercent(overview.usage);
    const timing_total = overview.latency_ms ?? 0;
    const first_response_percent =
      timing_total > 0 && overview.first_response_ms !== undefined
        ? Math.min(100, (overview.first_response_ms / timing_total) * 100)
        : 0;
    const pipeline = [overview.pipeline, overview.mode]
      .filter(Boolean)
      .join(" · ");

    return html`
      <section class="llm-overview" aria-label="LLM request overview">
        <section class="llm-route-flow" aria-label="Model request route">
          <div class="llm-route-step">
            <span class="eyebrow">Client</span>
            <strong
              >${displayValue(overview.client_method)}
              ${displayPath(overview.client_url)}</strong
            >
            <small
              >Response
              <span class="status-text ${statusTone(overview.client_status)}"
                >${statusLabel(overview.client_status)}</span
              ></small
            >
          </div>
          <span class="llm-route-arrow" aria-hidden="true">→</span>
          <div class="llm-route-step llm-route-model">
            <span class="eyebrow">${displayValue(overview.provider_id)}</span>
            <strong>${displayValue(overview.model)}</strong>
            <small
              >${displayValue(overview.endpoint)}${overview.stream === undefined ? "" : overview.stream ? " · streaming" : " · buffered"}</small
            >
          </div>
          <span class="llm-route-arrow" aria-hidden="true">→</span>
          <div class="llm-route-step">
            <span class="eyebrow">Provider</span>
            <strong
              >${displayValue(overview.provider_method)}
              ${displayPath(overview.provider_url)}</strong
            >
            <small
              >Response
              <span class="status-text ${statusTone(overview.provider_status)}"
                >${statusLabel(overview.provider_status)}</span
              ></small
            >
          </div>
        </section>

        <div class="llm-metrics-grid">
          <section
            class="llm-metric-panel llm-token-panel"
            aria-labelledby="llm-token-heading"
          >
            <header>
              <div>
                <p class="eyebrow">
                  ${overview.usage.kind ? `${overview.usage.kind} usage` : "Usage"}
                </p>
                <h3 id="llm-token-heading">Token usage</h3>
              </div>
              ${
                cache_percent === undefined
                  ? nothing
                  : html`<span
                      >${cache_percent.toFixed(1)}% of input cached</span
                    >`
              }
            </header>
            <dl class="llm-token-grid">
              <div class="llm-primary-metric">
                <dt>Total</dt>
                <dd>${formatTokens(overview.usage.total_tokens)}</dd>
              </div>
              <div>
                <dt>Input</dt>
                <dd>${formatTokens(overview.usage.input_tokens)}</dd>
              </div>
              <div>
                <dt>Output</dt>
                <dd>${formatTokens(overview.usage.output_tokens)}</dd>
              </div>
              <div>
                <dt>Cache read</dt>
                <dd>${formatTokens(overview.usage.cache_read_tokens)}</dd>
              </div>
              ${
                overview.usage.cache_write_tokens === undefined
                  ? nothing
                  : html`<div>
                      <dt>Cache write</dt>
                      <dd>
                        ${formatTokens(overview.usage.cache_write_tokens)}
                      </dd>
                    </div>`
              }
              <div>
                <dt>Reasoning</dt>
                <dd>${formatTokens(overview.usage.reasoning_tokens)}</dd>
              </div>
            </dl>
          </section>

          <section
            class="llm-metric-panel llm-timing-panel"
            aria-labelledby="llm-timing-heading"
          >
            <header>
              <div>
                <p class="eyebrow">Performance</p>
                <h3 id="llm-timing-heading">Response timing</h3>
              </div>
              <span
                >${overview.stream ? "Streamed" : overview.stream === false ? "Buffered" : "Mode unknown"}</span
              >
            </header>
            ${
              overview.latency_ms !== undefined &&
              overview.first_response_ms !== undefined
                ? html`
                    <div
                      class="llm-timing-bar"
                      title="First response ${formatMilliseconds(overview.first_response_ms)} of ${formatMilliseconds(overview.latency_ms)} total"
                    >
                      <span style=${`width: ${first_response_percent}%`}></span>
                    </div>
                  `
                : nothing
            }
            <dl class="llm-timing-grid">
              <div>
                <dt>First response</dt>
                <dd>${formatMilliseconds(overview.first_response_ms)}</dd>
              </div>
              ${
                overview.stream && overview.streamed_ms !== undefined
                  ? html`<div>
                      <dt>Streaming</dt>
                      <dd>${formatMilliseconds(overview.streamed_ms)}</dd>
                    </div>`
                  : nothing
              }
              <div class="llm-primary-metric">
                <dt>Total</dt>
                <dd>${formatMilliseconds(overview.latency_ms)}</dd>
              </div>
            </dl>
          </section>
        </div>

        ${this.renderContentSummary()}

        <dl class="metadata-grid llm-metadata-grid">
          ${
            overview.request_classification
              ? html`
                  <div>
                    <dt>Purpose</dt>
                    <dd>Compaction</dd>
                  </div>
                  <div>
                    <dt>Detection source</dt>
                    <dd>
                      ${classificationSourceLabel(overview.request_classification.source)}
                    </dd>
                  </div>
                `
              : nothing
          }
          <div>
            <dt>Timestamp</dt>
            <dd>
              ${ts === undefined ? "—" : formatTimestamp(ts, this.timezone)}
            </dd>
          </div>
          <div>
            <dt>Storage day</dt>
            <dd>${this.day}</dd>
          </div>
          <div>
            <dt>Account</dt>
            <dd title=${displayValue(overview.account_id)}>
              ${displayValue(overview.account_id)}
            </dd>
          </div>
          <div>
            <dt>Pipeline</dt>
            <dd title=${pipeline || "—"}>${pipeline || "—"}</dd>
          </div>
        </dl>
      </section>
    `;
  }
}

customElements.define("llm-request-overview", LlmRequestOverview);
