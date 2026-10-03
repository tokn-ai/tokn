import { LitElement, html, nothing } from "lit";
import "./payload-panel";
import "./llm-request-overview";
import "./web-search-detail";
import {
  displayPath,
  formatTimestamp,
  numberField,
  shortId,
  stringField,
} from "./format";
import type {
  DetailTab,
  LoadState,
  RequestDetail,
  RequestSummary,
  TimezoneMode,
} from "./types";
import { isCodexWebSearchEndpoint } from "./web-search";
import { isLlmRequest } from "./llm-request";
import {
  classificationSourceLabel,
  readRequestClassification,
} from "./request-classification";

const DETAIL_TABS: { id: DetailTab; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "client", label: "Client" },
  { id: "provider", label: "Provider" },
  { id: "raw", label: "Raw" },
];

function displayValue(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  if (typeof value === "boolean") {
    return value ? "Yes" : "No";
  }
  return String(value);
}

function jsonRecord(value: unknown): Record<string, unknown> | undefined {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  if (typeof value === "string") {
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
  return undefined;
}

function nestedValue(
  request: Record<string, unknown>,
  container: string,
  field: string,
): unknown {
  return jsonRecord(request[container])?.[field] ?? request[field];
}

function payloadUrl(
  day: string,
  request_id: string,
  row_id: string,
  field: string,
): string {
  const params = new URLSearchParams({ day, request_id, row_id, field });
  return `/api/request-payload?${params.toString()}`;
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

export class RequestDetailView extends LitElement {
  static properties = {
    detail: { attribute: false },
    summary: { attribute: false },
    state: { type: String },
    error_message: { type: String },
    active_tab: { type: String },
    timezone: { type: String },
  };

  declare detail: RequestDetail | undefined;
  declare summary: RequestSummary | undefined;
  declare state: LoadState;
  declare error_message: string | undefined;
  declare active_tab: DetailTab;
  declare timezone: TimezoneMode;

  createRenderRoot() {
    return this;
  }

  private openSession(session_id: string) {
    this.dispatchEvent(
      new CustomEvent<string>("open-session", {
        detail: session_id,
        bubbles: true,
        composed: true,
      }),
    );
  }

  private retry() {
    this.dispatchEvent(
      new CustomEvent("detail-retry", { bubbles: true, composed: true }),
    );
  }

  private close() {
    this.dispatchEvent(
      new CustomEvent("detail-close", { bubbles: true, composed: true }),
    );
  }

  private selectTab(tab: DetailTab) {
    this.dispatchEvent(
      new CustomEvent<DetailTab>("detail-tab-change", {
        detail: tab,
        bubbles: true,
        composed: true,
      }),
    );
  }

  private tabKeydown(event: KeyboardEvent) {
    const current_index = DETAIL_TABS.findIndex(
      (tab) => tab.id === this.active_tab,
    );
    let next_index: number | undefined;
    if (event.key === "ArrowRight") {
      next_index = (current_index + 1) % DETAIL_TABS.length;
    } else if (event.key === "ArrowLeft") {
      next_index =
        (current_index - 1 + DETAIL_TABS.length) % DETAIL_TABS.length;
    } else if (event.key === "Home") {
      next_index = 0;
    } else if (event.key === "End") {
      next_index = DETAIL_TABS.length - 1;
    }
    if (next_index === undefined) {
      return;
    }
    event.preventDefault();
    const tab = DETAIL_TABS[next_index];
    this.selectTab(tab.id);
    const buttons = this.querySelectorAll<HTMLButtonElement>("[role=tab]");
    buttons[next_index]?.focus();
  }

  private renderOverview(request: Record<string, unknown>) {
    if (isLlmRequest(request) && this.detail) {
      return html`
        <llm-request-overview
          .request=${request}
          .day=${this.detail.day}
          .request_id=${stringField(request, "request_id") ?? this.summary?.request_id ?? ""}
          .row_id=${this.detail.row_id}
          .timezone=${this.timezone}
        ></llm-request-overview>
      `;
    }

    const ts = numberField(request, "ts");
    const latency = nestedValue(request, "ctx_json", "latency_ms");
    const stream = nestedValue(request, "params_json", "stream");
    const classification = readRequestClassification(request);
    const metadata = [
      [
        "Timestamp",
        ts === undefined ? undefined : formatTimestamp(ts, this.timezone),
      ],
      ["Storage day", this.detail?.day],
      ["Endpoint", request.endpoint],
      ["Model", request.model],
      ["Provider", request.provider_id],
      ["Account", request.account_id],
      ["Latency", typeof latency === "number" ? `${latency} ms` : latency],
      ["Streaming", stream],
    ];
    if (classification) {
      metadata.push(
        ["Purpose", "Compaction"],
        ["Detection source", classificationSourceLabel(classification.source)],
      );
    }
    const inbound_status = numberField(request, "inbound_resp_status");
    const outbound_status = numberField(request, "outbound_resp_status");
    const final_status = numberField(request, "status");
    const request_id =
      stringField(request, "request_id") ?? this.summary?.request_id;
    const row_id = this.detail?.row_id;
    const endpoint =
      stringField(request, "inbound_req_url") ??
      stringField(request, "endpoint");
    const search_detail =
      this.detail && request_id && row_id && isCodexWebSearchEndpoint(endpoint)
        ? html`
            <web-search-detail
              .request_url=${payloadUrl(this.detail.day, request_id, row_id, "inbound_req_body")}
              .response_url=${payloadUrl(this.detail.day, request_id, row_id, "inbound_resp_body")}
            ></web-search-detail>
          `
        : nothing;
    return html`
      <section class="flow-grid" aria-label="Request flow">
        <div>
          <span>Client request</span>
          <strong>${stringField(request, "inbound_req_method") ?? "—"}</strong>
        </div>
        <span class="flow-arrow" aria-hidden="true">→</span>
        <div>
          <span>Provider response</span>
          <strong class="status-text ${statusTone(outbound_status)}"
            >${displayValue(outbound_status)}</strong
          >
        </div>
        <span class="flow-arrow" aria-hidden="true">→</span>
        <div>
          <span>Client response</span>
          <strong
            class="status-text ${statusTone(inbound_status ?? final_status)}"
          >
            ${displayValue(inbound_status ?? final_status)}
          </strong>
        </div>
      </section>
      <dl class="metadata-grid">
        ${metadata.map(
          ([label, value]) => html`
            <div>
              <dt>${label}</dt>
              <dd title=${displayValue(value)}>${displayValue(value)}</dd>
            </div>
          `,
        )}
      </dl>
      ${search_detail}
      <div class="payload-stack">
        <payload-panel
          label="Usage"
          .value=${request.usage_json}
        ></payload-panel>
      </div>
    `;
  }

  private renderRaw(request: Record<string, unknown>) {
    return html`
      <p class="raw-note">
        Network headers and bodies remain lazy and are not included in this
        overview record.
      </p>
      <div class="payload-stack">
        <payload-panel
          label="Request parameters"
          .value=${request.params_json}
        ></payload-panel>
        <payload-panel
          label="Request context"
          .value=${request.ctx_json}
        ></payload-panel>
        <payload-panel
          label="Persisted overview record"
          .value=${request}
          .redact_record_headers=${true}
        ></payload-panel>
      </div>
    `;
  }

  private renderClient(
    request: Record<string, unknown>,
    day: string,
    request_id: string,
    row_id: string,
  ) {
    return html`
      <section class="payload-group">
        <div class="payload-group-heading">
          <div>
            <span class="direction-label">Incoming</span>
            <h3>Client request</h3>
          </div>
          <span
            >${stringField(request, "inbound_req_method") ?? "—"}
            ${displayPath(stringField(request, "inbound_req_url"))}</span
          >
        </div>
        <payload-panel
          label="Request headers"
          .value=${request.inbound_req_headers}
          .load_url=${payloadUrl(day, request_id, row_id, "inbound_req_headers")}
          .is_headers=${true}
        ></payload-panel>
        <payload-panel
          label="Request body"
          .value=${request.inbound_req_body}
          .load_url=${payloadUrl(day, request_id, row_id, "inbound_req_body")}
        ></payload-panel>
      </section>
      <section class="payload-group">
        <div class="payload-group-heading">
          <div>
            <span class="direction-label">Outgoing</span>
            <h3>Client response</h3>
          </div>
          <span
            >Status
            ${displayValue(request.inbound_resp_status ?? request.status)}</span
          >
        </div>
        <payload-panel
          label="Response headers"
          .value=${request.inbound_resp_headers}
          .load_url=${payloadUrl(day, request_id, row_id, "inbound_resp_headers")}
          .is_headers=${true}
        ></payload-panel>
        <payload-panel
          label="Response body"
          .value=${request.inbound_resp_body}
          .load_url=${payloadUrl(day, request_id, row_id, "inbound_resp_body")}
        ></payload-panel>
      </section>
    `;
  }

  private renderProvider(
    request: Record<string, unknown>,
    day: string,
    request_id: string,
    row_id: string,
  ) {
    return html`
      <section class="payload-group">
        <div class="payload-group-heading">
          <div>
            <span class="direction-label">Outgoing</span>
            <h3>Provider request</h3>
          </div>
          <span
            >${stringField(request, "outbound_req_method") ?? "—"}
            ${displayPath(stringField(request, "outbound_req_url"))}</span
          >
        </div>
        <payload-panel
          label="Request headers"
          .value=${request.outbound_req_headers}
          .load_url=${payloadUrl(day, request_id, row_id, "outbound_req_headers")}
          .is_headers=${true}
        ></payload-panel>
        <payload-panel
          label="Request body"
          .value=${request.outbound_req_body}
          .load_url=${payloadUrl(day, request_id, row_id, "outbound_req_body")}
        ></payload-panel>
      </section>
      <section class="payload-group">
        <div class="payload-group-heading">
          <div>
            <span class="direction-label">Incoming</span>
            <h3>Provider response</h3>
          </div>
          <span>Status ${displayValue(request.outbound_resp_status)}</span>
        </div>
        <payload-panel
          label="Response headers"
          .value=${request.outbound_resp_headers}
          .load_url=${payloadUrl(day, request_id, row_id, "outbound_resp_headers")}
          .is_headers=${true}
        ></payload-panel>
        <payload-panel
          label="Response body"
          .value=${request.outbound_resp_body}
          .load_url=${payloadUrl(day, request_id, row_id, "outbound_resp_body")}
        ></payload-panel>
      </section>
    `;
  }

  private renderTab(
    request: Record<string, unknown>,
    day: string,
    request_id: string,
    row_id: string,
  ) {
    switch (this.active_tab) {
      case "client":
        return this.renderClient(request, day, request_id, row_id);
      case "provider":
        return this.renderProvider(request, day, request_id, row_id);
      case "raw":
        return this.renderRaw(request);
      default:
        return this.renderOverview(request);
    }
  }

  render() {
    if (!this.detail) {
      if (this.state === "loading") {
        return html`
          <section class="detail-state" aria-live="polite">
            <button
              type="button"
              class="mobile-back-button"
              @click=${this.close}
            >
              ← Requests
            </button>
            <span class="spinner" aria-hidden="true"></span>
            <p>Loading request detail…</p>
          </section>
        `;
      }
      if (this.state === "error") {
        return html`
          <section class="detail-state error-state" role="alert">
            <button
              type="button"
              class="mobile-back-button"
              @click=${this.close}
            >
              ← Requests
            </button>
            <strong>Request detail could not be loaded</strong>
            <p>${this.error_message}</p>
            <button type="button" class="primary-button" @click=${this.retry}>
              Retry
            </button>
          </section>
        `;
      }
      return html`<section class="detail-state">
        <p>Select a request to inspect its route, payloads, and responses.</p>
      </section>`;
    }

    const request = this.detail.request;
    const request_id =
      stringField(request, "request_id") ??
      this.summary?.request_id ??
      "unknown id";
    const session_id =
      stringField(request, "session_id") ??
      this.summary?.session_id ??
      undefined;
    const method =
      stringField(request, "inbound_req_method") ??
      this.summary?.inbound_req_method ??
      "REQUEST";
    const endpoint = displayPath(
      stringField(request, "inbound_req_url") ??
        this.summary?.inbound_req_url ??
        stringField(request, "endpoint"),
    );
    return html`
      <section class="detail-content">
        <header class="detail-header">
          <button type="button" class="mobile-back-button" @click=${this.close}>
            ← Requests
          </button>
          <div class="detail-title">
            <p class="eyebrow">request · ${shortId(request_id)}</p>
            <h2><span>${method}</span> ${endpoint}</h2>
            <p class="muted" title=${request_id}>${request_id}</p>
          </div>
          <div class="detail-actions">
            ${
              session_id
                ? html`<button
                    type="button"
                    class="secondary-button"
                    @click=${() => this.openSession(session_id)}
                  >
                    Open session
                  </button>`
                : nothing
            }
            <button
              type="button"
              class="icon-button"
              aria-label="Refresh request detail"
              title="Refresh request detail"
              @click=${this.retry}
            >
              ↻
            </button>
          </div>
        </header>
        ${
          this.state === "loading"
            ? html`<div class="inline-state" role="status">
                <span class="spinner" aria-hidden="true"></span>Refreshing
                detail…
              </div>`
            : nothing
        }
        ${
          this.state === "error"
            ? html`
                <div class="inline-error" role="alert">
                  <span>${this.error_message}</span>
                  <button type="button" @click=${this.retry}>Retry</button>
                </div>
              `
            : nothing
        }
        ${request.request_error ? html`<div class="request-error" role="alert">${String(request.request_error)}</div>` : nothing}
        <div
          class="detail-tabs"
          role="tablist"
          aria-label="Request detail sections"
          @keydown=${this.tabKeydown}
        >
          ${DETAIL_TABS.map(
            (tab) => html`
              <button
                id="request-tab-${tab.id}"
                type="button"
                role="tab"
                aria-selected=${String(this.active_tab === tab.id)}
                aria-controls="request-panel-${tab.id}"
                tabindex=${this.active_tab === tab.id ? "0" : "-1"}
                @click=${() => this.selectTab(tab.id)}
              >
                ${tab.label}
              </button>
            `,
          )}
        </div>
        <section
          id="request-panel-${this.active_tab}"
          class="detail-tab-panel"
          role="tabpanel"
          aria-labelledby="request-tab-${this.active_tab}"
          tabindex="0"
        >
          ${this.renderTab(request, this.detail.day, request_id, this.detail.row_id)}
        </section>
      </section>
    `;
  }
}

customElements.define("request-detail-view", RequestDetailView);
