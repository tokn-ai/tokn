import { LitElement, html, nothing } from "lit";
import {
  formatTimestamp,
  requestKey,
  requestOutcome,
  requestPath,
  shortId,
} from "./format";
import type { RequestSummary, TimezoneMode } from "./types";
import { classificationSourceLabel } from "./request-classification";

export class RequestList extends LitElement {
  static properties = {
    requests: { attribute: false },
    selected_key: { type: String },
    timezone: { type: String },
  };

  declare requests: RequestSummary[];
  declare selected_key: string | undefined;
  declare timezone: TimezoneMode;

  createRenderRoot() {
    return this;
  }

  private selectRequest(request: RequestSummary) {
    this.dispatchEvent(
      new CustomEvent<RequestSummary>("request-select", {
        detail: request,
        bubbles: true,
        composed: true,
      }),
    );
  }

  render() {
    const requests = this.requests ?? [];
    if (requests.length === 0) {
      return html`<p class="empty">
        No persisted requests match these filters.
      </p>`;
    }
    return html`
      <ul class="request-list" aria-label="Requests">
        ${requests.map((request) => {
          const outcome = requestOutcome(request);
          const selected = this.selected_key === requestKey(request);
          const method = request.inbound_req_method ?? "REQUEST";
          const path = requestPath(request);
          return html`
            <li>
              <button
                type="button"
                class="request-row ${selected ? "selected" : ""}"
                data-request-key=${requestKey(request)}
                aria-current=${selected ? "true" : "false"}
                @click=${() => this.selectRequest(request)}
              >
                <span class="request-row-time"
                  >${formatTimestamp(request.ts, this.timezone, true)}</span
                >
                <span class="status ${outcome.tone}" title=${outcome.title}
                  >${outcome.label}</span
                >
                <span class="request-row-main">
                  <span class="request-route"
                    ><strong>${method}</strong><span>${path}</span></span
                  >
                  <span class="request-context">
                    ${
                      request.request_classification?.purpose === "compaction"
                        ? html`<span
                            class="request-classification-badge"
                            title=${classificationSourceLabel(request.request_classification.source)}
                            >Compaction</span
                          >`
                        : nothing
                    }
                    <span>${request.model ?? "unknown model"}</span>
                    <span aria-hidden="true">·</span>
                    <span>${request.provider_id ?? "unknown provider"}</span>
                  </span>
                  <span class="request-identifiers">
                    <span title=${request.request_id}
                      >req ${shortId(request.request_id)}</span
                    >
                    ${
                      request.session_id
                        ? html`<span title=${request.session_id}
                            >session ${shortId(request.session_id)}</span
                          >`
                        : html`<span>no session</span>`
                    }
                  </span>
                </span>
              </button>
            </li>
          `;
        })}
      </ul>
    `;
  }
}

customElements.define("request-list", RequestList);
