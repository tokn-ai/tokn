import { LitElement, html, nothing } from "lit";
import type { PropertyValues } from "lit";
import { readInspector, isAbortError } from "./api";
import { displayPath } from "./format";
import type { LoadState, RequestPayload } from "./types";

const REDACTED = "••••••••";

function isSensitiveHeader(name: string): boolean {
  const normalized = name.toLowerCase().replaceAll("_", "-");
  return (
    normalized === "authorization" ||
    normalized === "proxy-authorization" ||
    normalized === "x-zcode-authorization" ||
    normalized === "cookie" ||
    normalized === "set-cookie" ||
    normalized.includes("api-key") ||
    normalized.includes("token") ||
    normalized.includes("secret")
  );
}

function redactHeaders(value: unknown): unknown {
  if (Array.isArray(value)) {
    if (
      value.length === 2 &&
      typeof value[0] === "string" &&
      isSensitiveHeader(value[0])
    ) {
      return [value[0], REDACTED];
    }
    return value.map((item) => redactHeaders(item));
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, child]) => [
        key,
        isSensitiveHeader(key) ? REDACTED : redactHeaders(child),
      ]),
    );
  }
  if (typeof value === "string") {
    try {
      return redactHeaders(JSON.parse(value) as unknown);
    } catch {
      return value.replace(
        /^([^:\r\n]+)(:\s*)(.*)$/gm,
        (line, name: string, separator: string) =>
          isSensitiveHeader(name.trim())
            ? `${name}${separator}${REDACTED}`
            : line,
      );
    }
  }
  return value;
}

function redactRecordHeaders(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => redactRecordHeaders(item));
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, child]) => [
        key,
        isHeaderContainer(key)
          ? redactHeaders(child)
          : redactRecordHeaders(child),
      ]),
    );
  }
  return value;
}

function isHeaderContainer(name: string): boolean {
  const normalized = name
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase()
    .replace(/[-\s]+/g, "_");
  return normalized === "headers" || normalized.endsWith("_headers");
}

function maskRecordPaths(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => maskRecordPaths(item));
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, child]) => [
        key,
        key.toLowerCase().endsWith("_url") && typeof child === "string"
          ? displayPath(child)
          : maskRecordPaths(child),
      ]),
    );
  }
  return value;
}

function formatPayload(value: unknown): string {
  if (typeof value === "string") {
    try {
      return JSON.stringify(JSON.parse(value) as unknown, null, 2);
    } catch {
      return value;
    }
  }
  return JSON.stringify(value, null, 2) ?? String(value);
}

function valueSummary(value: unknown): string {
  if (Array.isArray(value)) {
    return `${value.length} item${value.length === 1 ? "" : "s"}`;
  }
  if (value !== null && typeof value === "object") {
    const count = Object.keys(value).length;
    return `${count} field${count === 1 ? "" : "s"}`;
  }
  if (typeof value === "string") {
    return `${new Blob([value]).size.toLocaleString()} bytes`;
  }
  return typeof value;
}

export class PayloadPanel extends LitElement {
  static properties = {
    label: { type: String },
    value: { attribute: false },
    load_url: { type: String },
    is_headers: { type: Boolean },
    redact_record_headers: { type: Boolean },
    open: { type: Boolean, state: true },
    wrap: { type: Boolean, state: true },
    revealed: { type: Boolean, state: true },
    copy_state: { type: String, state: true },
    load_state: { type: String, state: true },
    loaded_value: { attribute: false, state: true },
    error_message: { type: String, state: true },
  };

  declare label: string;
  declare value: unknown;
  declare load_url: string | undefined;
  declare is_headers: boolean;
  declare redact_record_headers: boolean;
  declare open: boolean;
  declare wrap: boolean;
  declare revealed: boolean;
  declare copy_state: "idle" | "copied" | "error";
  declare load_state: LoadState;
  declare loaded_value: unknown;
  declare error_message: string | undefined;

  private load_controller: AbortController | undefined;
  private copy_timeout: number | undefined;

  constructor() {
    super();
    this.label = "Payload";
    this.is_headers = false;
    this.redact_record_headers = false;
    this.open = false;
    this.wrap = true;
    this.revealed = false;
    this.copy_state = "idle";
    this.load_state = "idle";
  }

  createRenderRoot() {
    return this;
  }

  disconnectedCallback() {
    this.load_controller?.abort();
    if (this.copy_timeout !== undefined) {
      window.clearTimeout(this.copy_timeout);
    }
    super.disconnectedCallback();
  }

  protected willUpdate(changed_properties: PropertyValues<this>) {
    if (
      !changed_properties.has("value") &&
      !changed_properties.has("load_url")
    ) {
      return;
    }
    this.load_controller?.abort();
    this.load_controller = undefined;
    if (this.copy_timeout !== undefined) {
      window.clearTimeout(this.copy_timeout);
      this.copy_timeout = undefined;
    }
    this.open = false;
    this.revealed = false;
    this.copy_state = "idle";
    this.load_state = "idle";
    this.loaded_value = undefined;
    this.error_message = undefined;
  }

  private effectiveValue(): unknown {
    return this.load_state === "ready" ? this.loaded_value : this.value;
  }

  private displayedValue(): string {
    const value = this.effectiveValue();
    const path_safe_value = this.redact_record_headers
      ? maskRecordPaths(value)
      : value;
    const safe_value = this.revealed
      ? path_safe_value
      : this.redact_record_headers
        ? redactRecordHeaders(path_safe_value)
        : this.is_headers
          ? redactHeaders(path_safe_value)
          : path_safe_value;
    return formatPayload(safe_value);
  }

  private toggleOpen(event: Event) {
    this.open = (event.currentTarget as HTMLDetailsElement).open;
    if (
      this.open &&
      this.value === undefined &&
      this.load_url &&
      this.load_state === "idle"
    ) {
      void this.loadPayload();
    }
  }

  private async loadPayload() {
    const load_url = this.load_url;
    if (!load_url) {
      return;
    }
    this.load_controller?.abort();
    const controller = new AbortController();
    this.load_controller = controller;
    this.load_state = "loading";
    this.error_message = undefined;
    try {
      const payload = await readInspector<RequestPayload>(
        load_url,
        controller.signal,
      );
      if (this.load_controller !== controller || this.load_url !== load_url) {
        return;
      }
      const expected_field = new URL(
        load_url,
        window.location.origin,
      ).searchParams.get("field");
      if (!expected_field || payload.field !== expected_field) {
        throw new Error("Payload response did not match the requested field");
      }
      this.loaded_value = payload.value;
      this.load_state = "ready";
    } catch (error) {
      if (this.load_controller !== controller || isAbortError(error)) {
        return;
      }
      this.load_state = "error";
      this.error_message =
        error instanceof Error ? error.message : "Unable to load payload";
    } finally {
      if (this.load_controller === controller) {
        this.load_controller = undefined;
      }
    }
  }

  private async copyValue() {
    try {
      await navigator.clipboard.writeText(this.displayedValue());
      this.copy_state = "copied";
      if (this.copy_timeout !== undefined) {
        window.clearTimeout(this.copy_timeout);
      }
      this.copy_timeout = window.setTimeout(() => {
        this.copy_state = "idle";
        this.copy_timeout = undefined;
      }, 1500);
    } catch {
      this.copy_state = "error";
    }
  }

  render() {
    if (
      !this.load_url &&
      (this.value === null || this.value === undefined || this.value === "")
    ) {
      return nothing;
    }
    const value = this.effectiveValue();
    const has_sensitive_headers = this.is_headers || this.redact_record_headers;
    const summary =
      this.load_state === "loading"
        ? "Loading…"
        : this.load_state === "error"
          ? "Load failed"
          : value === null
            ? "No payload"
            : value === undefined
              ? "Load on open"
              : valueSummary(value);
    return html`
      <details
        class="payload-panel"
        ?open=${this.open}
        @toggle=${this.toggleOpen}
      >
        <summary>
          <span>${this.label}</span>
          <span class="payload-summary">${summary}</span>
        </summary>
        ${
          this.open
            ? this.load_state === "loading"
              ? html`<div class="payload-state" role="status">
                  <span class="spinner" aria-hidden="true"></span>Loading
                  payload…
                </div>`
              : this.load_state === "error"
                ? html`
                    <div class="payload-state payload-error" role="alert">
                      <span>${this.error_message}</span>
                      <button
                        type="button"
                        @click=${() => void this.loadPayload()}
                      >
                        Retry
                      </button>
                    </div>
                  `
                : value === null || value === undefined || value === ""
                  ? html`<div class="payload-state">
                      No payload was persisted.
                    </div>`
                  : html`
                      <div class="payload-toolbar">
                        <button
                          type="button"
                          @click=${() => void this.copyValue()}
                        >
                          ${this.copy_state === "copied" ? "Copied" : this.copy_state === "error" ? "Copy failed" : "Copy"}
                        </button>
                        <button
                          type="button"
                          aria-pressed=${String(this.wrap)}
                          @click=${() => (this.wrap = !this.wrap)}
                        >
                          ${this.wrap ? "No wrap" : "Wrap"}
                        </button>
                        ${
                          has_sensitive_headers
                            ? html`
                                <button
                                  type="button"
                                  class=${this.revealed ? "danger-button" : ""}
                                  aria-pressed=${String(this.revealed)}
                                  @click=${() => (this.revealed = !this.revealed)}
                                >
                                  ${this.revealed ? "Hide sensitive" : "Reveal sensitive"}
                                </button>
                              `
                            : nothing
                        }
                        <span class="payload-security-note">
                          ${has_sensitive_headers && !this.revealed ? "Sensitive headers redacted" : ""}
                        </span>
                      </div>
                      <pre
                        class=${this.wrap ? "wrap" : "nowrap"}
                      ><code>${this.displayedValue()}</code></pre>
                    `
            : nothing
        }
      </details>
    `;
  }
}

customElements.define("payload-panel", PayloadPanel);
