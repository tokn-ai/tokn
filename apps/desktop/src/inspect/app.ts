import inspectorStyles from "./styles.css?inline";
import { LitElement, html, nothing, unsafeCSS } from "lit";
import { InspectorError, readInspector, isAbortError } from "./api";
import { eventDetail, requestKey } from "./format";
import "./request-detail";
import "./request-list";
import "./session-views";
import type {
  DetailTab,
  LatestRequests,
  LoadState,
  RequestDay,
  RequestDetail,
  RequestFilters,
  RequestPage,
  RequestSummary,
  RequestUrlPath,
  SessionDetail,
  SessionNodeDetail,
  SessionNodeSummary,
  SessionSummary,
  SessionUsage,
  TimezoneMode,
  ViewerInfo,
  ViewName,
} from "./types";

const PAGE_LIMIT = 100;

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

function isDetailTab(value: string | null): value is DetailTab {
  return (
    value === "overview" ||
    value === "client" ||
    value === "provider" ||
    value === "raw"
  );
}

function emptyFilters(): RequestFilters {
  return {
    query: "",
    provider_id: "",
    url_path: "",
    status: "",
    errors_only: false,
  };
}

function requestDay(ts: number): string {
  return new Date(ts).toISOString().slice(0, 10);
}

type HistoryMode = "push" | "replace" | null;

class InspectApp extends LitElement {
  static properties = {
    active_view: { type: String },
    info: { attribute: false },
    requests: { attribute: false },
    request_days: { attribute: false },
    selected_day: { type: String },
    selected_request: { attribute: false },
    selected_request_id: { type: String },
    selected_request_row_id: { type: String },
    selected_request_detail: { attribute: false },
    request_list_state: { type: String },
    request_list_error: { type: String },
    request_detail_state: { type: String },
    request_detail_error: { type: String },
    next_cursor: { type: String },
    loading_more: { type: Boolean },
    load_more_error: { type: String },
    search_query: { type: String },
    provider_id: { type: String },
    url_path: { type: String },
    request_url_paths: { attribute: false },
    request_url_paths_loading: { type: Boolean },
    request_url_paths_error: { type: String },
    status_filter: { type: String },
    errors_only: { type: Boolean },
    applied_filters: { attribute: false },
    active_detail_tab: { type: String },
    timezone: { type: String },
    request_days_loading: { type: Boolean },
    request_days_error: { type: String },
    sessions: { attribute: false },
    selected_session: { attribute: false },
    selected_session_detail: { attribute: false },
    selected_session_usage: { attribute: false },
    sessions_loading: { type: Boolean },
    sessions_error: { type: String },
    session_search_query: { type: String },
    session_detail_state: { type: String },
    session_detail_error: { type: String },
    session_usage_state: { type: String },
    session_usage_error: { type: String },
    selected_session_node_id: { type: String },
    selected_session_node_detail: { attribute: false },
    session_node_state: { type: String },
    session_node_error: { type: String },
  };

  declare active_view: ViewName;
  declare info: ViewerInfo | undefined;
  declare requests: RequestSummary[];
  declare request_days: RequestDay[];
  declare selected_day: string | undefined;
  declare selected_request: RequestSummary | undefined;
  declare selected_request_id: string | undefined;
  declare selected_request_row_id: string | undefined;
  declare selected_request_detail: RequestDetail | undefined;
  declare request_list_state: LoadState;
  declare request_list_error: string | undefined;
  declare request_detail_state: LoadState;
  declare request_detail_error: string | undefined;
  declare next_cursor: string | undefined;
  declare loading_more: boolean;
  declare load_more_error: string | undefined;
  declare search_query: string;
  declare provider_id: string;
  declare url_path: string;
  declare request_url_paths: RequestUrlPath[];
  declare request_url_paths_loading: boolean;
  declare request_url_paths_error: string | undefined;
  declare status_filter: string;
  declare errors_only: boolean;
  declare applied_filters: RequestFilters;
  declare active_detail_tab: DetailTab;
  declare timezone: TimezoneMode;
  declare request_days_loading: boolean;
  declare request_days_error: string | undefined;
  declare sessions: SessionSummary[];
  declare selected_session: SessionSummary | undefined;
  declare selected_session_detail: SessionDetail | undefined;
  declare selected_session_usage: SessionUsage | undefined;
  declare sessions_loading: boolean;
  declare sessions_error: string | undefined;
  declare session_search_query: string;
  declare session_detail_state: LoadState;
  declare session_detail_error: string | undefined;
  declare session_usage_state: LoadState;
  declare session_usage_error: string | undefined;
  declare selected_session_node_id: string | undefined;
  declare selected_session_node_detail: SessionNodeDetail | undefined;
  declare session_node_state: LoadState;
  declare session_node_error: string | undefined;

  private request_load_id = 0;
  private request_detail_load_id = 0;
  private session_detail_load_id = 0;
  private session_usage_load_id = 0;
  private session_node_load_id = 0;
  private session_list_load_id = 0;
  private request_days_load_id = 0;
  private request_url_paths_load_id = 0;
  private sessions_loaded = false;
  private requested_request_id: string | undefined;
  private requested_request_row_id: string | undefined;
  private requested_session_id: string | undefined;
  private requested_session_node_id: string | undefined;
  private request_rows_context: string | undefined;
  private request_controller: AbortController | undefined;
  private request_url_paths_controller: AbortController | undefined;
  private request_detail_controller: AbortController | undefined;
  private session_list_controller: AbortController | undefined;
  private session_list_load: Promise<boolean> | undefined;
  private session_detail_controller: AbortController | undefined;
  private session_usage_controller: AbortController | undefined;
  private session_node_controller: AbortController | undefined;
  private navigation_workflow_id = 0;
  private readonly popstate_handler = () => void this.restoreFromHistory();

  constructor() {
    super();
    this.active_view = "requests";
    this.requests = [];
    this.request_days = [];
    this.sessions = [];
    this.request_list_state = "idle";
    this.request_detail_state = "idle";
    this.search_query = "";
    this.provider_id = "";
    this.url_path = "";
    this.request_url_paths = [];
    this.request_url_paths_loading = false;
    this.status_filter = "";
    this.errors_only = false;
    this.applied_filters = emptyFilters();
    this.active_detail_tab = "overview";
    this.timezone = "local";
    this.loading_more = false;
    this.request_days_loading = false;
    this.sessions_loading = false;
    this.session_search_query = "";
    this.session_detail_state = "idle";
    this.session_usage_state = "idle";
    this.session_node_state = "idle";
  }

  static styles = unsafeCSS(inspectorStyles);

  connectedCallback() {
    super.connectedCallback();
    this.restoreUrlState();
    window.addEventListener("popstate", this.popstate_handler);
    void this.loadInitialData();
  }

  disconnectedCallback() {
    window.removeEventListener("popstate", this.popstate_handler);
    this.request_controller?.abort();
    this.request_url_paths_controller?.abort();
    this.request_detail_controller?.abort();
    this.session_list_controller?.abort();
    this.session_detail_controller?.abort();
    this.session_usage_controller?.abort();
    this.session_node_controller?.abort();
    super.disconnectedCallback();
  }

  private restoreUrlState() {
    const params = new URLSearchParams(window.location.search);
    this.active_view =
      params.get("view") === "sessions" ? "sessions" : "requests";
    const day = params.get("day");
    this.selected_day =
      day && /^\d{4}-\d{2}-\d{2}$/.test(day) ? day : undefined;
    this.search_query = params.get("query") ?? "";
    this.provider_id = params.get("provider_id") ?? "";
    this.url_path = params.get("url_path") ?? "";
    const status = params.get("status") ?? "";
    this.status_filter = /^\d{3}$/.test(status) ? status : "";
    this.errors_only =
      params.get("errors_only") === "true" || params.get("errors_only") === "1";
    this.applied_filters = {
      query: this.search_query,
      provider_id: this.provider_id,
      url_path: this.url_path,
      status: this.status_filter,
      errors_only: this.errors_only,
    };
    this.requested_request_id = params.get("request_id") ?? undefined;
    const row_id = params.get("row_id");
    this.requested_request_row_id =
      row_id && /^-?\d+$/.test(row_id) ? row_id : undefined;
    const tab = params.get("tab");
    this.active_detail_tab = isDetailTab(tab) ? tab : "overview";
    this.requested_session_id = params.has("session_id")
      ? (params.get("session_id") ?? "")
      : undefined;
    this.requested_session_node_id = params.get("node_id") ?? undefined;
    this.timezone = params.get("timezone") === "utc" ? "utc" : "local";
  }

  private selectedRequestDay(): string | undefined {
    return (
      this.selected_request_detail?.day ??
      this.selected_request?.day ??
      this.selected_day
    );
  }

  private syncUrl(mode: Exclude<HistoryMode, null> = "replace") {
    const params = new URLSearchParams();
    if (this.active_view === "sessions") {
      params.set("view", "sessions");
      const session_id =
        this.selected_session?.session_id ?? this.requested_session_id;
      if (session_id !== undefined) {
        params.set("session_id", session_id);
      }
      if (this.selected_session_node_id) {
        params.set("node_id", this.selected_session_node_id);
      }
    } else {
      const day = this.selected_request_id
        ? this.selectedRequestDay()
        : this.selected_day;
      if (day) {
        params.set("day", day);
      }
      if (this.applied_filters.query) {
        params.set("query", this.applied_filters.query);
      }
      if (this.applied_filters.provider_id) {
        params.set("provider_id", this.applied_filters.provider_id);
      }
      if (this.applied_filters.url_path) {
        params.set("url_path", this.applied_filters.url_path);
      }
      if (this.applied_filters.status) {
        params.set("status", this.applied_filters.status);
      }
      if (this.applied_filters.errors_only) {
        params.set("errors_only", "true");
      }
      if (this.selected_request_id) {
        params.set("request_id", this.selected_request_id);
        if (this.selected_request_row_id) {
          params.set("row_id", this.selected_request_row_id);
        }
        params.set("tab", this.active_detail_tab);
      }
    }
    params.set("timezone", this.timezone);
    const query = params.toString();
    const url = `${window.location.pathname}${query ? `?${query}` : ""}`;
    if (`${window.location.pathname}${window.location.search}` === url) {
      return;
    }
    if (mode === "push") {
      window.history.pushState(null, "", url);
    } else {
      window.history.replaceState(null, "", url);
    }
  }

  private async loadInitialData() {
    const workflow_id = ++this.navigation_workflow_id;
    void this.loadInfo();
    await this.loadUrlState(workflow_id);
  }

  private async restoreFromHistory() {
    const workflow_id = ++this.navigation_workflow_id;
    this.request_controller?.abort();
    this.request_detail_controller?.abort();
    this.session_detail_controller?.abort();
    this.session_node_controller?.abort();
    this.resetRequestSelection();
    this.resetSessionSelection();
    this.restoreUrlState();
    if (this.active_view === "requests") {
      this.requests = [];
      this.next_cursor = undefined;
      this.request_rows_context = undefined;
    }
    await this.loadUrlState(workflow_id);
  }

  private async loadUrlState(workflow_id: number) {
    const requested_request_id = this.requested_request_id;
    const requested_request_row_id = this.requested_request_row_id;
    if (this.active_view === "sessions") {
      const requested_session_id = this.requested_session_id;
      const requested_node_id = this.requested_session_node_id;
      const loaded = await this.ensureSessionsLoaded();
      if (
        !loaded ||
        workflow_id !== this.navigation_workflow_id ||
        requested_session_id === undefined
      ) {
        return;
      }
      await this.loadSession(
        requested_session_id,
        this.sessions.find(
          (session) => session.session_id === requested_session_id,
        ),
        false,
        null,
        requested_node_id,
      );
      return;
    }
    void this.loadRequestDays();
    let loaded: boolean;
    if (this.selected_day) {
      void this.loadRequestUrlPaths(this.selected_day);
      loaded = await this.loadRequests();
    } else {
      loaded = await this.loadLatestRequests();
      if (loaded && this.selected_day) {
        void this.loadRequestUrlPaths(this.selected_day);
      }
      if (loaded && this.selected_day && this.hasAppliedFilters()) {
        loaded = await this.loadRequests();
      }
    }
    if (!loaded || workflow_id !== this.navigation_workflow_id) {
      return;
    }
    if (requested_request_id && this.selected_day) {
      const summary = this.requests.find(
        (request) =>
          request.request_id === requested_request_id &&
          (!requested_request_row_id ||
            request.row_id === requested_request_row_id),
      );
      await this.loadRequestDetail(
        this.selected_day,
        requested_request_id,
        requested_request_row_id ?? summary?.row_id,
        summary,
        false,
        null,
      );
    }
  }

  private async loadInfo() {
    try {
      this.info = await readInspector<ViewerInfo>("/api/info");
    } catch {
      this.info = undefined;
    }
  }

  private async loadLatestRequests(): Promise<boolean> {
    this.request_controller?.abort();
    const controller = new AbortController();
    this.request_controller = controller;
    const load_id = ++this.request_load_id;
    this.requests = [];
    this.next_cursor = undefined;
    this.request_rows_context = undefined;
    this.request_list_state = "loading";
    this.request_list_error = undefined;
    try {
      const page = await readInspector<LatestRequests>(
        `/api/requests/latest?limit=${PAGE_LIMIT}`,
        controller.signal,
      );
      if (
        load_id !== this.request_load_id ||
        this.request_controller !== controller
      ) {
        return false;
      }
      this.selected_day = page.day ?? undefined;
      this.requests = page.requests;
      this.next_cursor = page.next_cursor ?? undefined;
      this.request_rows_context = this.requestContext(
        this.selected_day,
        emptyFilters(),
      );
      this.request_list_state = "ready";
      this.syncUrl();
      return true;
    } catch (error) {
      if (load_id === this.request_load_id && !isAbortError(error)) {
        this.request_list_state = "error";
        this.request_list_error = errorMessage(
          error,
          "Unable to load recent requests",
        );
      }
      return false;
    } finally {
      if (this.request_controller === controller) {
        this.request_controller = undefined;
      }
    }
  }

  private requestContext(
    day = this.selected_day,
    filters = this.applied_filters,
  ): string | undefined {
    return day
      ? JSON.stringify([
          day,
          filters.query,
          filters.provider_id,
          filters.url_path,
          filters.status,
          filters.errors_only,
        ])
      : undefined;
  }

  private requestParams(
    day: string,
    filters: RequestFilters,
    cursor?: string,
  ): URLSearchParams {
    const params = new URLSearchParams({ day, limit: String(PAGE_LIMIT) });
    if (filters.query) {
      params.set("query", filters.query);
    }
    if (filters.provider_id) {
      params.set("provider_id", filters.provider_id);
    }
    if (filters.url_path) {
      params.set("url_path", filters.url_path);
    }
    if (filters.status) {
      params.set("status", filters.status);
    }
    if (filters.errors_only) {
      params.set("errors_only", "true");
    }
    if (cursor) {
      params.set("cursor", cursor);
    }
    return params;
  }

  private async loadRequests(append = false): Promise<boolean> {
    const day = this.selected_day;
    if (!day) {
      this.request_list_state = "idle";
      this.requests = [];
      this.next_cursor = undefined;
      this.request_rows_context = undefined;
      return false;
    }
    const filters = { ...this.applied_filters };
    const context = this.requestContext(day, filters);
    const cursor = append ? this.next_cursor : undefined;
    if (append && (!cursor || this.request_rows_context !== context)) {
      return false;
    }
    this.request_controller?.abort();
    const controller = new AbortController();
    this.request_controller = controller;
    const load_id = ++this.request_load_id;
    if (append) {
      this.loading_more = true;
      this.load_more_error = undefined;
    } else {
      this.loading_more = false;
      if (this.request_rows_context !== context) {
        this.requests = [];
        this.next_cursor = undefined;
        this.request_rows_context = undefined;
      }
      this.request_list_state = "loading";
      this.request_list_error = undefined;
      this.load_more_error = undefined;
    }
    try {
      const page = await readInspector<RequestPage>(
        `/api/requests?${this.requestParams(day, filters, cursor).toString()}`,
        controller.signal,
      );
      if (
        load_id !== this.request_load_id ||
        this.request_controller !== controller ||
        this.requestContext() !== context
      ) {
        return false;
      }
      if (append) {
        const existing = new Set(
          this.requests.map((request) => requestKey(request)),
        );
        this.requests = [
          ...this.requests,
          ...page.requests.filter(
            (request) => !existing.has(requestKey(request)),
          ),
        ];
      } else {
        this.requests = page.requests;
      }
      this.next_cursor = page.next_cursor ?? undefined;
      this.request_rows_context = context;
      this.request_list_state = "ready";
      return true;
    } catch (error) {
      if (load_id !== this.request_load_id || isAbortError(error)) {
        return false;
      }
      if (error instanceof InspectorError && error.status === 503) {
        this.markRequestDayUnavailable(day);
      }
      if (append) {
        this.load_more_error = errorMessage(
          error,
          "Unable to load more requests",
        );
      } else {
        this.request_list_state = "error";
        this.request_list_error = errorMessage(
          error,
          "Unable to load requests",
        );
      }
      return false;
    } finally {
      if (load_id === this.request_load_id) {
        this.loading_more = false;
      }
      if (this.request_controller === controller) {
        this.request_controller = undefined;
      }
    }
  }

  private async loadRequestDays() {
    const load_id = ++this.request_days_load_id;
    this.request_days_loading = true;
    this.request_days_error = undefined;
    try {
      const request_days =
        await readInspector<RequestDay[]>("/api/request-days");
      if (load_id === this.request_days_load_id) {
        this.request_days = request_days;
      }
    } catch (error) {
      if (load_id === this.request_days_load_id) {
        this.request_days_error = errorMessage(
          error,
          "Unable to load request day states",
        );
      }
    } finally {
      if (load_id === this.request_days_load_id) {
        this.request_days_loading = false;
      }
    }
  }

  private async loadRequestUrlPaths(day: string) {
    this.request_url_paths_controller?.abort();
    const controller = new AbortController();
    this.request_url_paths_controller = controller;
    const load_id = ++this.request_url_paths_load_id;
    this.request_url_paths_loading = true;
    this.request_url_paths_error = undefined;
    try {
      const params = new URLSearchParams({ day });
      const paths = await readInspector<RequestUrlPath[]>(
        `/api/request-url-paths?${params.toString()}`,
        controller.signal,
      );
      if (
        load_id === this.request_url_paths_load_id &&
        this.selected_day === day
      ) {
        this.request_url_paths = paths;
      }
    } catch (error) {
      if (load_id === this.request_url_paths_load_id && !isAbortError(error)) {
        this.request_url_paths = [];
        this.request_url_paths_error = errorMessage(
          error,
          "Unable to load URL paths",
        );
      }
    } finally {
      if (load_id === this.request_url_paths_load_id) {
        this.request_url_paths_loading = false;
      }
      if (this.request_url_paths_controller === controller) {
        this.request_url_paths_controller = undefined;
      }
    }
  }

  private markRequestDayUnavailable(day: string) {
    if (this.request_days.some((candidate) => candidate.day === day)) {
      this.request_days = this.request_days.map((candidate) =>
        candidate.day === day
          ? { ...candidate, state: "unavailable" }
          : candidate,
      );
    } else {
      this.request_days = [{ day, state: "unavailable" }, ...this.request_days];
    }
  }

  private resetRequestSelection() {
    this.request_detail_controller?.abort();
    this.request_detail_controller = undefined;
    this.request_detail_load_id += 1;
    this.selected_request = undefined;
    this.selected_request_id = undefined;
    this.selected_request_row_id = undefined;
    this.selected_request_detail = undefined;
    this.request_detail_state = "idle";
    this.request_detail_error = undefined;
    this.active_detail_tab = "overview";
  }

  private resetSessionSelection() {
    this.session_detail_controller?.abort();
    this.session_usage_controller?.abort();
    this.session_node_controller?.abort();
    this.session_detail_controller = undefined;
    this.session_usage_controller = undefined;
    this.session_node_controller = undefined;
    this.session_detail_load_id += 1;
    this.session_usage_load_id += 1;
    this.session_node_load_id += 1;
    this.requested_session_id = undefined;
    this.requested_session_node_id = undefined;
    this.selected_session = undefined;
    this.selected_session_detail = undefined;
    this.selected_session_usage = undefined;
    this.selected_session_node_id = undefined;
    this.selected_session_node_detail = undefined;
    this.session_detail_state = "idle";
    this.session_detail_error = undefined;
    this.session_usage_state = "idle";
    this.session_usage_error = undefined;
    this.session_node_state = "idle";
    this.session_node_error = undefined;
  }

  private async closeRequestDetail() {
    const selected_key =
      this.selected_request_row_id && this.selectedRequestDay()
        ? requestKey({
            day: this.selectedRequestDay()!,
            row_id: this.selected_request_row_id,
          })
        : undefined;
    ++this.navigation_workflow_id;
    this.resetRequestSelection();
    this.syncUrl("push");
    if (!selected_key || !window.matchMedia("(max-width: 680px)").matches) {
      return;
    }
    await this.updateComplete;
    const rows = this.renderRoot.querySelectorAll<HTMLButtonElement>(
      "request-list [data-request-key]",
    );
    [...rows].find((row) => row.dataset.requestKey === selected_key)?.focus();
  }

  private async closeSessionDetail() {
    const session_id =
      this.selected_session?.session_id ?? this.requested_session_id;
    ++this.navigation_workflow_id;
    this.resetSessionSelection();
    this.syncUrl("push");
    if (
      session_id === undefined ||
      !window.matchMedia("(max-width: 680px)").matches
    ) {
      return;
    }
    await this.updateComplete;
    const rows = this.renderRoot.querySelectorAll<HTMLButtonElement>(
      "session-list [data-session-id]",
    );
    [...rows].find((row) => row.dataset.sessionId === session_id)?.focus();
  }

  private async loadRequestDetail(
    day: string,
    request_id: string,
    row_id: string | undefined,
    summary: RequestSummary | undefined,
    preserve: boolean,
    history_mode: HistoryMode = "replace",
  ): Promise<boolean> {
    this.request_detail_controller?.abort();
    const controller = new AbortController();
    this.request_detail_controller = controller;
    const load_id = ++this.request_detail_load_id;
    this.selected_day = day;
    this.selected_request = summary;
    this.selected_request_id = request_id;
    this.selected_request_row_id = row_id;
    if (!preserve) {
      this.selected_request_detail = undefined;
    }
    this.request_detail_state = "loading";
    this.request_detail_error = undefined;
    if (history_mode) {
      this.syncUrl(history_mode);
    }
    try {
      const params = new URLSearchParams({ day, request_id });
      if (row_id) {
        params.set("row_id", row_id);
      }
      const detail = await readInspector<RequestDetail>(
        `/api/request?${params.toString()}`,
        controller.signal,
      );
      if (
        load_id === this.request_detail_load_id &&
        this.request_detail_controller === controller
      ) {
        const locator_changed = this.selected_request_row_id !== detail.row_id;
        this.selected_request_detail = detail;
        this.selected_request_row_id = detail.row_id;
        this.request_detail_state = "ready";
        if (history_mode || locator_changed) {
          this.syncUrl("replace");
        }
        return true;
      }
      return false;
    } catch (error) {
      if (load_id === this.request_detail_load_id && !isAbortError(error)) {
        this.request_detail_state = "error";
        this.request_detail_error = errorMessage(
          error,
          "Unable to load request detail",
        );
      }
      return false;
    } finally {
      if (this.request_detail_controller === controller) {
        this.request_detail_controller = undefined;
      }
    }
  }

  private async selectRequest(request: RequestSummary) {
    ++this.navigation_workflow_id;
    const preserve =
      this.selected_request_id === request.request_id &&
      this.selected_request_detail?.day === request.day &&
      this.selected_request_detail.row_id === request.row_id;
    const loading = this.loadRequestDetail(
      request.day,
      request.request_id,
      request.row_id,
      request,
      preserve,
      "push",
    );
    if (window.matchMedia("(max-width: 680px)").matches) {
      await this.updateComplete;
      this.renderRoot
        .querySelector<HTMLButtonElement>(
          "request-detail-view .mobile-back-button",
        )
        ?.focus();
    }
    const loaded = await loading;
    if (loaded && window.matchMedia("(max-width: 680px)").matches) {
      await this.updateComplete;
      this.renderRoot
        .querySelector<HTMLButtonElement>(
          "request-detail-view .mobile-back-button",
        )
        ?.focus();
    }
  }

  private retryRequestDetail() {
    const day =
      this.selected_request_detail?.day ??
      this.selected_request?.day ??
      this.selected_day;
    if (day && this.selected_request_id) {
      void this.loadRequestDetail(
        day,
        this.selected_request_id,
        this.selected_request_row_id,
        this.selected_request,
        Boolean(this.selected_request_detail),
        null,
      );
    }
  }

  private selectDay(day: string) {
    if (day === this.selected_day) {
      return;
    }
    ++this.navigation_workflow_id;
    this.selected_day = day;
    this.request_url_paths = [];
    this.resetRequestSelection();
    this.syncUrl("push");
    void this.loadRequestUrlPaths(day);
    void this.loadRequests();
  }

  private pickerDays(): RequestDay[] {
    if (
      !this.selected_day ||
      this.request_days.some(
        (request_day) => request_day.day === this.selected_day,
      )
    ) {
      return this.request_days;
    }
    return [
      { day: this.selected_day, state: "available" },
      ...this.request_days,
    ];
  }

  private adjacentAvailableDay(offset: -1 | 1): string | undefined {
    const available = this.pickerDays()
      .filter((request_day) => request_day.state === "available")
      .map((request_day) => request_day.day)
      .sort();
    if (!this.selected_day) {
      return undefined;
    }
    const index = available.indexOf(this.selected_day);
    return index < 0 ? undefined : available[index + offset];
  }

  private submitFilters(event: SubmitEvent) {
    event.preventDefault();
    ++this.navigation_workflow_id;
    this.applied_filters = {
      query: this.search_query.trim(),
      provider_id: this.provider_id.trim(),
      url_path: this.url_path,
      status: this.status_filter.trim(),
      errors_only: this.errors_only,
    };
    this.resetRequestSelection();
    this.syncUrl("push");
    void this.loadRequests();
  }

  private clearFilters() {
    this.search_query = "";
    this.provider_id = "";
    this.url_path = "";
    this.status_filter = "";
    this.errors_only = false;
    this.applied_filters = emptyFilters();
    ++this.navigation_workflow_id;
    this.resetRequestSelection();
    this.syncUrl("push");
    void this.loadRequests();
  }

  private hasAppliedFilters(): boolean {
    return Boolean(
      this.applied_filters.query ||
      this.applied_filters.provider_id ||
      this.applied_filters.url_path ||
      this.applied_filters.status ||
      this.applied_filters.errors_only,
    );
  }

  private filtersChanged(): boolean {
    return (
      this.search_query.trim() !== this.applied_filters.query ||
      this.provider_id.trim() !== this.applied_filters.provider_id ||
      this.url_path !== this.applied_filters.url_path ||
      this.status_filter.trim() !== this.applied_filters.status ||
      this.errors_only !== this.applied_filters.errors_only
    );
  }

  private providerOptions(): string[] {
    const providers = new Set(
      this.requests.flatMap((request) =>
        request.provider_id ? [request.provider_id] : [],
      ),
    );
    if (this.applied_filters.provider_id) {
      providers.add(this.applied_filters.provider_id);
    }
    return [...providers].sort();
  }

  private urlPathOptions(): RequestUrlPath[] {
    if (
      !this.url_path ||
      this.request_url_paths.some((option) => option.url_path === this.url_path)
    ) {
      return this.request_url_paths;
    }
    return [
      { url_path: this.url_path, request_count: 0 },
      ...this.request_url_paths,
    ];
  }

  private ensureSessionsLoaded(force = false): Promise<boolean> {
    if (this.sessions_loaded && !force) {
      return Promise.resolve(true);
    }
    if (this.session_list_load && !force) {
      return this.session_list_load;
    }
    this.session_list_controller?.abort();
    const controller = new AbortController();
    this.session_list_controller = controller;
    const load_id = ++this.session_list_load_id;
    this.sessions_loading = true;
    this.sessions_error = undefined;
    const load = this.loadSessions(controller, load_id);
    this.session_list_load = load;
    return load;
  }

  private async loadSessions(
    controller: AbortController,
    load_id: number,
  ): Promise<boolean> {
    try {
      const sessions = await readInspector<SessionSummary[]>(
        "/api/sessions?limit=100",
        controller.signal,
      );
      if (
        load_id !== this.session_list_load_id ||
        this.session_list_controller !== controller
      ) {
        return false;
      }
      this.sessions = sessions;
      this.sessions_loaded = true;
      if (this.selected_session) {
        this.selected_session =
          sessions.find(
            (session) =>
              session.session_id === this.selected_session?.session_id,
          ) ?? this.selected_session;
      }
      return true;
    } catch (error) {
      if (load_id === this.session_list_load_id && !isAbortError(error)) {
        this.sessions_error = errorMessage(error, "Unable to load sessions");
      }
      return false;
    } finally {
      if (
        load_id === this.session_list_load_id &&
        this.session_list_controller === controller
      ) {
        this.session_list_controller = undefined;
        this.session_list_load = undefined;
        this.sessions_loading = false;
      }
    }
  }

  private retrySessions() {
    const workflow_id = ++this.navigation_workflow_id;
    this.sessions_loaded = false;
    void this.retrySessionsAndRestore(workflow_id);
  }

  private async retrySessionsAndRestore(workflow_id: number) {
    const loaded = await this.ensureSessionsLoaded(true);
    if (
      !loaded ||
      workflow_id !== this.navigation_workflow_id ||
      this.active_view !== "sessions"
    ) {
      return;
    }
    const session_id =
      this.selected_session?.session_id ?? this.requested_session_id;
    if (session_id === undefined) {
      return;
    }
    const node_id =
      this.selected_session_node_id ?? this.requested_session_node_id;
    await this.loadSession(
      session_id,
      this.sessions.find((session) => session.session_id === session_id),
      this.selected_session_detail?.session.session_id === session_id,
      null,
      node_id,
    );
  }

  private async refreshSessions() {
    const workflow_id = this.navigation_workflow_id;
    const session_id =
      this.selected_session?.session_id ?? this.requested_session_id;
    const node_id = this.selected_session_node_id;
    const loaded = await this.ensureSessionsLoaded(true);
    const current_session_id =
      this.selected_session?.session_id ?? this.requested_session_id;
    if (
      loaded &&
      workflow_id === this.navigation_workflow_id &&
      session_id !== undefined &&
      current_session_id === session_id &&
      this.selected_session_node_id === node_id
    ) {
      await this.loadSession(
        session_id,
        this.sessions.find((session) => session.session_id === session_id),
        true,
        null,
        node_id,
      );
    }
  }

  private filteredSessions(): SessionSummary[] {
    const query = this.session_search_query.trim().toLocaleLowerCase();
    if (!query) {
      return this.sessions;
    }
    return this.sessions.filter((session) =>
      [
        session.session_id,
        session.model,
        session.provider_id,
        session.account_id,
        session.endpoint,
        session.status === null ? null : String(session.status),
      ].some((value) => value?.toLocaleLowerCase().includes(query)),
    );
  }

  private async loadSessionUsage(
    session_id: string,
    preserve: boolean,
  ): Promise<boolean> {
    this.session_usage_controller?.abort();
    const controller = new AbortController();
    this.session_usage_controller = controller;
    const load_id = ++this.session_usage_load_id;
    if (!preserve) {
      this.selected_session_usage = undefined;
    }
    this.session_usage_state = "loading";
    this.session_usage_error = undefined;
    try {
      const params = new URLSearchParams({ session_id });
      const usage = await readInspector<SessionUsage | null>(
        `/api/session-usage?${params.toString()}`,
        controller.signal,
      );
      if (
        load_id === this.session_usage_load_id &&
        this.session_usage_controller === controller
      ) {
        this.selected_session_usage = usage ?? undefined;
        this.session_usage_state = "ready";
        return true;
      }
      return false;
    } catch (error) {
      if (load_id === this.session_usage_load_id && !isAbortError(error)) {
        this.session_usage_state = "error";
        this.session_usage_error = errorMessage(
          error,
          "Unable to load session usage",
        );
      }
      return false;
    } finally {
      if (this.session_usage_controller === controller) {
        this.session_usage_controller = undefined;
      }
    }
  }

  private async loadSession(
    session_id: string,
    session: SessionSummary | undefined,
    preserve: boolean,
    history_mode: HistoryMode = "push",
    requested_node_id?: string,
  ): Promise<boolean> {
    this.session_detail_controller?.abort();
    this.session_node_controller?.abort();
    const controller = new AbortController();
    this.session_detail_controller = controller;
    const load_id = ++this.session_detail_load_id;
    const node_load_id = ++this.session_node_load_id;
    this.requested_session_id = session_id;
    this.requested_session_node_id = requested_node_id;
    this.selected_session = session;
    if (!preserve) {
      this.selected_session_detail = undefined;
      this.selected_session_node_detail = undefined;
      this.selected_session_node_id = undefined;
      this.session_node_state = "idle";
      this.session_node_error = undefined;
    }
    void this.loadSessionUsage(session_id, preserve);
    this.session_detail_state = "loading";
    this.session_detail_error = undefined;
    if (history_mode) {
      this.syncUrl(history_mode);
    }
    try {
      const params = new URLSearchParams({ session_id, limit: "500" });
      const detail = await readInspector<SessionDetail>(
        `/api/session?${params.toString()}`,
        controller.signal,
      );
      if (
        load_id === this.session_detail_load_id &&
        this.session_detail_controller === controller
      ) {
        this.selected_session = detail.session;
        this.selected_session_detail = detail;
        this.sessions = this.sessions.map((candidate) =>
          candidate.session_id === detail.session.session_id
            ? detail.session
            : candidate,
        );
        this.session_detail_state = "ready";
        if (node_load_id !== this.session_node_load_id) {
          return true;
        }
        if (requested_node_id) {
          const selected_node = detail.nodes.find(
            (node) => node.node_id === requested_node_id,
          );
          void this.loadSessionNode(
            selected_node ?? requested_node_id,
            false,
            "replace",
          );
        } else {
          this.selected_session_node_id = undefined;
          this.selected_session_node_detail = undefined;
          this.session_node_state = "idle";
          this.syncUrl("replace");
        }
        return true;
      }
      return false;
    } catch (error) {
      if (load_id === this.session_detail_load_id && !isAbortError(error)) {
        this.session_detail_state = "error";
        this.session_detail_error = errorMessage(
          error,
          "Unable to load semantic session",
        );
      }
      return false;
    } finally {
      if (this.session_detail_controller === controller) {
        this.session_detail_controller = undefined;
      }
    }
  }

  private async loadSessionNode(
    node: SessionNodeSummary | string,
    preserve: boolean,
    history_mode: HistoryMode = "push",
  ): Promise<boolean> {
    const session_id =
      this.selected_session?.session_id ?? this.requested_session_id;
    if (session_id === undefined) {
      return false;
    }
    this.session_node_controller?.abort();
    const controller = new AbortController();
    this.session_node_controller = controller;
    const load_id = ++this.session_node_load_id;
    const node_id = typeof node === "string" ? node : node.node_id;
    this.requested_session_node_id = node_id;
    this.selected_session_node_id = node_id;
    if (!preserve) {
      this.selected_session_node_detail = undefined;
    }
    this.session_node_state = "loading";
    this.session_node_error = undefined;
    if (history_mode) {
      this.syncUrl(history_mode);
    }
    try {
      const params = new URLSearchParams({ session_id, node_id });
      const detail = await readInspector<SessionNodeDetail>(
        `/api/session-node?${params.toString()}`,
        controller.signal,
      );
      if (
        load_id === this.session_node_load_id &&
        this.session_node_controller === controller
      ) {
        this.selected_session_node_detail = detail;
        this.session_node_state = "ready";
        this.syncUrl("replace");
        return true;
      }
      return false;
    } catch (error) {
      if (load_id === this.session_node_load_id && !isAbortError(error)) {
        this.session_node_state = "error";
        this.session_node_error = errorMessage(
          error,
          "Unable to load semantic node content",
        );
      }
      return false;
    } finally {
      if (this.session_node_controller === controller) {
        this.session_node_controller = undefined;
      }
    }
  }

  private async selectSession(session: SessionSummary) {
    const workflow_id = ++this.navigation_workflow_id;
    const loaded = await this.loadSession(
      session.session_id,
      session,
      false,
      "push",
    );
    if (
      !loaded ||
      workflow_id !== this.navigation_workflow_id ||
      this.active_view !== "sessions" ||
      this.selected_session_detail?.session.session_id !== session.session_id ||
      !window.matchMedia("(max-width: 680px)").matches
    ) {
      return;
    }

    await this.updateComplete;
    const detail_view = this.renderRoot.querySelector<LitElement>(
      "session-detail-view",
    );
    await detail_view?.updateComplete;
    if (
      workflow_id === this.navigation_workflow_id &&
      this.active_view === "sessions" &&
      this.selected_session_detail?.session.session_id === session.session_id
    ) {
      detail_view
        ?.querySelector<HTMLButtonElement>(".mobile-back-button")
        ?.focus();
    }
  }

  private collapseSessionNode(history_mode: HistoryMode = "push") {
    this.session_node_controller?.abort();
    this.session_node_controller = undefined;
    ++this.session_node_load_id;
    this.requested_session_node_id = undefined;
    this.selected_session_node_id = undefined;
    this.selected_session_node_detail = undefined;
    this.session_node_state = "idle";
    this.session_node_error = undefined;
    if (history_mode) {
      this.syncUrl(history_mode);
    }
  }

  private selectSessionNode(node: SessionNodeSummary) {
    if (node.node_id === this.selected_session_node_id) {
      this.collapseSessionNode();
      return;
    }
    void this.loadSessionNode(node, false, "push");
  }

  private retrySessionDetail() {
    const session_id =
      this.selected_session?.session_id ?? this.requested_session_id;
    if (session_id !== undefined) {
      void this.loadSession(
        session_id,
        this.selected_session,
        Boolean(this.selected_session_detail),
        null,
        this.selected_session_node_id ?? this.requested_session_node_id,
      );
    }
  }

  private retrySessionUsage() {
    const session_id =
      this.selected_session?.session_id ?? this.requested_session_id;
    if (session_id !== undefined) {
      void this.loadSessionUsage(
        session_id,
        Boolean(this.selected_session_usage),
      );
    }
  }

  private retrySessionNode() {
    const node = this.selected_session_detail?.nodes.find(
      (candidate) => candidate.node_id === this.selected_session_node_id,
    );
    if (node ?? this.selected_session_node_id) {
      void this.loadSessionNode(
        node ?? this.selected_session_node_id!,
        Boolean(this.selected_session_node_detail),
        null,
      );
    }
  }

  private async openSession(session_id: string) {
    ++this.navigation_workflow_id;
    this.setActiveView("sessions", false, null);
    await this.ensureSessionsLoaded();
    const session = this.sessions.find(
      (candidate) => candidate.session_id === session_id,
    );
    await this.loadSession(session_id, session, false, "push");
  }

  private async openRequestFromSession(node: SessionNodeSummary) {
    ++this.navigation_workflow_id;
    this.setActiveView("requests", false, null);
    this.search_query = "";
    this.provider_id = "";
    this.url_path = "";
    this.status_filter = "";
    this.errors_only = false;
    this.applied_filters = emptyFilters();
    this.selected_day = requestDay(node.ts);
    this.resetRequestSelection();
    void this.loadRequestDays();
    void this.loadRequestUrlPaths(this.selected_day);
    void this.loadRequests();
    const loaded = await this.loadRequestDetail(
      this.selected_day,
      node.request_id,
      undefined,
      undefined,
      false,
      "push",
    );
    if (
      !loaded &&
      this.request_detail_state === "error" &&
      this.request_detail_error === "request not found"
    ) {
      this.request_detail_error =
        "Request history is unavailable; semantic session data is still retained.";
    }
  }

  private async loadRequestsView() {
    void this.loadRequestDays();
    if (this.selected_day) {
      void this.loadRequestUrlPaths(this.selected_day);
      await this.loadRequests();
    } else {
      await this.loadLatestRequests();
    }
  }

  private setActiveView(
    active_view: ViewName,
    load_view = true,
    history_mode: HistoryMode = "push",
  ) {
    if (history_mode === "push") {
      ++this.navigation_workflow_id;
    }
    this.active_view = active_view;
    if (history_mode) {
      this.syncUrl(history_mode);
    }
    if (!load_view) {
      return;
    }
    if (active_view === "sessions") {
      void this.ensureSessionsLoaded();
    } else if (this.request_list_state === "idle") {
      void this.loadRequestsView();
    }
  }

  private setTimezone(timezone: TimezoneMode) {
    this.timezone = timezone;
    this.syncUrl("push");
  }

  private setDetailTab(tab: DetailTab) {
    this.active_detail_tab = tab;
    this.syncUrl("push");
  }

  private renderDayPicker() {
    const days = this.pickerDays();
    const previous_day = this.adjacentAvailableDay(-1);
    const next_day = this.adjacentAvailableDay(1);
    return html`
      <div class="day-control">
        <span class="control-label">UTC storage day</span>
        <div class="day-navigation">
          <button
            type="button"
            class="icon-button"
            title="Previous available day"
            aria-label="Previous available day"
            ?disabled=${!previous_day}
            @click=${() => previous_day && this.selectDay(previous_day)}
          >
            ←
          </button>
          <select
            aria-label="Request storage day"
            .value=${this.selected_day ?? ""}
            ?disabled=${days.length === 0}
            @change=${(event: Event) => this.selectDay((event.target as HTMLSelectElement).value)}
          >
            ${this.selected_day ? nothing : html`<option value="">No request day</option>`}
            ${days.map(
              (request_day) => html`
                <option
                  value=${request_day.day}
                  ?disabled=${request_day.state !== "available"}
                >
                  ${request_day.day}${request_day.state === "empty" ? " · empty" : request_day.state === "unavailable" ? " · unavailable" : ""}
                </option>
              `,
            )}
          </select>
          <button
            type="button"
            class="icon-button"
            title="Next available day"
            aria-label="Next available day"
            ?disabled=${!next_day}
            @click=${() => next_day && this.selectDay(next_day)}
          >
            →
          </button>
        </div>
      </div>
    `;
  }

  private renderRequestToolbar() {
    const has_day = Boolean(this.selected_day);
    return html`
      <section class="request-toolbar" aria-label="Request controls">
        <div class="toolbar-primary">
          ${this.renderDayPicker()}
          <button
            type="button"
            class="refresh-button"
            ?disabled=${!has_day || this.request_list_state === "loading"}
            @click=${() => {
              void this.loadRequests();
              void this.loadRequestDays();
              if (this.selected_day) {
                void this.loadRequestUrlPaths(this.selected_day);
              }
            }}
          >
            <span aria-hidden="true">↻</span> Refresh requests
          </button>
          <div
            class="timezone-toggle"
            role="group"
            aria-label="Timestamp timezone"
          >
            <button
              type="button"
              aria-pressed=${String(this.timezone === "local")}
              @click=${() => this.setTimezone("local")}
            >
              Local
            </button>
            <button
              type="button"
              aria-pressed=${String(this.timezone === "utc")}
              @click=${() => this.setTimezone("utc")}
            >
              UTC
            </button>
          </div>
        </div>
        <form class="filter-bar" @submit=${this.submitFilters}>
          <label class="search-field">
            <span class="visually-hidden">Search requests</span>
            <span class="search-icon" aria-hidden="true">⌕</span>
            <input
              type="search"
              .value=${this.search_query}
              ?disabled=${!has_day}
              placeholder="Search request, session, model…"
              @input=${(event: Event) => (this.search_query = (event.target as HTMLInputElement).value)}
            />
          </label>
          <label>
            <span class="visually-hidden">Provider ID</span>
            <input
              list="provider-options"
              .value=${this.provider_id}
              ?disabled=${!has_day}
              placeholder="Any provider"
              @input=${(event: Event) => (this.provider_id = (event.target as HTMLInputElement).value)}
            />
            <datalist id="provider-options">
              ${this.providerOptions().map((provider_id) => html`<option value=${provider_id}></option>`)}
            </datalist>
          </label>
          <label>
            <span class="visually-hidden">URL path</span>
            <select
              class="url-path-filter"
              .value=${this.url_path}
              ?disabled=${!has_day || this.request_url_paths_loading}
              @change=${(event: Event) => (this.url_path = (event.target as HTMLSelectElement).value)}
            >
              <option value="">
                ${this.request_url_paths_loading ? "Loading URL paths…" : "Any URL path"}
              </option>
              ${this.urlPathOptions().map(
                (option) => html`
                  <option value=${option.url_path}>
                    ${option.url_path}${option.request_count ? ` · ${option.request_count.toLocaleString()}` : ""}
                  </option>
                `,
              )}
            </select>
          </label>
          <label>
            <span class="visually-hidden">Exact response status</span>
            <input
              class="status-filter"
              type="number"
              min="100"
              max="599"
              step="1"
              .value=${this.status_filter}
              ?disabled=${!has_day}
              placeholder="Any status"
              @input=${(event: Event) => (this.status_filter = (event.target as HTMLInputElement).value)}
            />
          </label>
          <label class="errors-filter">
            <input
              type="checkbox"
              .checked=${this.errors_only}
              ?disabled=${!has_day}
              @change=${(event: Event) => (this.errors_only = (event.target as HTMLInputElement).checked)}
            />
            <span>Errors only</span>
          </label>
          <button
            type="submit"
            class="primary-button"
            ?disabled=${!has_day || !this.filtersChanged()}
          >
            Apply
          </button>
          ${
            this.hasAppliedFilters()
              ? html`<button
                  type="button"
                  class="text-button"
                  @click=${this.clearFilters}
                >
                  Clear
                </button>`
              : nothing
          }
        </form>
        ${this.request_days_error ? html`<p class="toolbar-warning" role="status">Day scan: ${this.request_days_error}</p>` : nothing}
        ${this.request_url_paths_error ? html`<p class="toolbar-warning" role="status">URL paths: ${this.request_url_paths_error}</p>` : nothing}
      </section>
    `;
  }

  private renderRequestSidebar() {
    const has_content = this.requests.length > 0;
    return html`
      <div
        class="list-pane"
        aria-busy=${String(this.request_list_state === "loading")}
      >
        <header class="list-pane-header">
          <div>
            <strong>Requests</strong>
            <span
              >${this.requests.length.toLocaleString()}
              loaded${this.next_cursor ? " · more available" : ""}</span
            >
          </div>
          ${this.hasAppliedFilters() ? html`<span class="filter-indicator">Filtered</span>` : nothing}
        </header>
        ${
          this.request_list_state === "loading"
            ? html`
                <div class="inline-state" role="status">
                  <span class="spinner" aria-hidden="true"></span
                  >${has_content ? "Refreshing requests…" : "Loading requests…"}
                </div>
              `
            : nothing
        }
        ${
          this.request_list_state === "error"
            ? html`
                <div class="inline-error" role="alert">
                  <span>${this.request_list_error}</span>
                  <button
                    type="button"
                    @click=${() => void this.loadRequests()}
                  >
                    Retry
                  </button>
                </div>
              `
            : nothing
        }
        ${
          has_content
            ? html`
                <request-list
                  .requests=${this.requests}
                  .selected_key=${
                    this.selectedRequestDay() && this.selected_request_row_id
                      ? requestKey({
                          day: this.selectedRequestDay()!,
                          row_id: this.selected_request_row_id,
                        })
                      : undefined
                  }
                  .timezone=${this.timezone}
                  @request-select=${(event: Event) => void this.selectRequest(eventDetail<RequestSummary>(event))}
                ></request-list>
              `
            : this.request_list_state === "ready"
              ? html`<p class="empty">
                  No persisted requests match these filters.
                </p>`
              : this.request_list_state === "idle"
                ? html`<p class="empty">Choose an available request day.</p>`
                : nothing
        }
        ${
          this.load_more_error
            ? html`
                <div class="inline-error load-more-error" role="alert">
                  <span>${this.load_more_error}</span>
                  <button
                    type="button"
                    @click=${() => void this.loadRequests(true)}
                  >
                    Retry
                  </button>
                </div>
              `
            : nothing
        }
        ${
          this.next_cursor && has_content
            ? html`
                <div class="list-footer">
                  <button
                    type="button"
                    class="secondary-button"
                    ?disabled=${this.loading_more}
                    @click=${() => void this.loadRequests(true)}
                  >
                    ${this.loading_more ? "Loading…" : "Load more"}
                  </button>
                </div>
              `
            : has_content && this.request_list_state === "ready"
              ? html`<p class="end-of-list">End of loaded day</p>`
              : nothing
        }
      </div>
    `;
  }

  private renderSessionsSidebar() {
    const sessions = this.filteredSessions();
    const has_content = this.sessions.length > 0;
    return html`
      <div class="list-pane" aria-busy=${String(this.sessions_loading)}>
        <header class="list-pane-header">
          <div>
            <strong>Recent sessions</strong>
            <span>
              ${
                this.session_search_query
                  ? `${sessions.length.toLocaleString()} of ${this.sessions.length.toLocaleString()} loaded`
                  : `${this.sessions.length.toLocaleString()} loaded · newest first`
              }
            </span>
          </div>
          ${this.session_search_query ? html`<span class="filter-indicator">Filtered</span>` : nothing}
        </header>
        ${
          this.sessions_loading
            ? html`
                <div class="inline-state" role="status">
                  <span class="spinner" aria-hidden="true"></span
                  >${has_content ? "Refreshing sessions…" : "Loading sessions…"}
                </div>
              `
            : nothing
        }
        ${
          this.sessions_error
            ? html`
                <div class="inline-error" role="alert">
                  <span>${this.sessions_error}</span>
                  <button type="button" @click=${this.retrySessions}>
                    Retry
                  </button>
                </div>
              `
            : nothing
        }
        ${
          sessions.length > 0
            ? html`
                <session-list
                  .sessions=${sessions}
                  .selected_session_id=${this.selected_session?.session_id ?? this.requested_session_id}
                  .timezone=${this.timezone}
                  @session-select=${(event: Event) => void this.selectSession(eventDetail<SessionSummary>(event))}
                ></session-list>
              `
            : this.sessions_loaded && this.session_search_query
              ? html`<p class="empty">No recent sessions match this filter.</p>`
              : this.sessions_loaded
                ? html`
                    <div class="empty empty-session-list">
                      <strong>No semantic sessions available</strong>
                      <span
                        >The gateway records successful sessions here when
                        session persistence is enabled.</span
                      >
                    </div>
                  `
                : nothing
        }
        ${
          has_content && !this.session_search_query
            ? html`<p class="end-of-list">
                ${this.sessions.length === 100 ? "Latest 100 sessions" : "End of recent sessions"}
              </p>`
            : nothing
        }
      </div>
    `;
  }

  private renderSessionDetail() {
    return html`
      <session-detail-view
        .detail=${this.selected_session_detail}
        .node_detail=${this.selected_session_node_detail}
        .usage=${this.selected_session_usage}
        .state=${this.session_detail_state}
        .error_message=${this.session_detail_error}
        .usage_state=${this.session_usage_state}
        .usage_error_message=${this.session_usage_error}
        .node_state=${this.session_node_state}
        .node_error_message=${this.session_node_error}
        .selected_node_id=${this.selected_session_node_id}
        .timezone=${this.timezone}
        @session-close=${() => void this.closeSessionDetail()}
        @session-retry=${this.retrySessionDetail}
        @session-usage-retry=${this.retrySessionUsage}
        @session-node-retry=${this.retrySessionNode}
        @session-node-select=${(event: Event) => this.selectSessionNode(eventDetail<SessionNodeSummary>(event))}
        @open-request=${(event: Event) => void this.openRequestFromSession(eventDetail<SessionNodeSummary>(event))}
      ></session-detail-view>
    `;
  }

  private renderSessionToolbar() {
    return html`
      <section class="session-toolbar">
        <label class="session-search-field">
          <span class="visually-hidden">Filter recent sessions</span>
          <span class="search-icon" aria-hidden="true">⌕</span>
          <input
            type="search"
            .value=${this.session_search_query}
            placeholder="Filter session, model, provider…"
            @input=${(event: Event) => (this.session_search_query = (event.target as HTMLInputElement).value)}
          />
        </label>
        <p>
          <span class="source-indicator" aria-hidden="true"></span>Semantic
          trees and content from <code>sessions.db</code>
        </p>
        <div class="session-toolbar-actions">
          <button
            type="button"
            class="refresh-button"
            ?disabled=${this.sessions_loading}
            @click=${() => void this.refreshSessions()}
          >
            <span aria-hidden="true">↻</span> Refresh sessions
          </button>
          <div
            class="timezone-toggle"
            role="group"
            aria-label="Timestamp timezone"
          >
            <button
              type="button"
              aria-pressed=${String(this.timezone === "local")}
              @click=${() => this.setTimezone("local")}
            >
              Local
            </button>
            <button
              type="button"
              aria-pressed=${String(this.timezone === "utc")}
              @click=${() => this.setTimezone("utc")}
            >
              UTC
            </button>
          </div>
        </div>
      </section>
    `;
  }

  render() {
    const data_path =
      this.active_view === "sessions"
        ? this.info?.sessions_db
        : this.info?.requests_dir;
    const has_selection =
      this.active_view === "requests"
        ? Boolean(this.selected_request_id)
        : this.requested_session_id !== undefined;
    return html`
      <header class="app-header">
        <div>
          <p class="activity-eyebrow">OBSERVABILITY</p>
          <h1>Activity</h1>
          <p class="activity-description">
            Follow a request. Understand the conversation.
          </p>
        </div>
        <span
          class="activity-readonly"
          title="Browse local history without changing stored requests or sessions."
          >Read only</span
        >
      </header>
      <main class="app-shell">
        <div class="shell-navigation">
          <nav class="view-navigation" aria-label="Inspector views">
            <button
              type="button"
              aria-current=${this.active_view === "requests" ? "page" : "false"}
              @click=${() => this.setActiveView("requests")}
            >
              Requests
            </button>
            <button
              type="button"
              aria-current=${this.active_view === "sessions" ? "page" : "false"}
              @click=${() => this.setActiveView("sessions")}
            >
              Sessions
            </button>
          </nav>
          <span class="data-path" title=${data_path ?? ""}
            >${data_path ? "Stored on this device" : "Loading data source…"}</span
          >
        </div>
        ${
          this.active_view === "requests"
            ? this.renderRequestToolbar()
            : this.renderSessionToolbar()
        }
        <section
          class="viewer-grid ${this.active_view === "requests" ? "request-view" : "session-view"} ${has_selection ? "has-selection" : ""}"
        >
          <aside
            class="sidebar"
            aria-label=${this.active_view === "requests" ? "Request list" : "Session list"}
          >
            ${this.active_view === "requests" ? this.renderRequestSidebar() : this.renderSessionsSidebar()}
          </aside>
          <article
            class="detail-pane"
            aria-label=${this.active_view === "requests" ? "Request detail" : "Session detail"}
          >
            ${
              this.active_view === "requests"
                ? html`
                    <request-detail-view
                      .detail=${this.selected_request_detail}
                      .summary=${this.selected_request}
                      .state=${this.request_detail_state}
                      .error_message=${this.request_detail_error}
                      .active_tab=${this.active_detail_tab}
                      .timezone=${this.timezone}
                      @detail-retry=${this.retryRequestDetail}
                      @detail-close=${() => void this.closeRequestDetail()}
                      @detail-tab-change=${(event: Event) => this.setDetailTab(eventDetail<DetailTab>(event))}
                      @open-session=${(event: Event) => void this.openSession(eventDetail<string>(event))}
                    ></request-detail-view>
                  `
                : this.renderSessionDetail()
            }
          </article>
        </section>
      </main>
    `;
  }
}

customElements.define("inspect-app", InspectApp);
