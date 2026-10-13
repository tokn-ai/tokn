import { useEffect, useState } from "react";
import {
  Activity,
  ArrowRight,
  Check,
  CheckCircle2,
  Copy,
  LoaderCircle,
  Network,
  Power,
  RefreshCw,
  Route,
  Square,
  Users,
} from "lucide-react";
import { api } from "../lib/tauri";
import { useGateway } from "../lib/gateway";
import { useResource } from "../lib/useResource";
import { summarizeUsage } from "../lib/usage";
import { ErrorMessage, PageHeading, number } from "../components/Feedback";
import "./overview.css";

type OverviewDestination = "accounts" | "routing" | "inspect";

const gateway_states = {
  checking: { label: "Checking", title: "Checking your gateway" },
  starting: { label: "Starting", title: "Getting your gateway ready" },
  running: { label: "Running", title: "Your gateway is ready" },
  stopping: { label: "Stopping", title: "Stopping your gateway" },
  stopped: { label: "Stopped", title: "Start your local gateway" },
  unavailable: { label: "Unavailable", title: "Gateway status unavailable" },
};

export function Overview({
  onNavigate,
}: {
  onNavigate: (page: OverviewDestination) => void;
}) {
  const status = useGateway();
  const usage = useResource(api.usage);
  const [pending_action, setPendingAction] = useState<"start" | "stop" | null>(
    null,
  );
  const [action_error, setActionError] = useState("");
  const [copy_error, setCopyError] = useState("");
  const [copied, setCopied] = useState(false);
  const [show_all_models, setShowAllModels] = useState(false);

  useEffect(() => {
    if (!copied) return;
    const timer = window.setTimeout(() => setCopied(false), 2500);
    return () => window.clearTimeout(timer);
  }, [copied]);

  async function act(action: "start" | "stop") {
    setPendingAction(action);
    setActionError("");
    try {
      await (action === "start" ? api.start() : api.stop());
    } catch (error) {
      setActionError(String(error));
    } finally {
      await status.refresh();
      setPendingAction(null);
    }
  }

  async function copyEndpoint() {
    if (!endpoint) return;
    setCopyError("");
    try {
      await navigator.clipboard.writeText(endpoint);
      setCopied(true);
    } catch {
      setCopyError(
        "Couldn't copy the URL. Select the address to copy it manually.",
      );
    }
  }

  const { totals, models } = summarizeUsage(usage.data ?? []);
  const visible_models = show_all_models ? models : models.slice(0, 5);
  const has_status = Boolean(status.data);
  const reported_state = status.data?.state;
  const gateway_state =
    pending_action === "stop"
      ? "stopping"
      : pending_action === "start"
        ? "starting"
        : status.error
          ? "unavailable"
          : reported_state === "running" ||
              reported_state === "starting" ||
              reported_state === "stopped"
            ? reported_state
            : status.loading
              ? "checking"
              : "unavailable";
  const running = gateway_state === "running";
  const managed = status.data?.ownership === "managed";
  const external = status.data?.ownership === "external";
  const can_start = status.data?.ownership === "none" && !status.error;
  const endpoint = status.data?.address
    ? `http://${status.data.address}/v1`
    : "";
  const busy = pending_action !== null;
  const refreshing = status.loading || usage.loading;
  const usage_unavailable = !usage.data && Boolean(usage.error);
  const initial_loading = usage.loading && !usage.data;
  const { label: status_label, title: status_title } =
    gateway_states[gateway_state];
  const status_description = status.error
    ? "Couldn't confirm the current gateway status. Refresh to check the connection again."
    : external
      ? "Connected to a gateway started outside this app. Manage its process where you started it."
      : managed
        ? "Managed by Tokn Desktop. Your gateway stops when you quit the app."
        : !has_status
          ? "Your connection status will appear here once the local gateway has been checked."
          : "Use your connected accounts and routing configuration from one local endpoint.";

  return (
    <div className="overview-page">
      <PageHeading
        eyebrow="YOUR WORKSPACE"
        title="Overview"
        description="Your gateway and the activity flowing through it."
      >
        <button
          className="overview-refresh"
          onClick={() => {
            void status.refresh();
            void usage.refresh();
          }}
          disabled={busy || refreshing}
          aria-label={refreshing ? "Refreshing overview" : "Refresh overview"}
        >
          <RefreshCw
            size={15}
            className={refreshing ? "overview-spinning" : ""}
          />
          {refreshing ? "Refreshing" : "Refresh"}
        </button>
      </PageHeading>

      <ErrorMessage
        error={action_error || status.error || status.data?.last_error || ""}
      />

      <section className="gateway-panel" aria-labelledby="gateway-title">
        <div className="gateway-panel-main">
          <div className={`gateway-symbol ${running ? "is-running" : ""}`}>
            <Network size={25} strokeWidth={1.5} aria-hidden="true" />
          </div>
          <div className="gateway-summary">
            <div className="gateway-eyeline">
              <span>LOCAL GATEWAY</span>
              <span
                className={`gateway-status ${running ? "is-running" : ""}`}
                role="status"
              >
                <span className="gateway-status-dot" />
                {status_label}
              </span>
            </div>
            <h2 id="gateway-title">{status_title}</h2>
            <p>{status_description}</p>
          </div>
          <div className="gateway-control">
            {external ? (
              <button onClick={() => onNavigate("inspect")}>
                View activity <ArrowRight size={15} />
              </button>
            ) : (
              <button
                className={
                  managed || pending_action === "stop" ? "" : "primary"
                }
                disabled={busy || (!managed && !can_start)}
                onClick={() => void act(managed ? "stop" : "start")}
              >
                {busy ? (
                  <LoaderCircle size={15} className="overview-spinning" />
                ) : managed ? (
                  <Square size={13} />
                ) : (
                  <Power size={15} />
                )}
                {pending_action === "stop"
                  ? "Stopping…"
                  : pending_action === "start"
                    ? "Starting…"
                    : managed
                      ? "Stop gateway"
                      : "Start gateway"}
              </button>
            )}
          </div>
        </div>
        <div className="gateway-connection">
          <div className="gateway-endpoint">
            <span className="gateway-endpoint-label">API base URL</span>
            <code>
              {endpoint ||
                (status.loading
                  ? "Checking local address…"
                  : "Address unavailable")}
            </code>
            <button
              className="gateway-copy"
              disabled={!endpoint}
              onClick={() => void copyEndpoint()}
              aria-label={copied ? "API base URL copied" : "Copy API base URL"}
              title={copied ? "Copied" : "Copy API base URL"}
            >
              {copied ? <Check size={14} /> : <Copy size={14} />}
              <span aria-live="polite">{copied ? "Copied" : "Copy"}</span>
            </button>
          </div>
          <span className="gateway-connection-hint">
            For OpenAI-compatible clients
          </span>
        </div>
        {copy_error && (
          <div className="gateway-copy-error" role="alert">
            {copy_error}
          </div>
        )}
      </section>

      <section className="usage-section" aria-labelledby="usage-title">
        <div className="overview-section-heading">
          <h2 id="usage-title">Activity at a glance</h2>
          <span>Past 24 hours</span>
        </div>
        {usage.error && (
          <ErrorMessage
            error={`${usage.data ? "Showing previously loaded usage. " : ""}${usage.error}`}
          />
        )}
        <div className="usage-stats" aria-busy={initial_loading}>
          {[
            {
              label: "Requests",
              value: totals.requests,
              detail: "Recorded requests",
            },
            {
              label: "Input tokens",
              value: totals.input_tokens,
              detail: "Sent to your models",
            },
            {
              label: "Output tokens",
              value: totals.output_tokens,
              detail: "Generated by your models",
            },
            {
              label: "Cache read",
              value: totals.cached_tokens,
              detail: "Tokens read from cache",
            },
          ].map(({ label, value, detail }) => (
            <div className="usage-stat" key={label}>
              <p>{label}</p>
              <strong
                title={
                  initial_loading || usage_unavailable
                    ? undefined
                    : value.toLocaleString()
                }
              >
                {initial_loading ? (
                  <span className="usage-loading-value" aria-label="Loading" />
                ) : usage_unavailable ? (
                  "—"
                ) : (
                  number(value)
                )}
              </strong>
              <span>{detail}</span>
            </div>
          ))}
        </div>

        <div className="usage-models">
          <div className="usage-models-heading">
            <div>
              <h3>Model usage</h3>
              <p>Across your accounts, ordered by requests.</p>
            </div>
            {models.length > 0 && (
              <span className="usage-model-count">
                {models.length} {models.length === 1 ? "model" : "models"}
              </span>
            )}
          </div>
          {initial_loading ? (
            <div className="usage-placeholder" role="status">
              <LoaderCircle size={21} className="overview-spinning" />
              <span>Loading your activity…</span>
            </div>
          ) : usage_unavailable ? (
            <div className="usage-placeholder">
              <p>Usage couldn't be loaded.</p>
              <button onClick={() => void usage.refresh()}>Try again</button>
            </div>
          ) : !models.length ? (
            <div className="usage-placeholder">
              <Activity size={25} strokeWidth={1.4} aria-hidden="true" />
              <h3>Your next request starts here</h3>
              <p>
                Send a request through your gateway to see model usage here.
              </p>
              <button onClick={() => onNavigate("accounts")}>
                Review accounts <ArrowRight size={14} />
              </button>
            </div>
          ) : (
            <>
              <div
                className="usage-table-scroll"
                role="region"
                aria-label="Model usage"
                tabIndex={0}
              >
                <table className="usage-model-table">
                  <caption className="sr-only">
                    Model usage recorded in the past 24 hours
                  </caption>
                  <thead>
                    <tr>
                      <th scope="col">Model</th>
                      <th scope="col">Requests</th>
                      <th scope="col">Input</th>
                      <th scope="col">Output</th>
                      <th scope="col">Cache read</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visible_models.map((row) => (
                      <tr key={row.model}>
                        <th scope="row">
                          <span className="usage-model-name" title={row.model}>
                            {row.model || "Unknown model"}
                          </span>
                          <span className="usage-model-provider">
                            {row.providers.join(" · ") || "Unassigned provider"}
                          </span>
                        </th>
                        <td
                          className="usage-request-count"
                          title={row.requests.toLocaleString()}
                        >
                          <span>{number(row.requests)}</span>
                          <span
                            className="usage-request-bar"
                            aria-hidden="true"
                          >
                            <span
                              style={{
                                width: `${models[0].requests ? (row.requests / models[0].requests) * 100 : 0}%`,
                              }}
                            />
                          </span>
                        </td>
                        <td title={row.input_tokens.toLocaleString()}>
                          {number(row.input_tokens)}
                        </td>
                        <td title={row.output_tokens.toLocaleString()}>
                          {number(row.output_tokens)}
                        </td>
                        <td title={row.cached_tokens.toLocaleString()}>
                          {number(row.cached_tokens)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="usage-models-footer">
                <span>
                  <CheckCircle2 size={13} aria-hidden="true" /> Recorded locally
                </span>
                {models.length > 5 && (
                  <button
                    onClick={() => setShowAllModels(!show_all_models)}
                    aria-expanded={show_all_models}
                  >
                    {show_all_models
                      ? "Show fewer"
                      : `Show all ${models.length} models`}
                  </button>
                )}
              </div>
            </>
          )}
        </div>
      </section>

      <section className="overview-shortcuts" aria-label="Workspace shortcuts">
        {[
          {
            destination: "accounts" as const,
            icon: Users,
            title: "Accounts",
            description: "Connect providers and check availability.",
          },
          {
            destination: "routing" as const,
            icon: Route,
            title: "Routing",
            description: "Choose how requests reach your models.",
          },
          {
            destination: "inspect" as const,
            icon: Activity,
            title: "Activity",
            description: "Explore requests and conversations.",
          },
        ].map(({ destination, icon: Icon, title, description }) => (
          <button
            className="overview-shortcut"
            onClick={() => onNavigate(destination)}
            key={destination}
          >
            <Icon size={18} strokeWidth={1.6} aria-hidden="true" />
            <span>
              <strong>{title}</strong>
              <span>{description}</span>
            </span>
            <ArrowRight size={15} aria-hidden="true" />
          </button>
        ))}
      </section>
    </div>
  );
}
