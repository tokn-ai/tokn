import { useEffect, useState } from "react";
import {
  ArrowRight,
  Check,
  ArrowLeft,
  FileCode2,
  Info,
  RefreshCw,
  Save,
} from "lucide-react";
import { api } from "../lib/tauri";
import { useResource } from "../lib/useResource";
import { Empty, ErrorMessage, PageHeading } from "../components/Feedback";
import { Modal } from "../components/Modal";
import { RoutingBrowser } from "../components/routing/RoutingBrowser";
import { RoutingEditor } from "../components/routing/RoutingEditor";
import "./routing.css";

type ApplyState = "unknown" | "pending" | "applied" | "unconfirmed";

export function Routing() {
  const resource = useResource(api.routing);
  const [draft, setDraft] = useState("");
  const [operation, setOperation] = useState<"saving" | "applying" | null>(
    null,
  );
  const [apply_state, setApplyState] = useState<ApplyState>("unknown");
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [view, setView] = useState<"browse" | "advanced">("browse");
  const [confirm_reload, setConfirmReload] = useState(false);

  useEffect(() => {
    if (resource.data) {
      setDraft(resource.data.routing_toml);
    }
  }, [resource.data]);

  const dirty = !!resource.data && draft !== resource.data.routing_toml;
  const busy = operation !== null;
  const document = resource.data;
  const [status_title, status_description] = (() => {
    if (operation === "saving")
      return [
        "Validating and saving",
        "Checking the policy before writing it to disk.",
      ];
    if (operation === "applying")
      return [
        "Applying to gateway",
        "Waiting for the gateway to confirm the reload.",
      ];
    if (resource.loading)
      return [
        "Loading configuration",
        "Reading your routing policy from disk.",
      ];
    if (!document)
      return ["Configuration unavailable", "Reload from disk to try again."];
    if (resource.error)
      return [
        "Reload failed",
        "Showing the last loaded policy. Your draft is retained.",
      ];
    if (dirty) return ["Unsaved changes", "Validate and save before applying."];
    switch (apply_state) {
      case "pending":
        return [
          "Saved · apply pending",
          "Apply to reload the running gateway.",
        ];
      case "applied":
        return ["Gateway reloaded", "The last reload completed successfully."];
      case "unconfirmed":
        return [
          "Saved · apply unconfirmed",
          "The saved file remains on disk. Retry to confirm apply.",
        ];
      case "unknown":
        return ["Saved on disk", "Runtime configuration has not been checked."];
    }
  })();

  async function save() {
    if (!document || !dirty || busy || resource.loading) return;
    setOperation("saving");
    setError("");
    setMessage("");
    try {
      resource.setData(await api.saveRouting(document, draft));
      setApplyState("pending");
      setMessage(
        "Validated and saved to disk. Apply to the running gateway, or use this configuration on its next start.",
      );
    } catch (error) {
      setError(String(error));
    } finally {
      setOperation(null);
    }
  }

  async function apply() {
    if (!document || dirty || busy || resource.loading || resource.error)
      return;
    setOperation("applying");
    setError("");
    setMessage("");
    try {
      setMessage(await api.reload());
      setApplyState("applied");
    } catch (error) {
      setApplyState("unconfirmed");
      setError(String(error));
    } finally {
      setOperation(null);
    }
  }

  async function readFromDisk() {
    if (busy || resource.loading) return;
    setError("");
    setMessage("");
    setApplyState("unknown");
    await resource.refresh();
  }

  return (
    <div className="routing-page">
      <PageHeading
        eyebrow="GATEWAY CONFIGURATION"
        title="Routing"
        description="Choose how requests reach your providers."
      >
        <div className="actions routing-page-actions">
          <button
            type="button"
            className="routing-refresh"
            disabled={busy || resource.loading}
            onClick={() =>
              dirty ? setConfirmReload(true) : void readFromDisk()
            }
          >
            <RefreshCw size={15} />
            Reload from disk
          </button>
          <button
            type="button"
            disabled={!document || busy || resource.loading}
            aria-pressed={view === "advanced"}
            onClick={() => setView(view === "browse" ? "advanced" : "browse")}
          >
            {view === "advanced" ? (
              <ArrowLeft size={15} />
            ) : (
              <FileCode2 size={15} />
            )}
            {view === "advanced" ? "Back to routes" : "Advanced editor"}
          </button>
        </div>
      </PageHeading>

      <ErrorMessage error={error || resource.error} />
      {message && (
        <div className="routing-feedback" role="status">
          <Check size={16} aria-hidden="true" />
          <span>{message}</span>
        </div>
      )}

      {!!document?.overlay_paths.length && (
        <div className="routing-overlay">
          <Info size={17} aria-hidden="true" />
          <div>
            <strong>Configuration fragments can override this policy</strong>
            <p>
              The list includes these fragments. Advanced edits only the primary
              file; fragments load afterwards and may override its profiles and
              model scores.
            </p>
            <details>
              <summary>
                Show {document.overlay_paths.length}{" "}
                {document.overlay_paths.length === 1 ? "fragment" : "fragments"}
              </summary>
              <ul>
                {document.overlay_paths.map((path) => (
                  <li key={path}>
                    <code>{path}</code>
                  </li>
                ))}
              </ul>
            </details>
          </div>
        </div>
      )}

      {dirty && view === "browse" && (
        <div className="routing-draft-notice" role="status">
          <span>
            You have unsaved edits in Advanced. This list shows the saved
            configuration.
          </span>
          <button
            type="button"
            disabled={busy}
            onClick={() => setView("advanced")}
          >
            Return to draft
          </button>
        </div>
      )}
      {document ? (
        <>
          <div hidden={view !== "browse"}>
            {document.overview ? (
              <RoutingBrowser
                overview={document.overview}
                schema={document.schema}
                config_path={document.config_path}
                disabled={busy || resource.loading}
                onEdit={() => setView("advanced")}
              />
            ) : (
              <div className="routing-overview-unavailable">
                <h2>Routing summary unavailable</h2>
                <p>
                  Your configuration is still available in Advanced. Correct its
                  policy and save to rebuild this list.
                </p>
                <ErrorMessage
                  error={
                    document.overview_error ||
                    "The saved policy could not be summarized."
                  }
                />
                <button type="button" onClick={() => setView("advanced")}>
                  Open Advanced editor
                </button>
              </div>
            )}
          </div>
          <div hidden={view !== "advanced"}>
            <RoutingEditor
              document={document}
              draft={draft}
              visible={view === "advanced"}
              disabled={busy || resource.loading}
              onChange={(value) => {
                setDraft(value);
                setMessage("");
              }}
            />
          </div>
        </>
      ) : (
        <Empty>
          {resource.loading
            ? "Loading routing configuration…"
            : "Configuration could not be loaded. Reload from disk to try again."}
        </Empty>
      )}
      {confirm_reload && (
        <Modal
          title="Reload routing configuration?"
          onClose={() => setConfirmReload(false)}
        >
          <p className="routing-confirm-description">
            This replaces your unsaved routing draft with the file on disk. Your
            draft is kept if the file cannot be read.
          </p>
          <div className="actions routing-confirm-actions">
            <button type="button" onClick={() => setConfirmReload(false)}>
              Keep editing
            </button>
            <button
              type="button"
              className="danger"
              onClick={() => {
                setConfirmReload(false);
                void readFromDisk();
              }}
            >
              Discard draft & reload
            </button>
          </div>
        </Modal>
      )}

      <div className="routing-save-bar">
        <div className="routing-save-state" role="status">
          <span
            className={`routing-state-dot ${dirty || apply_state === "pending" || apply_state === "unconfirmed" ? "routing-state-pending" : ""}`}
            aria-hidden="true"
          />
          <div>
            <strong>{status_title}</strong>
            <span>{status_description}</span>
          </div>
        </div>
        <div className="routing-save-actions">
          <button
            type="button"
            disabled={
              busy || dirty || !document || resource.loading || !!resource.error
            }
            onClick={() => void apply()}
          >
            {operation === "applying" ? (
              <RefreshCw size={15} className="routing-spinning" />
            ) : (
              <ArrowRight size={15} />
            )}
            {operation === "applying" ? "Applying…" : "Apply to gateway"}
          </button>
          <button
            type="button"
            className="primary"
            disabled={busy || !dirty || resource.loading}
            onClick={() => void save()}
          >
            <Save size={15} />
            {operation === "saving"
              ? "Validating & saving…"
              : "Validate & save"}
          </button>
        </div>
      </div>
    </div>
  );
}
