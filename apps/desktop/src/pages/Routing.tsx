import { useEffect, useRef, useState } from "react";
import {
  ArrowRight,
  Check,
  ChevronDown,
  FileCode2,
  Info,
  RefreshCw,
  Save,
} from "lucide-react";
import { api } from "../lib/tauri";
import { useResource } from "../lib/useResource";
import { ErrorMessage, PageHeading } from "../components/Feedback";
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
  const [cursor, setCursor] = useState({ line: 1, column: 1 });
  const line_numbers = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (resource.data) {
      setDraft(resource.data.routing_toml);
      setCursor({ line: 1, column: 1 });
    }
  }, [resource.data]);

  const dirty = !!resource.data && draft !== resource.data.routing_toml;
  const busy = operation !== null;
  const document = resource.data;
  const filename = document?.config_path.split(/[\\/]/).pop();
  const line_count = draft.split("\n").length;
  const editor_disabled = !document || busy || resource.loading;
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

  function updateCursor(element: HTMLTextAreaElement) {
    const before = element.value.slice(0, element.selectionStart);
    const lines = before.split("\n");
    setCursor({ line: lines.length, column: lines.at(-1)!.length + 1 });
  }

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
    if (
      dirty &&
      !window.confirm(
        "Discard your unsaved routing changes and reload from disk?",
      )
    )
      return;
    setError("");
    setMessage("");
    setApplyState("unknown");
    await resource.refresh();
  }

  return (
    <div className="routing-page">
      <PageHeading eyebrow="GATEWAY CONFIGURATION" title="Routing">
        <button
          type="button"
          className="routing-refresh"
          disabled={busy || resource.loading}
          onClick={() => void readFromDisk()}
        >
          <RefreshCw size={15} />
          Reload from disk
        </button>
      </PageHeading>
      <p className="routing-intro">
        Choose how requests reach your providers. Edit your policy, save it,
        then apply it when you’re ready.
      </p>

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
              You’re editing the primary file. These fragments load afterwards
              and may override its profiles and model scores.
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

      <section
        className="routing-editor"
        aria-label="Routing policy editor"
        aria-busy={resource.loading || operation === "saving"}
      >
        <div className="routing-editor-header">
          <div className="routing-file">
            <FileCode2 size={18} aria-hidden="true" />
            <strong>{filename || "Configuration"}</strong>
            {document && (
              <span className="routing-schema">
                {document.schema === "v2" ? "v2" : "Legacy"}
              </span>
            )}
          </div>
          <span className="routing-language">TOML</span>
        </div>

        <div className="routing-editor-body">
          <div className="routing-gutter" aria-hidden="true">
            <div className="routing-line-numbers" ref={line_numbers}>
              <pre>
                {Array.from(
                  { length: line_count },
                  (_, index) => index + 1,
                ).join("\n")}
              </pre>
            </div>
          </div>
          <textarea
            className="routing-input"
            aria-label="Routing configuration in TOML"
            aria-describedby="routing-editor-help"
            spellCheck={false}
            autoCapitalize="off"
            autoCorrect="off"
            wrap="off"
            value={draft}
            disabled={editor_disabled}
            onChange={(event) => {
              setDraft(event.target.value);
              setMessage("");
              updateCursor(event.currentTarget);
            }}
            onClick={(event) => updateCursor(event.currentTarget)}
            onKeyUp={(event) => updateCursor(event.currentTarget)}
            onScroll={(event) => {
              if (line_numbers.current)
                line_numbers.current.scrollTop = event.currentTarget.scrollTop;
            }}
            placeholder={
              resource.loading
                ? "Loading your routing policy…"
                : document
                  ? "No routing policy sections are configured yet."
                  : "Reload from disk to load your configuration."
            }
          />
        </div>
        <div className="routing-editor-footer">
          <span id="routing-editor-help">
            Validated against your complete configuration before saving
          </span>
          <span>
            Ln {cursor.line}, Col {cursor.column}
          </span>
        </div>
      </section>

      {document && (
        <p className="routing-path" title={document.config_path}>
          {document.config_path}
        </p>
      )}

      <details className="routing-guide">
        <summary>
          <span>
            <Info size={16} aria-hidden="true" />A quick guide to routing policy
          </span>
          <ChevronDown size={16} aria-hidden="true" />
        </summary>
        <div className="routing-guide-content">
          <dl>
            <div>
              <dt>Defaults</dt>
              <dd>Set the policy requests use when no profile is selected.</dd>
            </div>
            <div>
              <dt>Profiles</dt>
              <dd>Define named policies for different agents or workflows.</dd>
            </div>
            {document?.schema === "v2" && (
              <div>
                <dt>Routes</dt>
                <dd>Define the routing options referenced by your policies.</dd>
              </div>
            )}
            <div>
              <dt>Model scores</dt>
              <dd>
                Prefer providers for exact model IDs or trailing wildcards such
                as <code>gpt-*</code>. Higher provider scores win.
              </dd>
            </div>
          </dl>
          <p>
            Listener and service settings stay in your config file. Saving
            updates the file; applying asks the running gateway to reload it.
          </p>
        </div>
      </details>

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
