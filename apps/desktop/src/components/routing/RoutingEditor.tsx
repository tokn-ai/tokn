import { useEffect, useId, useRef, useState } from "react";
import { ChevronDown, FileCode2, Info } from "lucide-react";
import type { RoutingDocument } from "../../lib/types";

export function RoutingEditor({
  document,
  draft,
  disabled,
  visible,
  onChange,
}: {
  document: RoutingDocument;
  draft: string;
  disabled: boolean;
  visible: boolean;
  onChange: (value: string) => void;
}) {
  const help_id = useId();
  const line_numbers = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLTextAreaElement>(null);
  const [cursor, setCursor] = useState({ line: 1, column: 1 });
  const filename = document.config_path.split(/[\\/]/).pop();
  const line_count = draft.split("\n").length;

  useEffect(() => {
    if (visible) input.current?.focus({ preventScroll: true });
  }, [visible]);

  useEffect(() => {
    if (input.current) updateCursor(input.current);
  }, [draft]);

  function updateCursor(element: HTMLTextAreaElement) {
    const lines = element.value.slice(0, element.selectionStart).split("\n");
    setCursor({ line: lines.length, column: lines.at(-1)!.length + 1 });
  }

  return (
    <>
      <section className="routing-editor" aria-label="Routing policy editor">
        <div className="routing-editor-header">
          <div className="routing-file">
            <FileCode2 size={18} aria-hidden="true" />
            <strong>{filename}</strong>
            <span className="routing-schema">
              {document.schema === "v2" ? "v2" : "Legacy"}
            </span>
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
            ref={input}
            className="routing-input"
            aria-label="Routing configuration in TOML"
            aria-describedby={help_id}
            spellCheck={false}
            autoCapitalize="off"
            autoCorrect="off"
            wrap="off"
            value={draft}
            disabled={disabled}
            onChange={(event) => {
              onChange(event.target.value);
              updateCursor(event.currentTarget);
            }}
            onSelect={(event) => updateCursor(event.currentTarget)}
            onScroll={(event) => {
              if (line_numbers.current)
                line_numbers.current.scrollTop = event.currentTarget.scrollTop;
            }}
            placeholder="No routing policy sections are configured yet."
          />
        </div>
        <div className="routing-editor-footer">
          <span id={help_id}>
            Validated against your complete configuration before saving
          </span>
          <span>
            Ln {cursor.line}, Col {cursor.column}
          </span>
        </div>
      </section>
      <p className="routing-path" title={document.config_path}>
        {document.config_path}
      </p>
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
              <dd>
                {document.schema === "v2"
                  ? "Create the default profile and route for the default API endpoint. Named profiles have their own policy."
                  : "Set the default policy and settings inherited by named profiles."}
              </dd>
            </div>
            <div>
              <dt>Profiles</dt>
              <dd>
                {document.schema === "v2"
                  ? "Choose a route, account pool, and API exposure for an agent or workflow."
                  : "Define named policies for different agents or workflows."}
              </dd>
            </div>
            {document.schema === "v2" && (
              <div>
                <dt>Routes</dt>
                <dd>
                  Define reusable request-handling policies. Multiple profiles
                  can share a route.
                </dd>
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
            Save validates all profiles and routes together. Apply reloads the
            running gateway. Listener and service settings stay in your config
            file.
          </p>
        </div>
      </details>
    </>
  );
}
