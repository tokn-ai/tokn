import type { RequestClassification } from "./types.js";

const SOURCE_LABELS: Record<RequestClassification["source"], string> = {
  endpoint: "Compaction endpoint",
  request_field: "Request field",
  codex_prompt: "Codex prompt",
  claude_code_prompt: "Claude Code prompt",
  opencode_prompt: "OpenCode prompt",
};

function jsonRecord(value: unknown): Record<string, unknown> | undefined {
  if (typeof value === "string") {
    try {
      value = JSON.parse(value) as unknown;
    } catch {
      return undefined;
    }
  }
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

export function readRequestClassification(
  request: Record<string, unknown>,
): RequestClassification | undefined {
  const parameters = jsonRecord(request.params_json);
  const classification = jsonRecord(
    parameters?.request_classification ?? request.request_classification,
  );
  if (
    classification?.purpose !== "compaction" ||
    typeof classification.source !== "string" ||
    !Object.hasOwn(SOURCE_LABELS, classification.source)
  ) {
    return undefined;
  }
  return {
    purpose: "compaction",
    source: classification.source as RequestClassification["source"],
  };
}

export function classificationSourceLabel(
  source: RequestClassification["source"],
): string {
  return SOURCE_LABELS[source];
}
