import type { UsageSummary } from "./types";

export interface UsageTotals {
  requests: number;
  input_tokens: number;
  output_tokens: number;
  cached_tokens: number;
}

export interface ModelUsage extends UsageTotals {
  model: string;
  providers: string[];
}

export function summarizeUsage(rows: UsageSummary[]) {
  const totals: UsageTotals = {
    requests: 0,
    input_tokens: 0,
    output_tokens: 0,
    cached_tokens: 0,
  };
  const models = new Map<string, ModelUsage>();
  for (const row of rows) {
    totals.requests += row.requests;
    totals.input_tokens += row.input_tokens;
    totals.output_tokens += row.output_tokens;
    totals.cached_tokens += row.cached_tokens;
    const current = models.get(row.model) ?? {
      model: row.model,
      providers: [],
      requests: 0,
      input_tokens: 0,
      output_tokens: 0,
      cached_tokens: 0,
    };
    if (row.provider && !current.providers.includes(row.provider)) {
      current.providers.push(row.provider);
    }
    current.requests += row.requests;
    current.input_tokens += row.input_tokens;
    current.output_tokens += row.output_tokens;
    current.cached_tokens += row.cached_tokens;
    models.set(row.model, current);
  }
  return {
    totals,
    models: [...models.values()].sort(
      (left, right) =>
        right.requests - left.requests || left.model.localeCompare(right.model),
    ),
  };
}
