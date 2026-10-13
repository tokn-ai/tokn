import { useState } from "react";
import { Empty, ErrorMessage, number } from "./Feedback";
import { api } from "../lib/tauri";
import type { AccountSummary } from "../lib/types";
import { useResource } from "../lib/useResource";

export function AccountUsage({ account }: { account?: AccountSummary }) {
  const usage = useResource(api.usage);
  const [scope, setScope] = useState<"selected" | "all">("selected");
  const rows = (usage.data ?? []).filter(
    (row) => scope === "all" || !account || row.account === account.id,
  );
  return (
    <section className="account-usage">
      <div className="account-usage-heading">
        <div>
          <h2>Recorded usage</h2>
          <p>Past 24 hours · local request history</p>
        </div>
        {account && (
          <div
            className="accounts-filter-group"
            role="group"
            aria-label="Usage scope"
          >
            <button
              aria-pressed={scope === "selected"}
              onClick={() => setScope("selected")}
            >
              This account
            </button>
            <button
              aria-pressed={scope === "all"}
              onClick={() => setScope("all")}
            >
              All accounts
            </button>
          </div>
        )}
      </div>
      <ErrorMessage error={usage.error} />
      {usage.loading && !usage.data ? (
        <Empty>Loading usage…</Empty>
      ) : usage.error && !usage.data ? null : !rows.length ? (
        <p className="account-usage-empty">
          No recorded usage for{" "}
          {scope === "selected" && account
            ? account.label || account.id
            : "these accounts"}{" "}
          in the past 24 hours.
        </p>
      ) : (
        <div className="account-usage-table">
          <table>
            <thead>
              <tr>
                <th scope="col">Model</th>
                <th scope="col">Requests</th>
                <th scope="col">Input tokens</th>
                <th scope="col">Output tokens</th>
                <th scope="col">Cache read</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={`${row.model}-${row.provider}-${row.account}`}>
                  <td>
                    <strong>{row.model}</strong>
                    <small>
                      {row.provider ?? "Unassigned"} /{" "}
                      {row.account ?? "Unassigned"}
                    </small>
                  </td>
                  <td title={row.requests.toLocaleString()}>
                    {number(row.requests)}
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
      )}
      <p className="account-help account-usage-footnote">
        Provider quota is checked separately from this recorded usage.
      </p>
    </section>
  );
}
