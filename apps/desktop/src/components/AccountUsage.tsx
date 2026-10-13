import { RefreshCw } from "lucide-react";
import { Empty, ErrorMessage, number } from "./Feedback";
import { api } from "../lib/tauri";
import type { AccountSummary } from "../lib/types";
import { useResource } from "../lib/useResource";

export function AccountUsage({
  account,
  disabled = false,
}: {
  account: AccountSummary;
  disabled?: boolean;
}) {
  const usage = useResource(api.usage);
  const rows = (usage.data ?? []).filter((row) => row.account === account.id);
  return (
    <section
      className="account-usage"
      aria-label={`Recorded usage for ${account.id}`}
    >
      <div className="account-usage-heading">
        <div>
          <h3>Recorded usage</h3>
          <p>Past 24 hours · this account</p>
        </div>
        <button
          disabled={disabled || usage.loading}
          onClick={() => void usage.refresh()}
        >
          <RefreshCw size={15} />
          {usage.loading ? "Refreshing…" : usage.error ? "Retry" : "Refresh"}
        </button>
      </div>
      <ErrorMessage error={usage.error} />
      {usage.error && usage.data && (
        <p className="account-usage-stale" role="status">
          Refresh failed. Previously loaded usage is shown below.
        </p>
      )}
      {usage.loading && !usage.data ? (
        <Empty>Loading usage…</Empty>
      ) : usage.error && !usage.data ? (
        <p className="account-help">
          Recorded usage could not be loaded. Retry to check this account’s
          activity.
        </p>
      ) : !rows.length ? (
        <p className="account-usage-empty">
          No recorded usage for this account in the past 24 hours.
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
                    <small>{row.provider ?? "Unassigned"}</small>
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
