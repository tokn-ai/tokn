import { useState } from "react";
import { Check, RefreshCw, ShieldCheck, Trash2 } from "lucide-react";
import { api } from "../../lib/tauri";
import type {
  AccountActivation,
  AccountEdit,
  AccountProbe,
  AccountSummary,
} from "../../lib/types";
import { ErrorMessage } from "../Feedback";

export interface AccountDraft {
  label: string;
  activation: AccountActivation;
}

const date = (seconds: number | null) =>
  seconds != null ? new Date(seconds * 1000).toLocaleString() : "Not reported";
const percent = (value: number) => Math.max(0, Math.min(100, value));
const words = (value: string) => value.replaceAll("_", " ");

export function AccountDetails({
  account,
  working,
  draft,
  onDraftChange,
  onSaved,
  onChecked,
  onBusyChange,
}: {
  account: AccountSummary;
  working: boolean;
  draft: AccountDraft;
  onDraftChange: (draft: AccountDraft) => void;
  onSaved: (edit: AccountEdit) => Promise<void>;
  onChecked: () => Promise<void>;
  onBusyChange: (busy: boolean) => void;
}) {
  const [probe, setProbe] = useState<AccountProbe>();
  const [busy, setBusy] = useState("");
  const [error, setError] = useState("");
  const [removing, setRemoving] = useState(false);
  const pending = working || !!busy;
  const original_activation = account.enabled ? account.tier : "disabled";
  const dirty =
    draft.label.trim() !== (account.label ?? "") ||
    draft.activation !== original_activation;

  async function run(action: string, operation: () => Promise<void>) {
    if (pending) return;
    setBusy(action);
    onBusyChange(true);
    setError("");
    try {
      await operation();
    } catch (error) {
      setError(String(error));
    } finally {
      setBusy("");
      onBusyChange(false);
    }
  }

  async function check(force: boolean) {
    await run(force ? "refreshing" : "checking", async () => {
      setProbe(await api.probeAccount(account.id, force));
      await onChecked();
    });
  }

  return (
    <section
      className="account-detail"
      aria-label={`Manage ${account.id}`}
      aria-busy={pending}
    >
      <header className="account-detail-header">
        <div className="account-avatar" aria-hidden="true">
          {account.provider.slice(0, 1).toUpperCase()}
        </div>
        <div className="account-detail-title">
          <p>{account.provider}</p>
          <h2>{account.label || account.id}</h2>
          <span>{account.username || account.id}</span>
        </div>
        <span
          className={`account-activation account-activation-${original_activation}`}
        >
          {original_activation}
        </span>
      </header>
      <ErrorMessage error={error} />
      <section className="account-detail-section">
        <div className="account-section-heading">
          <h3>Credentials</h3>
          <span
            className={`account-health ${account.credential_status === "stored" ? "" : "account-health-warning"}`}
          >
            {words(account.credential_status)} locally
          </span>
        </div>
        <dl className="account-facts">
          <div>
            <dt>Account ID</dt>
            <dd className="account-id">{account.id}</dd>
          </div>
          <div>
            <dt>Credential type</dt>
            <dd>{words(account.credential_kind)}</dd>
          </div>
          <div>
            <dt>Expires</dt>
            <dd>{date(account.expires_at)}</dd>
          </div>
          <div>
            <dt>Last refreshed</dt>
            <dd>{date(account.last_refresh)}</dd>
          </div>
        </dl>
        <p className="account-help">
          Local status describes stored credentials. Check with the provider for
          current authentication and quota.
        </p>
        <div className="account-inline-actions">
          <button disabled={pending} onClick={() => void check(false)}>
            <ShieldCheck size={15} />
            {busy === "checking" ? "Checking…" : "Check status & quota"}
          </button>
          {account.can_refresh && (
            <button disabled={pending} onClick={() => void check(true)}>
              <RefreshCw size={15} />
              {busy === "refreshing" ? "Refreshing…" : "Refresh credentials"}
            </button>
          )}
        </div>
      </section>
      {probe && (
        <section
          className="account-detail-section account-quota"
          aria-label="Provider authentication and quota"
        >
          <div className="account-section-heading">
            <h3>Provider quota</h3>
            <span
              className={`account-health ${probe.authentication === "failed" ? "account-health-warning" : ""}`}
            >
              Auth: {probe.authentication}
            </span>
          </div>
          <p className="account-help">
            Checked {date(probe.checked_at)}
            {probe.plan ? ` · ${probe.plan}` : ""}
          </p>
          {probe.message && (
            <p className="account-quota-message" role="status">
              {probe.message}
            </p>
          )}
          {probe.quota_status === "unsupported" ? (
            <p className="account-help">This provider does not report quota.</p>
          ) : probe.quota_status === "unavailable" ? (
            <p className="account-help">
              Quota is unavailable. Remaining allowance is unknown.
            </p>
          ) : (
            <>
              {probe.headline && (
                <p className="account-quota-headline">{probe.headline}</p>
              )}
              <div className="account-quota-grid">
                {probe.metered && (
                  <div className="account-quota-bucket">
                    <span className="account-quota-label">
                      {probe.metered.label}
                    </span>
                    <strong>
                      {probe.metered.remaining.toLocaleString()}{" "}
                      <small>remaining</small>
                    </strong>
                    {probe.metered.entitlement != null ? (
                      <>
                        <span className="account-help">
                          of {probe.metered.entitlement.toLocaleString()}{" "}
                          allowance
                        </span>
                        {probe.metered.entitlement > 0 && (
                          <progress
                            aria-label={`${probe.metered.label} remaining`}
                            max={100}
                            value={percent(
                              (probe.metered.remaining /
                                probe.metered.entitlement) *
                                100,
                            )}
                          />
                        )}
                      </>
                    ) : (
                      <span className="account-help">Unlimited allowance</span>
                    )}
                  </div>
                )}
                {probe.secondary.map((bucket, index) => {
                  const used_percent =
                    bucket.percent_used ??
                    (bucket.total != null &&
                    bucket.total > 0 &&
                    bucket.used != null
                      ? (bucket.used / bucket.total) * 100
                      : undefined);
                  const remaining_percent =
                    used_percent != null
                      ? 100 - percent(used_percent)
                      : undefined;
                  return (
                    <div
                      className="account-quota-bucket"
                      key={`${bucket.label}-${index}`}
                    >
                      <span className="account-quota-label">
                        {bucket.label}
                      </span>
                      <strong>
                        {remaining_percent != null
                          ? `${remaining_percent.toFixed(1)}%`
                          : bucket.used != null
                            ? bucket.used.toLocaleString()
                            : "—"}{" "}
                        <small>
                          {remaining_percent != null
                            ? "remaining"
                            : bucket.used != null
                              ? "used"
                              : "not reported"}
                        </small>
                      </strong>
                      {used_percent != null && (
                        <span className="account-help">
                          {used_percent.toFixed(1)}% used
                          {bucket.total != null
                            ? ` of ${bucket.total.toLocaleString()}`
                            : ""}
                        </span>
                      )}
                      {used_percent == null && bucket.total != null && (
                        <span className="account-help">
                          {bucket.total.toLocaleString()} allowance
                        </span>
                      )}
                      {remaining_percent != null && (
                        <progress
                          aria-label={`${bucket.label} remaining`}
                          max={100}
                          value={remaining_percent}
                        />
                      )}
                      {bucket.reset_at_ms != null && (
                        <span className="account-quota-reset">
                          Resets {new Date(bucket.reset_at_ms).toLocaleString()}
                        </span>
                      )}
                    </div>
                  );
                })}
              </div>
              {probe.reset_date && (
                <p className="account-help">Resets {probe.reset_date}</p>
              )}
            </>
          )}
        </section>
      )}
      <section className="account-detail-section">
        <div className="account-section-heading">
          <h3>Routing preferences</h3>
          {dirty && <span className="accounts-unsaved">Unsaved changes</span>}
        </div>
        <fieldset disabled={pending} className="account-form">
          <label>
            Display name
            <input
              value={draft.label}
              placeholder={account.id}
              onChange={(event) =>
                onDraftChange({ ...draft, label: event.target.value })
              }
            />
          </label>
          <label>
            Account activation
            <select
              value={draft.activation}
              onChange={(event) =>
                onDraftChange({
                  ...draft,
                  activation: event.target.value as AccountActivation,
                })
              }
            >
              <option value="active">Active · use first</option>
              <option value="fallback">Fallback · use when needed</option>
              <option value="disabled">Disabled · do not use</option>
            </select>
          </label>
          <p className="account-help">
            Active accounts are tried first. Fallback accounts are used when
            active accounts cannot serve the request. Shared across profiles.
          </p>
          <div className="account-inline-actions">
            <button
              className="primary"
              disabled={pending || !dirty}
              onClick={() =>
                void run("saving", async () => {
                  const edit: AccountEdit = {
                    action: "update",
                    id: account.id,
                    label: draft.label.trim() || null,
                    activation: draft.activation,
                  };
                  await api.editAccount(edit);
                  await onSaved(edit);
                })
              }
            >
              <Check size={15} />
              {busy === "saving" ? "Saving…" : "Save changes"}
            </button>
            {dirty && (
              <button
                onClick={() =>
                  onDraftChange({
                    label: account.label ?? "",
                    activation: original_activation,
                  })
                }
              >
                Discard edits
              </button>
            )}
          </div>
        </fieldset>
      </section>
      <footer className="account-remove">
        {!removing ? (
          <button
            className="account-remove-trigger"
            disabled={pending}
            onClick={() => setRemoving(true)}
          >
            <Trash2 size={14} />
            Remove account
          </button>
        ) : (
          <div className="account-remove-confirmation">
            <strong>Remove {account.id}?</strong>
            <p>
              This removes its local credential. Routes selecting this account
              may stop working. Provider access is not revoked.
            </p>
            <div className="account-inline-actions">
              <button disabled={pending} onClick={() => setRemoving(false)}>
                Keep account
              </button>
              <button
                className="danger"
                disabled={pending}
                onClick={() =>
                  void run("removing", async () => {
                    const edit: AccountEdit = {
                      action: "remove",
                      id: account.id,
                    };
                    await api.editAccount(edit);
                    await onSaved(edit);
                  })
                }
              >
                {busy === "removing" ? "Removing…" : "Remove account"}
              </button>
            </div>
          </div>
        )}
      </footer>
    </section>
  );
}
