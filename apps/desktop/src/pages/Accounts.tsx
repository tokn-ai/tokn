import { useState } from "react";
import { ChevronRight, Plus, RefreshCw, Search } from "lucide-react";
import { api } from "../lib/tauri";
import { useResource } from "../lib/useResource";
import type {
  AccountActivation,
  AccountEdit,
  AccountSummary,
} from "../lib/types";
import { Empty, ErrorMessage, PageHeading } from "../components/Feedback";
import { AccountUsage } from "../components/AccountUsage";
import { AddAccount } from "../components/accounts/AddAccount";
import {
  AccountDetails,
  type AccountDraft,
} from "../components/accounts/AccountDetails";
import "./accounts.css";

const activationOf = (account: AccountSummary): AccountActivation =>
  account.enabled ? account.tier : "disabled";

function hasChanges(account: AccountSummary, draft?: AccountDraft) {
  return (
    !!draft &&
    (draft.label.trim() !== (account.label ?? "") ||
      draft.activation !== activationOf(account))
  );
}

export function Accounts() {
  const accounts = useResource(api.accounts);
  const [selected, setSelected] = useState("");
  const [adding, setAdding] = useState(false);
  const [working, setWorking] = useState(false);
  const [message, setMessage] = useState("");
  const [search, setSearch] = useState("");
  const [provider_filter, setProviderFilter] = useState("");
  const [activation_filter, setActivationFilter] = useState<
    AccountActivation | ""
  >("");
  const [drafts, setDrafts] = useState<Record<string, AccountDraft>>({});
  const all_accounts = accounts.data ?? [];
  const providers = [
    ...new Set(all_accounts.map((account) => account.provider)),
  ];
  const visible = all_accounts.filter(
    (account) =>
      (!provider_filter || account.provider === provider_filter) &&
      (!activation_filter || activationOf(account) === activation_filter) &&
      `${account.id} ${account.label ?? ""} ${account.provider} ${account.username ?? ""}`
        .toLowerCase()
        .includes(search.trim().toLowerCase()),
  );
  const account =
    visible.find((account) => account.id === selected) ?? visible[0];
  const draft_count = all_accounts.filter((item) =>
    hasChanges(item, drafts[item.id]),
  ).length;

  function updateDraft(id: string, draft: AccountDraft) {
    setDrafts((current) => ({ ...current, [id]: draft }));
  }

  async function saved(edit?: AccountEdit) {
    const action = !edit
      ? "added"
      : edit.action === "remove"
        ? "removed"
        : "saved";
    if (edit) {
      // The backend has confirmed this mutation. Keep that truth visible even
      // if the following read fails; disabling preserves the stored tier.
      accounts.setData(
        all_accounts.flatMap((item) => {
          if (item.id !== edit.id) return [item];
          if (edit.action === "remove") return [];
          return [
            {
              ...item,
              label: edit.label?.trim() || null,
              enabled: edit.activation !== "disabled",
              tier:
                edit.activation === "disabled" ? item.tier : edit.activation,
            },
          ];
        }),
      );
      setDrafts((current) => {
        const next = { ...current };
        delete next[edit.id];
        return next;
      });
    }
    let refresh_failed = false;
    try {
      accounts.setData(await api.accounts());
    } catch {
      refresh_failed = true;
    }
    const refresh_note = refresh_failed
      ? " The account list could not refresh. Refresh the list to see the latest account details."
      : "";
    setMessage(`Account ${action}.${refresh_note}`);
    try {
      const status = await api.status();
      if (status.state === "running") {
        await api.reload();
        setMessage(
          `Account ${action} and applied to the gateway.${refresh_note}`,
        );
      } else {
        setMessage(
          `Account ${action}. The change will apply when the gateway starts.${refresh_note}`,
        );
      }
    } catch {
      setMessage(
        `Account ${action}. Reload or restart the gateway to apply the change.${refresh_note}`,
      );
    }
  }

  function clearFilters() {
    setSearch("");
    setProviderFilter("");
    setActivationFilter("");
  }

  return (
    <div className="accounts-page">
      <PageHeading
        eyebrow="PROVIDER ACCESS"
        title="Accounts"
        description="Connect providers and choose which accounts your gateway uses."
      >
        <div className="actions">
          <button
            disabled={accounts.loading || working}
            onClick={() => void accounts.refresh()}
          >
            <RefreshCw size={15} />
            Refresh
          </button>
          <button
            className="primary"
            disabled={working}
            onClick={() => setAdding(true)}
          >
            <Plus size={16} />
            Add account
          </button>
        </div>
      </PageHeading>
      <ErrorMessage error={accounts.error} />
      {message && (
        <div className="notice" role="status">
          {message}
        </div>
      )}
      {adding && (
        <AddAccount onSaved={() => saved()} onClose={() => setAdding(false)} />
      )}
      <div className="accounts-toolbar">
        <label className="accounts-search">
          <Search size={16} aria-hidden="true" />
          <input
            aria-label="Search accounts"
            type="search"
            disabled={working}
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search accounts…"
          />
        </label>
        <label className="accounts-provider-filter">
          <span className="accounts-field-label">Provider</span>
          <select
            aria-label="Filter by provider"
            disabled={working}
            value={provider_filter}
            onChange={(event) => setProviderFilter(event.target.value)}
          >
            <option value="">All providers</option>
            {providers.map((provider) => (
              <option key={provider}>{provider}</option>
            ))}
          </select>
        </label>
        <div
          className="accounts-filter-group"
          role="group"
          aria-label="Filter by activation"
        >
          {(["", "active", "fallback", "disabled"] as const).map((filter) => (
            <button
              key={filter}
              disabled={working}
              aria-pressed={activation_filter === filter}
              onClick={() => setActivationFilter(filter)}
            >
              {filter ? filter[0].toUpperCase() + filter.slice(1) : "All"}
            </button>
          ))}
        </div>
      </div>
      {draft_count > 0 && (
        <p className="accounts-draft-note" role="status">
          <span className="accounts-draft-dot" />
          {draft_count} {draft_count === 1 ? "account has" : "accounts have"}{" "}
          unsaved edits. Drafts are kept as you switch accounts or filters.
        </p>
      )}
      {accounts.loading && !accounts.data ? (
        <Empty>Loading accounts…</Empty>
      ) : accounts.error && !accounts.data ? (
        <Empty>Accounts could not be loaded. Refresh to try again.</Empty>
      ) : !all_accounts.length ? (
        <div className="accounts-empty">
          <div className="accounts-empty-symbol">+</div>
          <h2>Connect your first provider</h2>
          <p>Sign in or import a credential to start routing requests.</p>
          <button className="primary" onClick={() => setAdding(true)}>
            <Plus size={16} />
            Add account
          </button>
        </div>
      ) : !visible.length ? (
        <div className="accounts-empty">
          <Search size={25} />
          <h2>No accounts match</h2>
          <p>Try another search or reset the filters.</p>
          <button onClick={clearFilters}>Clear filters</button>
        </div>
      ) : (
        <div className="accounts-workspace">
          <section className="accounts-list" aria-label="Provider accounts">
            <div className="accounts-list-heading">
              <strong>Your accounts</strong>
              <span>
                {visible.length} of {all_accounts.length}
              </span>
            </div>
            {providers
              .filter((provider) =>
                visible.some((item) => item.provider === provider),
              )
              .map((provider) => (
                <section className="accounts-provider-group" key={provider}>
                  <h3>
                    {provider}
                    <span>
                      {
                        visible.filter((item) => item.provider === provider)
                          .length
                      }
                    </span>
                  </h3>
                  {visible
                    .filter((item) => item.provider === provider)
                    .map((item) => (
                      <button
                        key={item.id}
                        disabled={working}
                        className={`accounts-list-row ${account.id === item.id ? "is-selected" : ""}`}
                        aria-pressed={account.id === item.id}
                        onClick={() => setSelected(item.id)}
                      >
                        <span className="accounts-list-identity">
                          <strong>{item.label || item.id}</strong>
                          <small>{item.username || item.id}</small>
                          <span className="accounts-list-state">
                            <span
                              className={`account-activation account-activation-${activationOf(item)}`}
                            >
                              {activationOf(item)}
                            </span>
                            {item.credential_status !== "stored" && (
                              <span className="accounts-credential-warning">
                                Credential {item.credential_status}
                              </span>
                            )}
                            {hasChanges(item, drafts[item.id]) && (
                              <span className="accounts-unsaved">Unsaved</span>
                            )}
                          </span>
                        </span>
                        <ChevronRight size={15} aria-hidden="true" />
                      </button>
                    ))}
                </section>
              ))}
          </section>
          <AccountDetails
            key={account.id}
            account={account}
            working={working}
            draft={
              drafts[account.id] ?? {
                label: account.label ?? "",
                activation: activationOf(account),
              }
            }
            onDraftChange={(draft) => updateDraft(account.id, draft)}
            onSaved={saved}
            onChecked={accounts.refresh}
            onBusyChange={setWorking}
          />
        </div>
      )}
      {!!all_accounts.length && <AccountUsage account={account} />}
    </div>
  );
}
