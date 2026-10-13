import { useRef, useState } from "react";
import { Plus, RefreshCw, Search } from "lucide-react";
import { api } from "../lib/tauri";
import { useResource } from "../lib/useResource";
import type {
  AccountActivation,
  AccountEdit,
  AccountSummary,
} from "../lib/types";
import { Empty, ErrorMessage, PageHeading } from "../components/Feedback";
import { Modal } from "../components/Modal";
import { AccountList } from "../components/accounts/AccountList";
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
  const directory = useRef<HTMLDivElement>(null);
  const [selected, setSelected] = useState("");
  const [adding, setAdding] = useState(false);
  const [working, setWorking] = useState(false);
  const [message, setMessage] = useState("");
  const [search, setSearch] = useState("");
  const [provider_filter, setProviderFilter] = useState("");
  const [activation_filter, setActivationFilter] = useState<
    AccountActivation | ""
  >("");
  const [drafts, setDrafts] = useState(() => new Map<string, AccountDraft>());
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
  const account = all_accounts.find((item) => item.id === selected);
  const draft_ids = new Set(
    all_accounts
      .filter((item) => hasChanges(item, drafts.get(item.id)))
      .map((item) => item.id),
  );
  const draft_count = draft_ids.size;

  function updateDraft(id: string, draft: AccountDraft) {
    setDrafts((current) => new Map(current).set(id, draft));
  }

  async function saved(edit?: AccountEdit) {
    const action = !edit
      ? "added"
      : edit.action === "remove"
        ? "removed"
        : "saved";
    if (edit) {
      if (edit.action === "remove") {
        setSelected("");
        requestAnimationFrame(() =>
          directory.current?.focus({ preventScroll: true }),
        );
      }
      // The backend has confirmed this mutation. Keep that truth visible even
      // if the following read fails; disabling preserves the stored tier.
      accounts.setData((current) =>
        (current ?? []).flatMap((item) => {
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
        const next = new Map(current);
        next.delete(edit.id);
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
    <div className="accounts-page" ref={directory} tabIndex={-1}>
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
          unsaved edits. Drafts stay here when you close a panel or switch
          pages.
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
          <button
            className="primary"
            disabled={working}
            onClick={() => setAdding(true)}
          >
            <Plus size={16} />
            Add account
          </button>
        </div>
      ) : !visible.length ? (
        <div className="accounts-empty">
          <Search size={25} />
          <h2>No accounts match</h2>
          <p>Try another search or reset the filters.</p>
          <button disabled={working} onClick={clearFilters}>
            Clear filters
          </button>
        </div>
      ) : (
        <AccountList
          accounts={visible}
          total_count={all_accounts.length}
          draft_ids={draft_ids}
          working={working}
          onOpen={(id) => {
            setMessage("");
            setSelected(id);
          }}
        />
      )}
      {account && (
        <Modal
          title={account.label || account.id}
          description={`${account.provider} · ${account.username || account.id}`}
          class_name="account-panel"
          return_focus={directory}
          busy={working}
          onClose={() => setSelected("")}
        >
          <AccountDetails
            key={account.id}
            account={account}
            working={working}
            message={message}
            load_error={accounts.error}
            draft={
              drafts.get(account.id) ?? {
                label: account.label ?? "",
                activation: activationOf(account),
              }
            }
            onDraftChange={(draft) => updateDraft(account.id, draft)}
            onSaved={saved}
            onChecked={accounts.refresh}
            onBusyChange={setWorking}
          />
        </Modal>
      )}
    </div>
  );
}
