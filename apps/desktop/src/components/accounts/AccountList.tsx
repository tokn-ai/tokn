import { useId } from "react";
import { ChevronRight } from "lucide-react";
import type { AccountSummary } from "../../lib/types";
import "./AccountList.css";

interface AccountListProps {
  accounts: AccountSummary[];
  total_count: number;
  draft_ids: ReadonlySet<string>;
  working: boolean;
  onOpen: (id: string) => void;
}

const credential_labels: Record<string, string> = {
  api_key: "API key",
  oauth: "OAuth",
  access_token: "Access token",
};

function credentialLabel(kind: string) {
  return credential_labels[kind] ?? kind.replaceAll("_", " ");
}

function credentialStatus(status: string) {
  if (status === "stored") return "Stored locally";
  if (status === "expired") return "Expired locally";
  if (status === "missing") return "Missing locally";
  return `${status.replaceAll("_", " ")} locally`;
}

export function AccountList({
  accounts,
  total_count,
  draft_ids,
  working,
  onOpen,
}: AccountListProps) {
  const list_id = useId();
  const groups = new Map<string, AccountSummary[]>();
  for (const account of accounts) {
    const group = groups.get(account.provider);
    if (group) group.push(account);
    else groups.set(account.provider, [account]);
  }

  return (
    <section className="account-directory" aria-label="Provider accounts">
      <header className="account-directory-heading">
        <h2>Your accounts</h2>
        <span>
          {accounts.length === total_count
            ? `${total_count} ${total_count === 1 ? "account" : "accounts"}`
            : `${accounts.length} of ${total_count} accounts`}
        </span>
      </header>
      <div className="account-directory-columns" aria-hidden="true">
        <span>Account</span>
        <span>Routing</span>
        <span>Credentials</span>
        <span />
      </div>
      {[...groups].map(([provider, group], index) => (
        <section
          className="account-directory-group"
          aria-labelledby={`${list_id}-provider-${index}`}
          key={provider}
        >
          <h3 id={`${list_id}-provider-${index}`}>
            <span>{provider || "Unknown provider"}</span>
            <span>{group.length}</span>
          </h3>
          <ul className="account-directory-rows">
            {group.map((account) => {
              const name = account.label || account.username || account.id;
              const identity =
                account.username &&
                account.username !== name &&
                account.username !== account.id
                  ? `${account.username} · ${account.id}`
                  : name === account.id
                    ? "Account ID"
                    : account.id;
              const activation = account.enabled ? account.tier : "disabled";
              const credential_kind = credentialLabel(account.credential_kind);
              const credential_status = credentialStatus(
                account.credential_status,
              );
              const has_draft = draft_ids.has(account.id);
              return (
                <li key={account.id}>
                  <button
                    type="button"
                    className="account-directory-row"
                    disabled={working}
                    aria-haspopup="dialog"
                    aria-label={`Open ${name}. Account ID ${account.id}. ${activation} routing. ${credential_kind}, ${credential_status.toLowerCase()}.${has_draft ? " Unsaved edits." : ""}`}
                    onClick={() => onOpen(account.id)}
                  >
                    <span className="account-directory-identity">
                      <span className="account-directory-name-line">
                        <strong title={name}>{name}</strong>
                        {has_draft && (
                          <span className="account-directory-draft">
                            Unsaved
                          </span>
                        )}
                      </span>
                      <span
                        className="account-directory-account-id"
                        title={
                          identity === "Account ID" ? account.id : identity
                        }
                      >
                        {identity}
                      </span>
                    </span>
                    <span className="account-directory-routing">
                      <span
                        className={`account-directory-activation account-directory-activation-${activation}`}
                      >
                        <span aria-hidden="true" />
                        {activation}
                      </span>
                    </span>
                    <span className="account-directory-credentials">
                      <span className="account-directory-credential-kind">
                        {credential_kind}
                      </span>
                      <span
                        className={`account-directory-credential-status ${account.credential_status === "stored" ? "" : "account-directory-credential-warning"}`}
                      >
                        {credential_status}
                      </span>
                    </span>
                    <ChevronRight
                      className="account-directory-chevron"
                      size={16}
                      aria-hidden="true"
                    />
                  </button>
                </li>
              );
            })}
          </ul>
        </section>
      ))}
    </section>
  );
}
