import { useEffect, useRef, useState } from "react";
import { listen } from "@tauri-apps/api/event";
import { Check, Copy, KeyRound, RefreshCw } from "lucide-react";
import { api } from "../../lib/tauri";
import { useResource } from "../../lib/useResource";
import type {
  AccountImport,
  LoginProgress,
  LoginTicket,
} from "../../lib/types";
import { ErrorMessage } from "../Feedback";
import { Modal } from "../Modal";

export function AddAccount({
  onSaved,
  onClose,
}: {
  onSaved: () => Promise<void>;
  onClose: () => void;
}) {
  const providers = useResource(api.accountProviders);
  const [provider_id, setProvider] = useState("");
  const provider =
    providers.data?.find((item) => item.id === provider_id) ??
    providers.data?.[0];
  const [id, setId] = useState("");
  const [source, setSource] = useState("string");
  const [flavor, setFlavor] = useState<AccountImport["flavor"]>("api_key");
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [ticket, setTicket] = useState<LoginTicket>();
  const [phase, setPhase] = useState("");
  const [copied, setCopied] = useState("");
  const login_ref = useRef<string | undefined>(undefined);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      if (login_ref.current) void api.cancelAccountLogin(login_ref.current);
    };
  }, []);
  useEffect(() => {
    setSource(
      provider?.sources.includes("string")
        ? "string"
        : (provider?.sources[0] ?? ""),
    );
    setFlavor(provider?.default_flavor ?? "api_key");
    setValue("");
  }, [provider?.id]);

  async function save() {
    if (!provider) return;
    setBusy(true);
    setError("");
    try {
      const request = {
        id: id.trim(),
        provider: provider.id,
        source,
        flavor,
        value,
      };
      setValue("");
      await api.importAccount(request);
      await onSaved();
      onClose();
    } catch (error) {
      setError(String(error));
    } finally {
      if (mounted.current) setBusy(false);
    }
  }
  async function login() {
    if (!provider) return;
    setBusy(true);
    setError("");
    setCopied("");
    setPhase("Requesting a device code…");
    let unlisten: (() => void) | undefined;
    try {
      unlisten = await listen<LoginProgress>(
        "account-login-progress",
        ({ payload }) => {
          if (payload.login_id === login_ref.current)
            setPhase(
              payload.phase === "saving"
                ? "Saving account…"
                : "Waiting for browser sign-in…",
            );
        },
      );
      const next = await api.beginAccountLogin(id.trim(), provider.id);
      login_ref.current = next.login_id;
      if (!mounted.current) {
        await api.cancelAccountLogin(next.login_id);
        return;
      }
      setTicket(next);
      setPhase("Waiting for browser sign-in…");
      await api.completeAccountLogin(next.login_id);
      await onSaved();
      onClose();
    } catch (error) {
      if (mounted.current) setError(String(error));
    } finally {
      unlisten?.();
      login_ref.current = undefined;
      if (mounted.current) {
        setBusy(false);
        setTicket(undefined);
        setPhase("");
      }
    }
  }
  async function copy(value: string, kind: string) {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(kind);
    } catch {
      setError("Unable to copy. Select and copy the address or code manually.");
    }
  }
  return (
    <Modal title="Add account" busy={busy} onClose={onClose}>
      <p className="account-add-intro">
        Connect a provider using sign-in or an existing credential.
      </p>
      <ErrorMessage error={providers.error || error} />
      {providers.loading ? (
        <p>Loading providers…</p>
      ) : providers.error && !provider ? (
        <div className="account-provider-error">
          <p>Provider options could not be loaded.</p>
          <button onClick={() => void providers.refresh()}>
            <RefreshCw size={15} />
            Try again
          </button>
        </div>
      ) : !provider ? (
        <p>No credential providers are enabled in your configuration.</p>
      ) : (
        <>
          {!phase && (
            <fieldset disabled={busy} className="account-form">
              <label>
                Provider
                <select
                  value={provider.id}
                  onChange={(event) => setProvider(event.target.value)}
                >
                  {providers.data?.map((item) => (
                    <option key={item.id}>{item.id}</option>
                  ))}
                </select>
              </label>
              <label>
                Account ID (optional)
                <input
                  value={id}
                  maxLength={128}
                  onChange={(event) => setId(event.target.value)}
                  placeholder="Use provider identity automatically"
                  autoComplete="off"
                />
              </label>
              <p className="muted">
                Leave the ID empty to use your provider identity, or a generated
                unique name. Existing accounts will not be replaced.
              </p>
              {provider.device_login && (
                <div className="account-login-method">
                  <div>
                    <h3>Sign in with your provider</h3>
                    <p>Use a device code to connect your existing account.</p>
                  </div>
                  <button onClick={() => void login()}>
                    <KeyRound size={15} />
                    Sign in with device code
                  </button>
                </div>
              )}
              <div className="account-import-heading">Import a credential</div>
              <label>
                Import from
                <select
                  value={source}
                  onChange={(event) => {
                    setSource(event.target.value);
                    setValue("");
                  }}
                >
                  {provider.sources.map((item) => (
                    <option value={item} key={item}>
                      {(
                        {
                          string: "Paste credential",
                          env: "Environment variable",
                          file: "Credential file",
                        } as Record<string, string>
                      )[item] ?? item}
                    </option>
                  ))}
                </select>
              </label>
              {["string", "env", "file"].includes(source) && (
                <label>
                  Credential type
                  <select
                    value={flavor}
                    onChange={(event) =>
                      setFlavor(event.target.value as AccountImport["flavor"])
                    }
                  >
                    {provider.api_key && (
                      <option value="api_key">API key</option>
                    )}
                    {provider.refresh_token && (
                      <option value="refresh_token">Refresh token</option>
                    )}
                  </select>
                </label>
              )}
              <label>
                {source === "string"
                  ? "Credential"
                  : source === "env"
                    ? "Environment variable name"
                    : source === "file"
                      ? "File path"
                      : "Source value (optional)"}
                <input
                  type={source === "string" ? "password" : "text"}
                  value={value}
                  onChange={(event) => setValue(event.target.value)}
                  autoComplete="off"
                  spellCheck={false}
                />
              </label>
              {source === "env" && (
                <p className="muted">
                  The variable must be available to the desktop process.
                </p>
              )}
              <button
                className="primary"
                disabled={
                  !source ||
                  (["string", "env", "file"].includes(source) && !value.trim())
                }
                onClick={() => void save()}
              >
                Verify and add account
              </button>
            </fieldset>
          )}
          {busy && (
            <p className="account-login-progress" role="status">
              {phase || "Importing and verifying credential…"}
            </p>
          )}
          {ticket && (
            <div className="account-device-ticket">
              <p>Open this address in your browser and enter the code:</p>
              <div className="account-device-address">
                <span className="account-id">{ticket.verification_uri}</span>
                <button
                  aria-label="Copy sign-in address"
                  onClick={() => void copy(ticket.verification_uri, "address")}
                >
                  {copied === "address" ? (
                    <Check size={15} />
                  ) : (
                    <Copy size={15} />
                  )}
                </button>
              </div>
              <div className="account-device-code">
                <strong>{ticket.user_code}</strong>
                <button onClick={() => void copy(ticket.user_code, "code")}>
                  {copied === "code" ? <Check size={15} /> : <Copy size={15} />}
                  {copied === "code" ? "Copied" : "Copy code"}
                </button>
              </div>
              <p>
                Code expires after {Math.ceil(ticket.expires_in / 60)} minutes.
              </p>
              <button
                disabled={phase === "Saving account…"}
                onClick={() =>
                  void api
                    .cancelAccountLogin(ticket.login_id)
                    .catch((error) => setError(String(error)))
                }
              >
                Cancel login
              </button>
            </div>
          )}
        </>
      )}
    </Modal>
  );
}
