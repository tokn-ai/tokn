import { useEffect, useRef, useState } from "react";
import { ArrowRight, ChevronRight, FileCode2, Search } from "lucide-react";
import { Modal } from "../Modal";
import type {
  RoutingOverview,
  RoutingProfileSummary,
  RoutingRouteSummary,
  RoutingSource,
  RoutingSummaryField,
} from "../../lib/types";
import "./RoutingBrowser.css";

interface RoutingBrowserProps {
  overview: RoutingOverview;
  schema: string;
  config_path: string;
  disabled: boolean;
  onEdit: () => void;
}

type Selection = { kind: "profile" | "route"; key: string };
type BrowserTab = "profiles" | "routes";

function resourceKey(resource: { id: string; source: RoutingSource }) {
  return JSON.stringify([
    resource.source.path,
    resource.source.section,
    resource.id,
  ]);
}

function displayKind(kind: string) {
  return kind.replaceAll("_", " ");
}

function sectionLabel(section: string) {
  return section.startsWith("[") ? section : `[${section}]`;
}

function sourceLabel(source: RoutingSource, config_path: string) {
  if (source.path !== config_path) return "Read-only fragment";
  if (source.generated) return `Generated from ${sectionLabel(source.section)}`;
  return sectionLabel(source.section);
}

function matchesSearch(
  values: (string | null)[],
  fields: RoutingSummaryField[],
  query: string,
) {
  return [...values, ...fields.flatMap((field) => [field.label, field.value])]
    .join(" ")
    .toLowerCase()
    .includes(query);
}

function SummaryFields({ fields }: { fields: RoutingSummaryField[] }) {
  return (
    <dl className="routing-browser-facts">
      {fields.map((field, index) => (
        <div key={`${field.label}-${index}`}>
          <dt>{field.label}</dt>
          <dd>{field.value || "Not specified"}</dd>
        </div>
      ))}
    </dl>
  );
}

export function RoutingBrowser({
  overview,
  schema,
  config_path,
  disabled,
  onEdit,
}: RoutingBrowserProps) {
  const directory = useRef<HTMLDivElement>(null);
  const [active_tab, setActiveTab] = useState<BrowserTab>("profiles");
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState<Selection | null>(null);
  const is_v2 = schema === "v2";
  const tab = is_v2 ? active_tab : "profiles";
  const search = query.trim().toLowerCase();
  const profiles = overview.profiles.filter((profile) =>
    matchesSearch(
      [
        profile.id,
        profile.route_id,
        profile.route_kind,
        profile.api_path,
        profile.source.section,
        profile.source.path,
      ],
      profile.fields,
      search,
    ),
  );
  const routes = overview.routes.filter((route) =>
    matchesSearch(
      [
        route.id,
        route.kind,
        route.target,
        route.source.section,
        route.source.path,
        ...route.used_by,
      ],
      route.fields,
      search,
    ),
  );
  const profile =
    selected?.kind === "profile"
      ? overview.profiles.find((item) => resourceKey(item) === selected.key)
      : undefined;
  const route =
    selected?.kind === "route" && is_v2
      ? overview.routes.find((item) => resourceKey(item) === selected.key)
      : undefined;
  const linked_route = profile?.route_id
    ? overview.routes.find((item) => item.id === profile.route_id)
    : undefined;
  const detail = profile ?? route;
  const source = detail?.source;
  const can_edit = source?.path === config_path;
  const visible_count = tab === "profiles" ? profiles.length : routes.length;
  const total_count =
    tab === "profiles" ? overview.profiles.length : overview.routes.length;

  useEffect(() => {
    if (selected && !detail) setSelected(null);
  }, [selected, detail]);

  function edit() {
    setSelected(null);
    onEdit();
  }

  return (
    <div className="routing-browser" ref={directory} tabIndex={-1}>
      <div className="routing-browser-toolbar">
        {is_v2 ? (
          <div
            className="routing-browser-tabs"
            role="group"
            aria-label="Routing resource type"
          >
            {(["profiles", "routes"] as const).map((item) => (
              <button
                type="button"
                key={item}
                aria-pressed={tab === item}
                disabled={disabled}
                onClick={() => setActiveTab(item)}
              >
                {item === "profiles" ? "Profiles" : "Routes"}
                <span>
                  {item === "profiles"
                    ? overview.profiles.length
                    : overview.routes.length}
                </span>
              </button>
            ))}
          </div>
        ) : (
          <h2 className="routing-browser-legacy-heading">Profiles</h2>
        )}
        <label className="routing-browser-search">
          <Search size={15} aria-hidden="true" />
          <input
            type="search"
            aria-label={`Search ${tab}`}
            placeholder={`Search ${tab}…`}
            value={query}
            disabled={disabled}
            onChange={(event) => setQuery(event.target.value)}
          />
        </label>
      </div>
      <div className="routing-browser-description">
        <p>
          {tab === "profiles"
            ? is_v2
              ? "Profiles select a route and define how clients use it."
              : "Profiles choose the routing policy and accounts for each workflow."
            : "Routes define request handling and provider selection."}
        </p>
        <span>Saved configuration</span>
      </div>
      <section
        className="routing-browser-directory"
        aria-label={`Saved ${tab}`}
      >
        <div
          className={`routing-browser-columns routing-browser-columns-${tab}`}
          aria-hidden="true"
        >
          <span>{tab === "profiles" ? "Profile" : "Route"}</span>
          <span>
            {tab === "profiles" ? (is_v2 ? "Route" : "Mode") : "Target"}
          </span>
          <span>{tab === "profiles" ? "API path" : "Used by"}</span>
          <span />
        </div>
        {visible_count === 0 ? (
          <div className="routing-browser-empty">
            <Search size={22} strokeWidth={1.5} aria-hidden="true" />
            <h3>{search ? `No ${tab} match` : `No ${tab} configured`}</h3>
            <p>
              {search
                ? "Try a name, endpoint, provider, or route."
                : tab === "routes"
                  ? "Add a route in the Advanced editor, then reference it from a profile."
                  : "Add a profile in the Advanced editor to define a routing policy."}
            </p>
            <button
              type="button"
              disabled={disabled}
              onClick={search ? () => setQuery("") : edit}
            >
              {search ? "Clear search" : "Open Advanced editor"}
            </button>
          </div>
        ) : (
          <ul className="routing-browser-rows">
            {tab === "profiles"
              ? profiles.map((item) => (
                  <ProfileRow
                    key={resourceKey(item)}
                    profile={item}
                    is_v2={is_v2}
                    config_path={config_path}
                    disabled={disabled}
                    onOpen={() =>
                      setSelected({ kind: "profile", key: resourceKey(item) })
                    }
                  />
                ))
              : routes.map((item) => (
                  <RouteRow
                    key={resourceKey(item)}
                    route={item}
                    config_path={config_path}
                    disabled={disabled}
                    onOpen={() =>
                      setSelected({ kind: "route", key: resourceKey(item) })
                    }
                  />
                ))}
          </ul>
        )}
        <footer className="routing-browser-directory-footer">
          <span>
            {visible_count === total_count
              ? total_count
              : `${visible_count} of ${total_count}`}{" "}
            {total_count === 1 ? tab.slice(0, -1) : tab}
          </span>
          <span>
            {overview.model_score_count} model score{" "}
            {overview.model_score_count === 1 ? "rule" : "rules"}
          </span>
        </footer>
      </section>
      {detail && source && (
        <Modal
          key={`${selected?.kind}:${selected?.key}`}
          title={detail.id}
          description={`${profile ? "Profile" : "Route"} · ${sectionLabel(source.section)} · Saved configuration`}
          class_name="routing-browser-panel"
          return_focus={directory}
          busy={disabled}
          onClose={() => setSelected(null)}
        >
          <div
            className="routing-browser-panel-body"
            role="region"
            aria-label="Routing details"
            tabIndex={0}
          >
            {profile && (
              <div className="routing-browser-profile-summary">
                <dl className="routing-browser-facts">
                  <div>
                    <dt>{is_v2 ? "Route" : "Routing mode"}</dt>
                    <dd>
                      {linked_route ? (
                        <button
                          type="button"
                          className="routing-browser-reference"
                          disabled={disabled}
                          onClick={() =>
                            setSelected({
                              kind: "route",
                              key: resourceKey(linked_route),
                            })
                          }
                        >
                          {profile.route_id}
                          <ArrowRight size={13} aria-hidden="true" />
                        </button>
                      ) : (
                        profile.route_id || displayKind(profile.route_kind)
                      )}
                    </dd>
                  </div>
                  <div>
                    <dt>API path</dt>
                    <dd>
                      <code>{profile.api_path ?? "No API mount"}</code>
                    </dd>
                  </div>
                </dl>
              </div>
            )}
            {route && (
              <SummaryFields
                fields={[
                  { label: "Route type", value: displayKind(route.kind) },
                  { label: "Target", value: route.target },
                ]}
              />
            )}
            <SummaryFields fields={detail.fields} />
            {route && (
              <section
                className="routing-browser-references"
                aria-label="Profiles using this route"
              >
                <h3>Used by profiles</h3>
                {route.used_by.length ? (
                  <div>
                    {route.used_by.map((id) => {
                      const referenced_profile = overview.profiles.find(
                        (item) => item.id === id,
                      );
                      return referenced_profile ? (
                        <button
                          type="button"
                          key={resourceKey(referenced_profile)}
                          className="routing-browser-reference"
                          disabled={disabled}
                          onClick={() =>
                            setSelected({
                              kind: "profile",
                              key: resourceKey(referenced_profile),
                            })
                          }
                        >
                          {id}
                          <ArrowRight size={13} aria-hidden="true" />
                        </button>
                      ) : (
                        <span key={id}>{id}</span>
                      );
                    })}
                  </div>
                ) : (
                  <p>No saved profiles reference this route.</p>
                )}
              </section>
            )}
            <section
              className="routing-browser-source"
              aria-label="Configuration source"
            >
              <h3>
                <FileCode2 size={15} aria-hidden="true" /> Configuration source
              </h3>
              <code>{sectionLabel(source.section)}</code>
              <p className="routing-browser-source-path">{source.path}</p>
              {source.generated && is_v2 && (
                <p>
                  Generated from the default policy. This defines the default
                  profile and route; other named profiles have their own policy.
                </p>
              )}
              {!can_edit && (
                <p>
                  This entry comes from a configuration fragment. Edit that file
                  to change it.
                </p>
              )}
            </section>
          </div>
          <footer className="routing-browser-panel-footer">
            <span>
              {can_edit
                ? "Changes are made in the Advanced editor."
                : "This source is read only in the app."}
            </span>
            {can_edit && (
              <button type="button" disabled={disabled} onClick={edit}>
                <FileCode2 size={15} aria-hidden="true" /> Edit in Advanced
              </button>
            )}
          </footer>
        </Modal>
      )}
    </div>
  );
}

function ProfileRow({
  profile,
  is_v2,
  config_path,
  disabled,
  onOpen,
}: {
  profile: RoutingProfileSummary;
  is_v2: boolean;
  config_path: string;
  disabled: boolean;
  onOpen: () => void;
}) {
  return (
    <li>
      <button
        type="button"
        className="routing-browser-row"
        disabled={disabled}
        aria-haspopup="dialog"
        aria-label={`Open profile ${profile.id}, ${sectionLabel(profile.source.section)}. ${profile.route_id ? `Route ${profile.route_id}. ` : ""}${profile.api_path ?? "No API mount"}.`}
        onClick={onOpen}
      >
        <span className="routing-browser-identity">
          <strong>{profile.id}</strong>
          <span>{sourceLabel(profile.source, config_path)}</span>
        </span>
        <span className="routing-browser-cell">
          <span>{profile.route_id || displayKind(profile.route_kind)}</span>
          {is_v2 && profile.route_id && (
            <small>{displayKind(profile.route_kind)}</small>
          )}
        </span>
        <span className="routing-browser-cell routing-browser-path">
          <code>{profile.api_path ?? "No API mount"}</code>
        </span>
        <ChevronRight size={16} aria-hidden="true" />
      </button>
    </li>
  );
}

function RouteRow({
  route,
  config_path,
  disabled,
  onOpen,
}: {
  route: RoutingRouteSummary;
  config_path: string;
  disabled: boolean;
  onOpen: () => void;
}) {
  return (
    <li>
      <button
        type="button"
        className="routing-browser-row"
        disabled={disabled}
        aria-haspopup="dialog"
        aria-label={`Open route ${route.id}. ${displayKind(route.kind)}. ${route.target}. Used by ${route.used_by.length} ${route.used_by.length === 1 ? "profile" : "profiles"}.`}
        onClick={onOpen}
      >
        <span className="routing-browser-identity">
          <strong>{route.id}</strong>
          <span>{sourceLabel(route.source, config_path)}</span>
        </span>
        <span className="routing-browser-cell">
          <span>{route.target}</span>
          <small>{displayKind(route.kind)}</small>
        </span>
        <span className="routing-browser-cell routing-browser-reference-count">
          {route.used_by.length}{" "}
          {route.used_by.length === 1 ? "profile" : "profiles"}
        </span>
        <ChevronRight size={16} aria-hidden="true" />
      </button>
    </li>
  );
}
