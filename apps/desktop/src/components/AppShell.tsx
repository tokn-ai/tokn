import { useCallback, useEffect, useState } from "react";
import {
  Activity,
  ArrowUpRight,
  ChevronRight,
  GitBranch,
  LayoutDashboard,
  Radio,
  Server,
  ShieldCheck,
} from "lucide-react";
import { Overview } from "../pages/Overview";
import { Accounts } from "../pages/Accounts";
import { Routing } from "../pages/Routing";
import { Inspect } from "../pages/Inspect";
import { useGateway } from "../lib/gateway";
import { ThemePicker } from "./ThemePicker";

type PageId = "overview" | "inspect" | "accounts" | "routing";

const pages = [
  { id: "overview", title: "Overview", icon: LayoutDashboard },
  { id: "inspect", title: "Activity", icon: Activity },
  { id: "accounts", title: "Accounts", icon: ShieldCheck },
  { id: "routing", title: "Routing", icon: GitBranch },
] as const;

function initialPage(): PageId {
  const params = new URLSearchParams(window.location.search);
  return [
    "view",
    "day",
    "request_id",
    "row_id",
    "session_id",
    "node_id",
    "query",
    "provider_id",
    "url_path",
    "status",
    "errors_only",
    "tab",
    "timezone",
  ].some((key) => params.has(key))
    ? "inspect"
    : "overview";
}

export function AppShell() {
  const [active, setActive] = useState<PageId>(initialPage);
  const [visited, setVisited] = useState<Set<PageId>>(
    () => new Set([initialPage()]),
  );
  const gateway = useGateway();
  const navigate = useCallback((page: PageId) => {
    setVisited((previous) => new Set([...previous, page]));
    setActive(page);
  }, []);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (
        !(event.metaKey || event.ctrlKey) ||
        event.altKey ||
        event.shiftKey ||
        event.defaultPrevented ||
        document.querySelector("dialog[open]")
      )
        return;
      if (
        event
          .composedPath()
          .some(
            (target) =>
              target instanceof HTMLElement &&
              (target.matches("input, textarea, select") ||
                target.isContentEditable),
          )
      )
        return;
      const page = pages[Number(event.key) - 1];
      if (page) {
        event.preventDefault();
        navigate(page.id);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [navigate]);

  const state = gateway.error
    ? "unavailable"
    : (gateway.data?.state ?? "checking");
  const state_label =
    (
      {
        running: "Gateway online",
        starting: "Starting gateway",
        stopped: "Gateway stopped",
        unavailable: "Gateway unavailable",
        checking: "Checking gateway",
      } as Record<string, string>
    )[state] ?? state;
  const title = pages.find((page) => page.id === active)!.title;

  return (
    <div className="app-shell">
      <a className="skip-link" href="#page-content">
        Skip to content
      </a>
      <aside className="app-sidebar">
        <button
          className="app-brand"
          onClick={() => navigate("overview")}
          aria-label="Tokn overview"
        >
          <span className="brand-symbol" aria-hidden="true">
            <span />
            <span />
            <span />
          </span>
          <span className="brand-wordmark">
            tokn<span>Local gateway</span>
          </span>
        </button>
        <div className="sidebar-section-label">WORKSPACE</div>
        <nav className="main-navigation" aria-label="Main navigation">
          {pages.map((page, index) => (
            <button
              key={page.id}
              className={active === page.id ? "active" : ""}
              onClick={() => navigate(page.id)}
              aria-current={active === page.id ? "page" : undefined}
              aria-label={page.title}
              title={`${page.title} · ⌘ / Ctrl ${index + 1}`}
            >
              <page.icon size={17} strokeWidth={1.7} />
              <span>{page.title}</span>
              <span className="nav-shortcut" aria-hidden="true">
                {index + 1}
              </span>
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <button
            className="sidebar-instance"
            onClick={() => navigate("overview")}
            aria-label={`${state_label}. Open overview`}
          >
            <span className={`status-dot ${state}`} />
            <span>
              <strong>{state_label}</strong>
              <small>{gateway.data?.address || "On this device"}</small>
            </span>
            <ArrowUpRight size={14} />
          </button>
          <ThemePicker />
          <div className="sidebar-footnote">
            <Server size={12} /> Your machine. Your models.
          </div>
        </div>
      </aside>
      <div className="app-workspace">
        <header className="workspace-bar">
          <div className="workspace-breadcrumb">
            <span>Workspace</span>
            <ChevronRight size={13} />
            <strong>{title}</strong>
          </div>
          <span className="workspace-local">
            <Radio size={13} /> Local
          </span>
        </header>
        <main
          id="page-content"
          className={`page-content ${active === "inspect" ? "page-content-inspect" : ""}`}
          tabIndex={-1}
        >
          {pages.map(
            (page) =>
              visited.has(page.id) && (
                <div
                  key={page.id}
                  className="page-view"
                  hidden={active !== page.id}
                >
                  {page.id === "overview" && <Overview onNavigate={navigate} />}
                  {page.id === "accounts" && <Accounts />}
                  {page.id === "routing" && <Routing />}
                  {page.id === "inspect" && <Inspect />}
                </div>
              ),
          )}
        </main>
      </div>
    </div>
  );
}
