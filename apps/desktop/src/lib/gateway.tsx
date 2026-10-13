import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import { listen } from "@tauri-apps/api/event";
import { api } from "./tauri";
import { useResource } from "./useResource";
import type { GatewayStatus } from "./types";

interface GatewayResource {
  data: GatewayStatus | undefined;
  loading: boolean;
  error: string;
  refresh: () => Promise<void>;
}

const GatewayContext = createContext<GatewayResource | null>(null);

export function GatewayProvider({ children }: { children: ReactNode }) {
  const resource = useResource(api.status);
  const [event_error, setEventError] = useState("");
  const [listener_attempt, setListenerAttempt] = useState(0);
  const refresh = useCallback(async () => {
    if (event_error) {
      setEventError("");
      setListenerAttempt((attempt) => attempt + 1);
    }
    await resource.refresh();
  }, [event_error, resource.refresh]);

  useEffect(() => {
    let disposed = false;
    const unlisten = listen<GatewayStatus>("gateway-status", (event) => {
      if (!disposed) resource.setData(event.payload);
    });
    void unlisten.catch((error) => {
      if (!disposed) setEventError(String(error));
    });
    return () => {
      disposed = true;
      void unlisten.then((stop) => stop()).catch(() => {});
    };
  }, [resource.setData, listener_attempt]);

  return (
    <GatewayContext.Provider
      value={{
        data: resource.data,
        loading: resource.loading,
        error: resource.error || event_error,
        refresh,
      }}
    >
      {children}
    </GatewayContext.Provider>
  );
}

export function useGateway() {
  const resource = useContext(GatewayContext);
  if (!resource) throw new Error("useGateway requires GatewayProvider");
  return resource;
}
