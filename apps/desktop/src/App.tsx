import { AppShell } from "./components/AppShell";
import { GatewayProvider } from "./lib/gateway";

export default function App() {
  return (
    <GatewayProvider>
      <AppShell />
    </GatewayProvider>
  );
}
