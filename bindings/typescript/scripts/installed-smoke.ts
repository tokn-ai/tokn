import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

// Resolve from this installed fixture, never from the source checkout.
const packageName: string = "@tokn/requests";
const { Client, request }: typeof import("../src/index.js") = await import(packageName);
const require = createRequire(import.meta.url);
const manifestPath = require.resolve("@tokn/requests/package.json");
const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as Record<string, unknown>;
assert.equal(manifest["version"], process.env["TOKN_PACKAGE_VERSION"]);
assert(!(await readdir(dirname(manifestPath))).some((path) => path.endsWith(".node")));
const nativePackage = process.env["TOKN_NATIVE_PACKAGE"];
assert(nativePackage !== undefined);
const binding = require(nativePackage) as { nativeAbiVersion(): number };
assert.equal(binding.nativeAbiVersion(), 1);
const nativeManifest = require(`${nativePackage}/package.json`) as Record<string, unknown>;
assert.equal(nativeManifest["version"], manifest["version"]);
for (const name of Object.keys(manifest["optionalDependencies"] as Record<string, unknown>)) {
  if (name !== nativePackage) {
    assert.throws(() => require.resolve(name), { code: "MODULE_NOT_FOUND" });
  }
}
assert.deepEqual(request("smart").prompt("Hello").maxTokens(32).build(), {
  model: "smart",
  messages: [{ role: "user", content: "Hello" }],
  max_output_tokens: 32,
});

let requestCount = 0;
const server = createServer(async (incoming, outgoing) => {
  const chunks: Buffer[] = [];
  for await (const chunk of incoming) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
  assert.equal(incoming.url, "/chat/completions");
  assert.equal(body["model"], "mock-model");
  requestCount += 1;
  outgoing.writeHead(200, { "content-type": "application/json" });
  outgoing.end(JSON.stringify({
    id: "installed-requests-smoke",
    object: "chat.completion",
    model: "mock-model",
    choices: [{ index: 0, message: { role: "assistant", content: "installed native answer" }, finish_reason: "stop" }],
    usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3 },
  }));
});
server.listen(0, "127.0.0.1");
await once(server, "listening");
const address = server.address();
assert(address !== null && typeof address === "object");
const fixture = await mkdtemp(join(tmpdir(), "tokn-installed-client-"));
let client: Awaited<ReturnType<typeof Client.create>> | undefined;
try {
  const configPath = join(fixture, "config.toml");
  const authPath = join(fixture, "auth.yaml");
  await writeFile(configPath, '[defaults]\nmode = "exact"\n');
  await writeFile(authPath, `version: 1\naccounts:\n  - id: local-llama\n    provider: llama-cpp\n    base_url: http://127.0.0.1:${address.port}\n`);
  client = await Client.create({ config_path: configPath, auth_path: authPath });
  const response = await client.generate("llama-cpp/mock-model").prompt("Hello").send();
  assert.equal(response.text, "installed native answer");
  assert.equal(response.usage?.total_tokens, 3);
  assert.equal(requestCount, 1);
  console.log(`Installed @tokn/requests ${manifest["version"]}: native ABI and provider request passed`);
} finally {
  await client?.close();
  server.closeAllConnections();
  await new Promise<void>((resolve) => server.close(() => resolve()));
  await rm(fixture, { recursive: true, force: true });
}
