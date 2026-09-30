import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../..", import.meta.url));
const failures: string[] = [];

function readText(path: string): string {
  return readFileSync(join(root, path), "utf8");
}

function object(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function array(value: unknown, label: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be an array`);
  }
  return value;
}

function string(value: unknown, label: string): string {
  if (typeof value !== "string") {
    throw new Error(`${label} must be a string`);
  }
  return value;
}

function readJson(path: string): Record<string, unknown> {
  return object(JSON.parse(readText(path)), path);
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Only fixed metadata fields are read here; Cargo parses the Rust manifests.
function tomlString(text: string, key: string, label: string): string {
  const pattern = new RegExp(`^\\s*${escapeRegex(key)}\\s*=\\s*("(?:[^"\\\\]|\\\\.)*"|'[^']*')\\s*(?:#.*)?$`, "gm");
  const matches = [...text.matchAll(pattern)];
  if (matches.length !== 1) {
    throw new Error(`${label} must contain exactly one quoted ${key}`);
  }
  const value = matches[0]![1]!;
  return value.startsWith('"') ? string(JSON.parse(value), label) : value.slice(1, -1);
}

function tomlSection(path: string, section: string): string {
  const text = readText(path);
  const header = new RegExp(`^\\s*\\[${escapeRegex(section)}\\]\\s*(?:#.*)?$`, "gm");
  const matches = [...text.matchAll(header)];
  if (matches.length !== 1) {
    throw new Error(`${path} must contain exactly one [${section}] section`);
  }
  const start = matches[0]!.index! + matches[0]![0].length;
  const tail = text.slice(start);
  const next = tail.search(/^\s*\[/m);
  return next === -1 ? tail : tail.slice(0, next);
}

function expectVersion(label: string, actual: unknown, expected: string): void {
  if (actual !== expected) {
    failures.push(`${label}: expected ${expected}, got ${JSON.stringify(actual)}`);
  }
}

function checkWorkspaceDependencies(expected: string): void {
  const dependencies = tomlSection("Cargo.toml", "workspace.dependencies");
  for (const line of dependencies.split(/\r?\n/)) {
    if (!/\bpath\s*=/.test(line)) {
      continue;
    }
    const entry = /^\s*([\w-]+)\s*=\s*\{(.*)\}\s*(?:#.*)?$/.exec(line);
    if (entry === null) {
      throw new Error("Local workspace dependencies must use inline tables in Cargo.toml");
    }
    const field = /(?:^|,)\s*version\s*=\s*("(?:[^"\\]|\\.)*"|'[^']*')\s*(?=,|$)/.exec(entry[2]!);
    if (field === null) {
      failures.push(`Cargo.toml workspace dependency ${entry[1]}: missing version`);
      continue;
    }
    const quoted = field[1]!;
    const version = quoted.startsWith('"') ? JSON.parse(quoted) : quoted.slice(1, -1);
    expectVersion(`Cargo.toml workspace dependency ${entry[1]}`, version, expected);
  }
}

function cargoPackages(manifest_path: string): Record<string, unknown>[] {
  const result = spawnSync(
    "cargo",
    ["metadata", "--locked", "--offline", "--no-deps", "--format-version", "1", "--manifest-path", manifest_path],
    { cwd: root, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 },
  );
  if (result.error !== undefined || result.status !== 0) {
    throw new Error(`Cargo metadata failed for ${manifest_path}: ${result.error?.message ?? result.stderr.trim()}`);
  }
  const metadata = object(JSON.parse(result.stdout), `Cargo metadata (${manifest_path})`);
  return array(metadata["packages"], "Cargo packages").map((value) => object(value, "Cargo package"));
}

function checkLock(path: string, expected: string, required_names: Set<string>): void {
  const names = new Set<string>();
  for (const entry of readText(path).split(/^\s*\[\[package\]\]\s*(?:#.*)?$/m).slice(1)) {
    // Registry packages can coincidentally have the same version as tokn.
    if (/^\s*source\s*=/m.test(entry)) {
      continue;
    }
    const name = tomlString(entry, "name", path);
    if (name !== "tokn" && !name.startsWith("tokn-")) {
      continue;
    }
    if (names.has(name)) {
      failures.push(`${path}: duplicate local package ${name}`);
    }
    names.add(name);
    expectVersion(`${path} (${name})`, tomlString(entry, "version", path), expected);
  }
  for (const name of required_names) {
    if (!names.has(name)) {
      failures.push(`${path}: missing local package ${name}`);
    }
  }
}

function checkNpm(expected: string): number {
  const path = "bindings/typescript/package.json";
  const facade = readJson(path);
  const package_name = "@tokn-ai/requests";
  const facade_name = string(facade["name"], `${path} name`);
  if (facade_name !== package_name) {
    failures.push(`${path} name: expected ${package_name}, got ${facade_name}`);
  }
  const napi = object(facade["napi"], `${path} napi`);
  const napi_package_name = string(napi["packageName"], `${path} napi.packageName`);
  if (napi_package_name !== package_name) {
    failures.push(`${path} napi.packageName: expected ${package_name}, got ${napi_package_name}`);
  }
  expectVersion(`${path} version`, facade["version"], expected);
  const optional = object(facade["optionalDependencies"] ?? {}, `${path} optionalDependencies`);
  for (const [name, version] of Object.entries(optional)) {
    expectVersion(`${path} optional dependency ${name}`, version, expected);
  }

  const platform_names = new Set<string>();
  const platforms_path = "bindings/typescript/platforms";
  if (existsSync(join(root, platforms_path))) {
    for (const entry of readdirSync(join(root, platforms_path), { withFileTypes: true })) {
      if (!entry.isDirectory()) {
        continue;
      }
      const manifest_path = join(platforms_path, entry.name, "package.json");
      const manifest = readJson(manifest_path);
      const name = string(manifest["name"], `${manifest_path} name`);
      const expected_name = `${package_name}-${entry.name}`;
      if (name !== expected_name) {
        failures.push(`${manifest_path} name: expected ${expected_name}, got ${name}`);
      }
      if (platform_names.has(name)) {
        failures.push(`${manifest_path}: duplicate platform package ${name}`);
      }
      platform_names.add(name);
      expectVersion(`${manifest_path} version`, manifest["version"], expected);
      expectVersion(`${path} optional dependency ${name}`, optional[name], expected);
    }
  }
  for (const name of Object.keys(optional)) {
    if (!platform_names.has(name)) {
      failures.push(`${path}: optional dependency ${name} has no checked-in platform manifest`);
    }
  }
  return platform_names.size;
}

function main(): void {
  const tag = readText("VERSION").trim();
  const match = /^v((?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?)$/.exec(tag);
  const prerelease = tag.includes("-") ? tag.slice(tag.indexOf("-") + 1) : "";
  if (match === null || prerelease.split(".").some((part) => /^0\d+$/.test(part))) {
    throw new Error("VERSION must be a v-prefixed semantic version without build metadata");
  }
  const version = match[1]!;
  const notes_path = `docs/releases/${tag}.md`;
  if (!existsSync(join(root, notes_path)) || readText(notes_path).trim() === "") {
    failures.push(`Missing release notes: ${notes_path}`);
  }
  expectVersion(
    "Cargo.toml workspace package",
    tomlString(tomlSection("Cargo.toml", "workspace.package"), "version", "Cargo.toml"),
    version,
  );
  // Cargo metadata omits declarations that no crate currently consumes.
  checkWorkspaceDependencies(version);

  const packages = cargoPackages("Cargo.toml");
  const local_names = new Set(packages.map((value) => string(value["name"], "Cargo package name")));
  for (const value of packages) {
    const name = string(value["name"], "Cargo package name");
    const manifest_path = relative(root, string(value["manifest_path"], "Cargo manifest path"));
    expectVersion(`${manifest_path} (${name})`, value["version"], version);
    for (const item of array(value["dependencies"], `${name} dependencies`)) {
      const dependency = object(item, "Cargo dependency");
      if (dependency["path"] !== undefined && local_names.has(string(dependency["name"], "Cargo dependency name"))) {
        const requirement = dependency["req"];
        if (requirement !== `^${version}` && requirement !== `=${version}`) {
          failures.push(
            `${manifest_path} dependency ${dependency["name"]}: expected ^${version} or =${version}, got ${requirement}`,
          );
        }
      }
    }
  }
  checkLock("Cargo.lock", version, local_names);

  const desktop = cargoPackages("apps/desktop/src-tauri/Cargo.toml");
  const desktop_names = new Set(desktop.map((value) => string(value["name"], "Desktop Cargo package name")));
  for (const value of desktop) {
    expectVersion(`apps/desktop/src-tauri/Cargo.toml (${value["name"]})`, value["version"], version);
  }
  checkLock("apps/desktop/src-tauri/Cargo.lock", version, desktop_names);
  for (const path of ["apps/desktop/package.json", "apps/desktop/src-tauri/tauri.conf.json"]) {
    expectVersion(`${path} version`, readJson(path)["version"], version);
  }
  const python_path = "bindings/python/pyproject.toml";
  expectVersion(
    `${python_path} project`,
    tomlString(tomlSection(python_path, "project"), "version", python_path),
    version,
  );
  const platform_count = checkNpm(version);

  if (failures.length > 0) {
    throw new Error(`Release metadata is inconsistent:\n${failures.map((failure) => `- ${failure}`).join("\n")}`);
  }
  console.log(
    `Release metadata matches ${tag}: ${packages.length} Rust packages, desktop, Python, npm, ` +
      `and ${platform_count} native npm packages.`,
  );
}

try {
  main();
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
}
