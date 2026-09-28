import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { cp, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

type Manifest = Record<string, unknown>;

interface PackedPackage {
  readonly package_name: string;
  readonly filename: string;
  readonly integrity: string;
  readonly manifest: Manifest;
}

interface PackResult {
  readonly filename: string;
  readonly files: ReadonlyArray<{ readonly path: string }>;
}

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repositoryRoot = resolve(packageRoot, "../..");
const releaseRoot = join(packageRoot, "release");
const platforms = [
  { suffix: "darwin-arm64", target: "aarch64-apple-darwin", os: "darwin", cpu: "arm64" },
  { suffix: "linux-x64-gnu", target: "x86_64-unknown-linux-gnu", os: "linux", cpu: "x64" },
  { suffix: "win32-x64-msvc", target: "x86_64-pc-windows-msvc", os: "win32", cpu: "x64" },
] as const;

async function readManifest(path: string): Promise<Manifest> {
  const value: unknown = JSON.parse(await readFile(path, "utf8"));
  assert(value !== null && typeof value === "object" && !Array.isArray(value), `Invalid manifest: ${path}`);
  return value as Manifest;
}

function npmCommand(): string {
  return process.platform === "win32" ? "npm.cmd" : "npm";
}

async function run(command: string, args: string[], cwd: string, env: NodeJS.ProcessEnv = process.env): Promise<void> {
  await new Promise<void>((resolvePromise, reject) => {
    const child = spawn(command, args, { cwd, env, stdio: "inherit", shell: command.endsWith(".cmd") });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) {
        resolvePromise();
      } else {
        reject(new Error(`${command} failed: ${signal ?? code}`));
      }
    });
  });
}

async function packDirectory(directory: string, expectedNative?: string): Promise<PackedPackage> {
  const manifest = await readManifest(join(directory, "package.json"));
  const output = execFileSync(npmCommand(), ["pack", "--json", "--ignore-scripts", "--cache", join(dirname(directory), "cache"), "--pack-destination", releaseRoot], {
    cwd: directory,
    encoding: "utf8",
    shell: process.platform === "win32",
  });
  const results = JSON.parse(output) as PackResult[];
  assert.equal(results.length, 1);
  const result = results[0]!;
  const files = result.files.map((file) => file.path);
  assert(files.includes("LICENSE"), "Every npm archive must include the MIT license");
  if (expectedNative === undefined) {
    assert(files.includes("dist/index.js") && files.includes("dist/index.d.ts") && files.includes("_native.cjs"));
    assert(!files.some((file) => file.endsWith(".node")), "The facade must load its optional native package");
  } else {
    assert.deepEqual(files.filter((file) => file.endsWith(".node")), [expectedNative]);
  }
  return {
    package_name: String(manifest["name"]),
    filename: result.filename,
    integrity: `sha512-${createHash("sha512").update(await readFile(join(releaseRoot, result.filename))).digest("base64")}`,
    manifest,
  };
}

async function pack(artifactDirectory?: string, selectedPlatform?: string): Promise<void> {
  const manifest = await readManifest(join(packageRoot, "package.json"));
  const version = (await readFile(join(repositoryRoot, "VERSION"), "utf8")).trim().replace(/^v/, "");
  assert.equal(manifest["version"], version);
  assert.equal(manifest["private"], undefined);
  assert.deepEqual([...(manifest["napi"] as Manifest)["targets"] as string[]].sort(), platforms.map((platform) => platform.target).sort());
  assert.deepEqual(manifest["optionalDependencies"], Object.fromEntries(platforms.map((platform) => [`@tokn/sdk-${platform.suffix}`, version])));
  const selected = selectedPlatform === undefined ? platforms : platforms.filter((platform) => platform.suffix === selectedPlatform);
  assert(selected.length > 0, `Unsupported platform: ${selectedPlatform}`);
  await rm(releaseRoot, { recursive: true, force: true });
  await mkdir(releaseRoot, { recursive: true });
  const staging = await mkdtemp(join(tmpdir(), "tokn-npm-pack-"));
  try {
    const facade = join(staging, "facade");
    await mkdir(facade);
    for (const path of ["package.json", "README.md", "dist", "src"]) {
      await cp(join(packageRoot, path), join(facade, path), { recursive: true });
    }
    const loader = artifactDirectory === undefined
      ? join(packageRoot, "_native.cjs")
      : join(resolve(artifactDirectory), "npm-native-linux-x64-gnu", "_native.cjs");
    await cp(loader, join(facade, "_native.cjs"));
    await cp(join(repositoryRoot, "LICENSE"), join(facade, "LICENSE"));
    const archives: PackedPackage[] = [];
    for (const platform of selected) {
      const nativeName = `tokn_sdk.${platform.suffix}.node`;
      const nativePath = artifactDirectory === undefined
        ? join(packageRoot, nativeName)
        : join(resolve(artifactDirectory), `npm-native-${platform.suffix}`, nativeName);
      const binary = await readFile(nativePath);
      const expectedMagic = { darwin: "cffaedfe", linux: "7f454c46", win32: "4d5a" }[platform.os];
      assert(binary.length > 64 && binary.subarray(0, expectedMagic.length / 2).toString("hex") === expectedMagic,
        `Invalid ${platform.suffix} native binary`);
      const platformManifest = await readManifest(join(packageRoot, "platforms", platform.suffix, "package.json"));
      assert.equal(platformManifest["name"], `@tokn/sdk-${platform.suffix}`);
      assert.equal(platformManifest["version"], version);
      assert.equal(platformManifest["main"], nativeName);
      assert.deepEqual(platformManifest["os"], [platform.os]);
      assert.deepEqual(platformManifest["cpu"], [platform.cpu]);
      if (platform.os === "linux") {
        assert.deepEqual(platformManifest["libc"], ["glibc"]);
      }
      const directory = join(staging, platform.suffix);
      await mkdir(directory);
      await writeFile(join(directory, "package.json"), `${JSON.stringify(platformManifest, null, 2)}\n`);
      await cp(nativePath, join(directory, nativeName));
      await cp(join(repositoryRoot, "LICENSE"), join(directory, "LICENSE"));
      await writeFile(join(directory, "README.md"), `# ${platformManifest["name"]}\n\nNative binding for [@tokn/sdk](https://www.npmjs.com/package/@tokn/sdk). Install the facade package instead.\n`);
      archives.push(await packDirectory(directory, nativeName));
    }
    archives.push(await packDirectory(facade));
    await writeFile(join(releaseRoot, "manifest.json"), `${JSON.stringify(archives, null, 2)}\n`);
    console.log(`Assembled ${archives.length} npm archives in ${releaseRoot}`);
  } finally {
    await rm(staging, { recursive: true, force: true });
  }
}

async function testInstalled(selectedPlatform: string): Promise<void> {
  const platform = platforms.find((candidate) => candidate.suffix === selectedPlatform);
  assert(platform !== undefined, `Unsupported platform: ${selectedPlatform}`);
  assert.equal(platform.os, process.platform);
  assert.equal(platform.cpu, process.arch);
  const archives = JSON.parse(await readFile(join(releaseRoot, "manifest.json"), "utf8")) as PackedPackage[];
  const facade = archives.find((archive) => archive.package_name === "@tokn/sdk");
  assert(facade !== undefined);
  assert(archives.some((archive) => archive.package_name === `@tokn/sdk-${selectedPlatform}`));
  const tarballs = new Map<string, Buffer>();
  for (const archive of archives) {
    const bytes = await readFile(join(releaseRoot, archive.filename));
    assert.equal(`sha512-${createHash("sha512").update(bytes).digest("base64")}`, archive.integrity);
    tarballs.set(`/-/${archive.filename}`, bytes);
  }
  let registry = "";
  const server = createServer((incoming, outgoing) => {
    const path = decodeURIComponent((incoming.url ?? "").split("?")[0]!);
    const tarball = tarballs.get(path);
    if (tarball !== undefined) {
      outgoing.writeHead(200, { "content-type": "application/octet-stream" });
      outgoing.end(tarball);
      return;
    }
    const archive = archives.find((candidate) => `/${candidate.package_name}` === path);
    if (archive === undefined) {
      outgoing.writeHead(404, { "content-type": "application/json" });
      outgoing.end(JSON.stringify({ error: "Package not included in the release fixture" }));
      return;
    }
    const version = String(archive.manifest["version"]);
    outgoing.writeHead(200, { "content-type": "application/json" });
    outgoing.end(JSON.stringify({
      name: archive.package_name,
      "dist-tags": { latest: version },
      versions: {
        [version]: {
          ...archive.manifest,
          dist: { tarball: `${registry}/-/${archive.filename}`, integrity: archive.integrity },
        },
      },
    }));
  });
  await new Promise<void>((resolvePromise) => server.listen(0, "127.0.0.1", resolvePromise));
  const address = server.address();
  assert(address !== null && typeof address === "object");
  registry = `http://127.0.0.1:${address.port}`;
  const fixture = await mkdtemp(join(tmpdir(), "tokn-npm-install-"));
  try {
    for (const runtime of ["node", "bun"] as const) {
      const directory = join(fixture, runtime);
      await mkdir(directory);
      await writeFile(join(directory, "package.json"), JSON.stringify({
        private: true,
        type: "module",
        dependencies: { "@tokn/sdk": facade.manifest["version"] },
      }));
      await writeFile(join(directory, ".npmrc"), `@tokn:registry=${registry}\n`);
      await cp(join(packageRoot, "scripts/installed-smoke.ts"), join(directory, "installed-smoke.ts"));
      const env = {
        ...process.env,
        TOKN_PACKAGE_VERSION: String(facade.manifest["version"]),
        TOKN_NATIVE_PACKAGE: `@tokn/sdk-${selectedPlatform}`,
        npm_config_userconfig: join(directory, ".npmrc"),
      };
      if (runtime === "node") {
        await run(npmCommand(), ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--registry", registry, "--cache", join(directory, "cache")], directory, env);
        await run(process.execPath, ["--experimental-strip-types", "installed-smoke.ts"], directory, env);
      } else {
        await run("bun", ["install", "--ignore-scripts", "--registry", registry, "--cache-dir", join(directory, "cache")], directory, env);
        await run("bun", ["run", "installed-smoke.ts"], directory, env);
      }
    }
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolvePromise) => server.close(() => resolvePromise()));
    await rm(fixture, { recursive: true, force: true });
  }
}

function checkGlibc(path: string): void {
  const output = execFileSync("readelf", ["--version-info", resolve(path)], { encoding: "utf8" });
  const versions = [...output.matchAll(/Name: GLIBC_(\d+)\.(\d+)/g)].map((match) => [Number(match[1]), Number(match[2])]);
  assert(versions.length > 0, "The Linux artifact must declare its glibc symbol requirements");
  assert(!output.includes("GLIBC_PRIVATE"), "The Linux artifact must not require private glibc symbols");
  assert(versions.every(([major, minor]) => major! < 2 || (major === 2 && minor! <= 28)),
    `The Linux artifact exceeds the supported glibc 2.28 floor: ${versions.map((version) => version.join(".")).join(", ")}`);
  console.log("Verified Linux native symbols require glibc 2.28 or earlier");
}

async function publish(): Promise<void> {
  assert.equal(process.env["TOKN_NPM_PUBLISH"], "1", "Publication requires an explicit TOKN_NPM_PUBLISH=1 opt-in");
  const archives = JSON.parse(await readFile(join(releaseRoot, "manifest.json"), "utf8")) as PackedPackage[];
  assert.deepEqual(archives.map((archive) => archive.package_name), [
    ...platforms.map((platform) => `@tokn/sdk-${platform.suffix}`), "@tokn/sdk",
  ]);
  const facade = await readManifest(join(packageRoot, "package.json"));
  const pending: PackedPackage[] = [];
  // Check every immutable registry version before the first publication. A retry
  // may reuse an already published archive only when its integrity is identical.
  for (const archive of archives) {
    assert.equal(archive.manifest["version"], facade["version"]);
    assert.equal(`sha512-${createHash("sha512").update(await readFile(join(releaseRoot, archive.filename))).digest("base64")}`, archive.integrity);
    const response = await fetch(`https://registry.npmjs.org/${encodeURIComponent(archive.package_name)}/${archive.manifest["version"]}`, {
      signal: AbortSignal.timeout(30_000),
    });
    if (response.status === 404) {
      pending.push(archive);
    } else {
      assert(response.ok, `Registry preflight failed for ${archive.package_name}: HTTP ${response.status}`);
      const published = await response.json() as Manifest;
      assert.equal((published["dist"] as Manifest)["integrity"], archive.integrity,
        `${archive.package_name} already exists with different bytes; npm versions cannot be replaced`);
      console.log(`Already published with matching integrity: ${archive.package_name}`);
    }
  }
  const tag = String(facade["version"]).includes("-") ? "next" : "latest";
  for (const archive of pending) {
    await run(npmCommand(), ["publish", join(releaseRoot, archive.filename), "--ignore-scripts", "--access", "public", "--provenance", "--tag", tag], packageRoot);
  }
}

const [command, first, second] = process.argv.slice(2);
switch (command) {
  case "pack":
    await pack(first === "--target" ? undefined : first, first === "--target" ? second : undefined);
    break;
  case "test":
    assert(first !== undefined, "Provide the platform suffix to test");
    await testInstalled(first);
    break;
  case "check-glibc":
    assert(first !== undefined, "Provide the Linux .node artifact path");
    checkGlibc(first);
    break;
  case "publish":
    await publish();
    break;
  default:
    throw new Error("Usage: release.ts pack [artifact-directory | --target suffix] | test suffix | check-glibc binary | publish");
}
