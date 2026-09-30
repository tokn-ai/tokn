# SDK release preparation

The release version is `0.2.3`. `VERSION` is the source of truth, with a leading
`v`; Cargo, Python, npm, desktop metadata, and local Cargo lock entries use the
same version. Run the shared check from the repository root:

```sh
node --experimental-strip-types scripts/release/check-version.ts
cargo fmt --all
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
```

The version check also runs in CI and the CLI release workflow. The SDK release
source is the `v0.2.3-sdk` branch; the existing `v0.2.3` CLI tag identifies an
earlier commit. [PyPI `tokn-requests` 0.2.3](https://pypi.org/project/tokn-requests/0.2.3/)
has already been published from audited commit `8528671`. Its three `cp310-abi3`
wheels and source distribution match the verified CI artifacts. Do not upload
those files again. The remaining manual publication is npm `@tokn-ai/requests`
and its three native packages.

## Packages

| Registry | Package | Contents |
| --- | --- | --- |
| PyPI | `tokn-requests` | Python `tokn_requests` module, typed models, native extension, and source distribution |
| npm | `@tokn-ai/requests` | ESM façade, declarations, and internal native loader |
| npm | `@tokn-ai/requests-linux-x64-gnu` | Linux x64 glibc native binding |
| npm | `@tokn-ai/requests-darwin-arm64` | macOS arm64 native binding |
| npm | `@tokn-ai/requests-win32-x64-msvc` | Windows x64 MSVC native binding |

The npm façade declares each native package as an exact-version optional
dependency. A consumer receives the package for its OS, CPU, and libc. Source
checkouts use local pnpm workspace links so development does not depend on a
previous registry publication.

## Build and download npm release artifacts

Pushing `v0.2.3-sdk` runs `release-python.yml` and `release-npm.yml` to build,
test, and upload packages. These branch runs do not publish. For npm, wait for
`release-npm.yml` to finish successfully on the reviewed commit that contains
the `@tokn-ai` scope change. Local debug artifacts under
`tmp/` exercise packaging and APIs; use that green branch CI artifact for npm
publication. The earlier PyPI publication remains tied to commit `8528671`.

The initial branch push also makes builds available while the new workflows
are absent from the default branch. GitHub's **Run workflow** button requires
the workflow file on the default branch.
[GitHub workflow dispatch documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_dispatch).

Python preparation builds three `cp310-abi3` wheels, one for each of Linux x64,
macOS arm64, and Windows x64, plus one source distribution. Each wheel uses
CPython's Python 3.10 stable ABI, and CI installs and tests the same wheel on
regular CPython 3.10–3.14. Free-threaded Python builds use a different ABI.
The source distribution includes the Rust path dependencies and is rebuilt
offline with locked dependencies. See [Python packaging](../bindings/python/README.md).

npm preparation builds the three native packages, assembles the façade, checks
Linux glibc compatibility, and tests installation of the packed artifacts with
Node.js and Bun. See [npm packaging](../bindings/typescript/README.md) for the
platform baseline and package assembly commands.

The following commands use a POSIX shell and an authenticated GitHub CLI. List
the npm workflow runs, then replace the run-ID placeholder with the `databaseId`
for the reviewed scope-change commit. Confirm its `headSha`,
`status=completed`, and `conclusion=success`.

```sh
gh run list --repo tokn-ai/tokn --workflow release-npm.yml \
  --branch v0.2.3-sdk --limit 10 \
  --json databaseId,headSha,status,conclusion

npm_run=REPLACE_WITH_NPM_RUN_ID
release_dir=$(mktemp -d "${TMPDIR:-/tmp}/tokn-ai-requests-0.2.3.XXXXXX")
mkdir "$release_dir/npm"

gh run download "$npm_run" --repo tokn-ai/tokn \
  --name npm-packages --dir "$release_dir/npm"
```

Keep `release_dir` available in the same shell for the commands below. The npm
directory contains four `.tgz` archives and `manifest.json`. Use a fresh
directory so earlier `@tokn/requests` archives cannot be mixed into this
release. Check the manifest names and filenames before publishing.
[GitHub artifact download documentation](https://cli.github.com/manual/gh_run_download).

## Publish to npm manually

Use an npm account authorized to publish in the `@tokn-ai` scope. Authenticate,
then publish the three native archives before the façade:

```sh
npm login --registry=https://registry.npmjs.org
npm whoami --registry=https://registry.npmjs.org
npm publish "$release_dir/npm/tokn-ai-requests-darwin-arm64-0.2.3.tgz" \
  --registry=https://registry.npmjs.org --access public --ignore-scripts
npm publish "$release_dir/npm/tokn-ai-requests-linux-x64-gnu-0.2.3.tgz" \
  --registry=https://registry.npmjs.org --access public --ignore-scripts
npm publish "$release_dir/npm/tokn-ai-requests-win32-x64-msvc-0.2.3.tgz" \
  --registry=https://registry.npmjs.org --access public --ignore-scripts
npm publish "$release_dir/npm/tokn-ai-requests-0.2.3.tgz" \
  --registry=https://registry.npmjs.org --access public --ignore-scripts
```

Complete any browser login or two-factor prompts locally. Publishing the
downloaded tarballs preserves the files tested in CI.
[npm publish documentation](https://docs.npmjs.com/cli/v11/commands/npm-publish/).

## Verify and retry

Inspect the run's artifact versions and registry results. Install
`@tokn-ai/requests@0.2.3` in a clean consumer environment and check that its
native module loads. Published files cannot be replaced; correct packaging
failures through a new version rather than replacing 0.2.3 artifacts.

If an npm upload stops partway through, retain the same reviewed archives. Check
an already uploaded package with `npm view PACKAGE@0.2.3 dist.integrity` and
compare it with that package's `integrity` in the downloaded `manifest.json`.
Continue with the missing archives when the existing ones match. The OIDC
workflow performs this check before uploading and skips matching versions on a
retry. A conflicting archive requires a new release version.

## Optional automation for future releases

Manual publication above does not require GitHub publishing environments or
OIDC registration. For future automated releases, configure trusted publishers
with owner `tokn-ai`, repository `tokn`, and these workflow/environment pairs:

| Registry | Workflow | GitHub environment |
| --- | --- | --- |
| PyPI `tokn-requests` | `release-python.yml` | `pypi` |
| All four npm packages | `release-npm.yml` | `npm` |

Create the matching GitHub environments and enable direct `npm publish` in
each npm trusted publisher's allowed actions. Once dispatch is available,
`publish=false` rehearses a release and `publish=true` enables its publishing
job. Branch-push runs continue to build only. See
[PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/)
and [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/).
