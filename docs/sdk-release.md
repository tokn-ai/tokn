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
earlier commit. Publish the verified branch artifacts manually using the steps
below.

## Packages

| Registry | Package | Contents |
| --- | --- | --- |
| PyPI | `tokn-requests` | Python `tokn_requests` module, typed models, native extension, and source distribution |
| npm | `@tokn/requests` | ESM façade, declarations, and internal native loader |
| npm | `@tokn/requests-linux-x64-gnu` | Linux x64 glibc native binding |
| npm | `@tokn/requests-darwin-arm64` | macOS arm64 native binding |
| npm | `@tokn/requests-win32-x64-msvc` | Windows x64 MSVC native binding |

The npm façade declares each native package as an exact-version optional
dependency. A consumer receives the package for its OS, CPU, and libc. Source
checkouts use local pnpm workspace links so development does not depend on a
previous registry publication.

## Build and download release artifacts

Pushing `v0.2.3-sdk` runs `release-python.yml` and `release-npm.yml` to build,
test, and upload packages. These branch runs do not publish. Wait for both
workflows to finish successfully on the same reviewed commit. Local debug
artifacts under `tmp/` exercise packaging and APIs; use the green branch CI
artifacts for publication.

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
the runs, then replace the two run-ID placeholders with the `databaseId` values
for **Python release** and **Release @tokn/requests**. Confirm both entries have the
same `headSha`, `status=completed`, and `conclusion=success`.

```sh
gh run list --repo tokn-ai/tokn --branch v0.2.3-sdk --limit 10 \
  --json databaseId,workflowName,headSha,status,conclusion

python_run=REPLACE_WITH_PYTHON_RUN_ID
npm_run=REPLACE_WITH_NPM_RUN_ID
release_dir=$(mktemp -d "${TMPDIR:-/tmp}/tokn-requests-0.2.3.XXXXXX")
mkdir "$release_dir/python" "$release_dir/npm"

gh run download "$python_run" --repo tokn-ai/tokn \
  --pattern 'python-*' --dir "$release_dir/python"
gh run download "$npm_run" --repo tokn-ai/tokn \
  --name npm-packages --dir "$release_dir/npm"
```

Keep `release_dir` available in the same shell for the commands below. The
Python download contains artifact-named subdirectories with three `.whl` files
and one `.tar.gz` file. The npm directory contains four `.tgz` archives and
`manifest.json`. Run IDs select the exact builds; separate fresh directories
prevent mixing previous artifacts.
[GitHub artifact download documentation](https://cli.github.com/manual/gh_run_download).

## Publish to PyPI manually

Sign in to [PyPI](https://pypi.org/), verify your email, and configure two-factor
authentication. In account settings, create an API token. For the initial
upload of a new project, use an account-wide token; after `tokn-requests` exists,
replace it with a project-scoped token. PyPI uses `__token__` as the upload
username and the complete token, including its `pypi-` prefix, as the password.
[PyPI token documentation](https://pypi.org/help/#apitoken).

Install Twine in an isolated environment, check all four downloaded distributions,
then upload them:

```sh
python3 -m venv "$release_dir/tools"
"$release_dir/tools/bin/python" -m pip install --upgrade twine
"$release_dir/tools/bin/python" -m twine check --strict \
  "$release_dir"/python/*/*.whl "$release_dir"/python/*/*.tar.gz
"$release_dir/tools/bin/python" -m twine upload --repository pypi --username __token__ \
  "$release_dir"/python/*/*.whl "$release_dir"/python/*/*.tar.gz
```

Paste the API token into Twine's hidden password/token prompt. Keep the token
out of command arguments, shell history, and repository files. Twine uploads
these existing files without rebuilding them.
[Twine upload and validation documentation](https://twine.readthedocs.io/en/stable/).

## Publish to npm manually

Use an npm account authorized to publish in the `@tokn` scope. Authenticate,
then publish the three native archives before the façade:

```sh
npm login --registry=https://registry.npmjs.org
npm publish "$release_dir/npm/tokn-requests-darwin-arm64-0.2.3.tgz" \
  --registry=https://registry.npmjs.org --access public --ignore-scripts
npm publish "$release_dir/npm/tokn-requests-linux-x64-gnu-0.2.3.tgz" \
  --registry=https://registry.npmjs.org --access public --ignore-scripts
npm publish "$release_dir/npm/tokn-requests-win32-x64-msvc-0.2.3.tgz" \
  --registry=https://registry.npmjs.org --access public --ignore-scripts
npm publish "$release_dir/npm/tokn-requests-0.2.3.tgz" \
  --registry=https://registry.npmjs.org --access public --ignore-scripts
```

Complete any browser login or two-factor prompts locally. Publishing the
downloaded tarballs preserves the files tested in CI.
[npm publish documentation](https://docs.npmjs.com/cli/v11/commands/npm-publish/).

## Verify and retry

Inspect the run's artifact versions and registry results. Install `tokn-requests==0.2.3`
and `@tokn/requests@0.2.3` in clean consumer environments and check that the expected
native modules load. Published files cannot be replaced; correct packaging
failures through a new version rather than replacing 0.2.3 artifacts.

If a PyPI upload stops partway through, compare the uploaded files' SHA-256
hashes with the reviewed artifacts, then upload only the missing files. Keep
the same artifacts for the retry.

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
