# SDK releases

VERSION is the source of truth for the SDK release version. The Cargo
workspace, Python distribution, npm facade, native npm packages, desktop
metadata, and local lockfile entries must agree. The public packages are
tokn-requests on PyPI (import tokn_requests) and @tokn-ai/requests on npm.

From the source checkout, check metadata and run the relevant tests:

~~~sh
node --experimental-strip-types scripts/release/check-version.ts
cargo fmt --all
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
~~~

Development builds do not require release notes. Before publishing a version,
add docs/releases/vX.Y.Z.md for its VERSION value and run the stricter check:

~~~sh
node --experimental-strip-types scripts/release/check-version.ts --require-release-notes
~~~

## Packages

| Registry | Package | Contents |
| --- | --- | --- |
| PyPI | tokn-requests | Python tokn_requests module, typed models, native extension, and source distribution |
| npm | @tokn-ai/requests | ESM facade, declarations, and internal native loader |
| npm | @tokn-ai/requests-linux-x64-gnu | Linux x64 glibc native binding |
| npm | @tokn-ai/requests-darwin-arm64 | macOS arm64 native binding |
| npm | @tokn-ai/requests-win32-x64-msvc | Windows x64 MSVC native binding |

The npm facade declares each native package as an exact-version optional
dependency. Consumer installation selects the package for its OS, CPU, and
libc. Source checkouts use local pnpm workspace links.

Python preparation builds three cp310-abi3 wheels for Linux x64, macOS arm64,
and Windows x64, plus a source distribution. CI installs and tests the same
wheels on regular CPython 3.10–3.14, audits stable-ABI symbols, and rebuilds
the source distribution offline with locked dependencies. Free-threaded Python
requires a different ABI. See [Python packaging](../bindings/python/README.md).

npm preparation builds the three native packages, checks the Linux glibc 2.28
floor, assembles the facade, and tests installation of the packed archives
with Node.js and Bun. See [npm packaging](../bindings/typescript/README.md).

## Build and review artifacts

The Python and npm workflows run on manual dispatch. The examples below use a
POSIX shell. Choose a source branch or tag, record its commit, and read the
release version from that ref. Use a stable ref while reviewing artifacts.

~~~sh
source_ref=main
source_sha=$(git rev-parse "$source_ref")
release_tag=$(git show "$source_ref:VERSION")
release_version=$(printf '%s' "$release_tag" | cut -c2-)

gh workflow run release-python.yml --repo tokn-ai/tokn --ref "$source_ref" -f publish=false
gh workflow run release-npm.yml --repo tokn-ai/tokn --ref "$source_ref" -f publish=false
~~~

For build-only runs, the version input may be left empty; both workflows
validate the checkout's VERSION and package metadata. The Python run uploads
three wheels and an sdist; the npm run uploads four tarballs and manifest.json.
Neither build-only run publishes a package. A publish=true dispatch requires
an explicit version input matching VERSION, release notes, and configured
registry publishing credentials.

Find each completed run and verify that its headSha equals source_sha and its
conclusion is success. Use the databaseId for the reviewed run in the commands
below. Run IDs can differ between the two workflows.

~~~sh
gh run list --repo tokn-ai/tokn --workflow release-python.yml --limit 20 \
  --json databaseId,headSha,status,conclusion
gh run list --repo tokn-ai/tokn --workflow release-npm.yml --limit 20 \
  --json databaseId,headSha,status,conclusion

python_run=REPLACE_WITH_PYTHON_RUN_ID
npm_run=REPLACE_WITH_NPM_RUN_ID
release_dir=$(mktemp -d "/tmp/tokn-sdk-$release_version.XXXXXX")
mkdir "$release_dir/python" "$release_dir/npm"

gh run download "$python_run" --repo tokn-ai/tokn \
  --name python-wheel-linux-x64 --dir "$release_dir/python"
gh run download "$python_run" --repo tokn-ai/tokn \
  --name python-wheel-macos-arm64 --dir "$release_dir/python"
gh run download "$python_run" --repo tokn-ai/tokn \
  --name python-wheel-windows-x64 --dir "$release_dir/python"
gh run download "$python_run" --repo tokn-ai/tokn \
  --name python-sdist --dir "$release_dir/python"
gh run download "$npm_run" --repo tokn-ai/tokn \
  --name npm-packages --dir "$release_dir/npm"
~~~

Keep release_dir in the same shell for the publication commands. Confirm that
the Python directory has exactly three wheels and one source distribution.
Confirm that the npm directory has three native tarballs, one facade tarball,
and manifest.json, with the expected package names and release version.
Use a new directory for each release so artifacts cannot be mixed.
[GitHub artifact download documentation](https://cli.github.com/manual/gh_run_download).

## Publish manually

Check that the release version has not been published on either registry before
uploading. Registry versions are immutable. Use artifacts from the same
reviewed source commit for both registries. Authenticate with your PyPI and
npm accounts, then publish only the reviewed artifacts:

~~~sh
python3 -m venv "$release_dir/tools"
"$release_dir/tools/bin/python" -m pip install twine
"$release_dir/tools/bin/python" -m twine check --strict "$release_dir"/python/*
"$release_dir/tools/bin/python" -m twine upload "$release_dir"/python/*

npm login --registry=https://registry.npmjs.org
npm whoami --registry=https://registry.npmjs.org
~~~

The npm account needs write access to the @tokn-ai organization. Publish native
packages first and the facade only after all three native uploads succeed.
A prerelease version uses the next npm dist-tag; a stable version uses latest.

~~~sh
npm_tag=latest
case "$release_version" in
  *-*) npm_tag=next ;;
esac

publish_npm() {
  for suffix in darwin-arm64 linux-x64-gnu win32-x64-msvc; do
    npm publish "$release_dir/npm/tokn-ai-requests-$suffix-$release_version.tgz" \
      --registry=https://registry.npmjs.org --access public --ignore-scripts \
      --tag "$npm_tag" || return 1
  done
  npm publish "$release_dir/npm/tokn-ai-requests-$release_version.tgz" \
    --registry=https://registry.npmjs.org --access public --ignore-scripts \
    --tag "$npm_tag"
}
publish_npm
~~~

Complete any login and two-factor prompts locally. Publishing downloaded
tarballs preserves the files tested in CI.
[npm publish documentation](https://docs.npmjs.com/cli/v11/commands/npm-publish/).

## Verify and retry

Check the published PyPI files and npm versions, then install each package in
a clean consumer environment. Confirm that the Python module imports and the
npm native module loads on a supported platform. If an npm upload stops
partway through, retain the same archives. Compare an existing package's
dist.integrity from npm view with that package's integrity in manifest.json,
then publish only the missing archives. The automated npm job performs this
comparison and skips a matching version on retry. A conflicting archive or
incorrect published file requires a new version.

## Optional trusted publishing

Manual publication needs no GitHub publishing environment. For automated
publication, configure trusted publishers for owner tokn-ai, repository tokn,
with these workflow and environment pairs:

| Registry | Workflow | GitHub environment |
| --- | --- | --- |
| PyPI tokn-requests | release-python.yml | pypi |
| All four npm packages | release-npm.yml | npm |

Register a trusted publisher for each npm package and allow direct npm
publish. Configure the matching GitHub environments and their approval rules.
After reviewing a build-only run, a separate dispatch with publish=true and an
explicit matching version enables publication after all artifact tests pass.
Do not run an automated publication for a version already published manually.

~~~sh
gh workflow run release-python.yml --repo tokn-ai/tokn --ref "$source_ref" \
  -f version="$release_version" -f publish=true
gh workflow run release-npm.yml --repo tokn-ai/tokn --ref "$source_ref" \
  -f version="$release_version" -f publish=true
~~~

See [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/)
and [npm trusted publishing](https://docs.npmjs.com/trusted-publishers/).
