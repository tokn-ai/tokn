"""Build an sdist whose Cargo.lock matches maturin's reduced workspace.

Requires Python 3.12 or newer, maturin, and cached Cargo dependencies.
Workaround for https://github.com/PyO3/maturin/issues/2609.
"""

from __future__ import annotations

import argparse
import copy
import gzip
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
from typing import Any


def package_key(package: dict[str, Any]) -> tuple[str, str, str | None]:
  return package["name"], package["version"], package.get("source")


def reconcile_lockfile(source: Path) -> bytes:
  lockfile = source / "Cargo.lock"
  original = tomllib.loads(lockfile.read_text())
  manifest = source / "bindings/python/Cargo.toml"
  metadata_command = [
    "cargo", "metadata", "--offline", "--format-version", "1",
    "--manifest-path", str(manifest),
  ]
  subprocess.run(metadata_command, check=True, stdout=subprocess.DEVNULL)
  updated = lockfile.read_bytes()
  reconciled = tomllib.loads(updated.decode())
  original_packages = {package_key(package): package for package in original["package"]}
  for package in reconciled["package"]:
    retained = original_packages.get(package_key(package))
    if retained is None or retained.get("checksum") != package.get("checksum"):
      raise RuntimeError(f"Source distribution changed a dependency pin: {package_key(package)}")
  subprocess.run(metadata_command + ["--locked"], check=True, stdout=subprocess.DEVNULL)
  return updated


def build_sdist(out: Path, maturin: str) -> Path:
  repository = Path(__file__).resolve().parents[3]
  manifest = repository / "bindings/python/Cargo.toml"
  out = out.resolve()
  with tempfile.TemporaryDirectory(prefix="tokn-python-sdist-") as scratch:
    temporary = Path(scratch)
    subprocess.run(
      [
        maturin, "sdist", "--manifest-path", str(manifest),
        "--out", str(temporary / "dist"),
      ],
      cwd=repository,
      env={**os.environ, "CARGO_NET_OFFLINE": "true"},
      check=True,
    )
    archives = list((temporary / "dist").glob("*.tar.gz"))
    if len(archives) != 1:
      raise RuntimeError(f"Expected one source distribution, got {archives}")
    archive_path = archives[0]
    with tarfile.open(archive_path) as archive:
      roots = {member.name.split("/")[0] for member in archive.getmembers()}
      if len(roots) != 1:
        raise RuntimeError(f"Expected one source root, got {roots}")
      root = roots.pop()
      archive.extractall(temporary / "source", filter="data")
    lockfile = reconcile_lockfile(temporary / "source" / root)
    prepared = temporary / archive_path.name
    with tarfile.open(archive_path) as original, prepared.open("wb") as output:
      with gzip.GzipFile(fileobj=output, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
          for member in original.getmembers():
            if member.name == f"{root}/Cargo.lock":
              member = copy.copy(member)
              member.size = len(lockfile)
              archive.addfile(member, io.BytesIO(lockfile))
            else:
              archive.addfile(member, original.extractfile(member) if member.isfile() else None)
    out.mkdir(parents=True, exist_ok=True)
    destination = out / prepared.name
    shutil.copyfile(prepared, destination)
    return destination


def main() -> None:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--out", type=Path, default=Path("dist"))
  executable = Path(sys.executable).with_name("maturin.exe" if sys.platform == "win32" else "maturin")
  parser.add_argument("--maturin", default=str(executable) if executable.is_file() else "maturin")
  args = parser.parse_args()
  print(f"Prepared locked source distribution: {build_sdist(args.out, args.maturin)}")


if __name__ == "__main__":
  main()
