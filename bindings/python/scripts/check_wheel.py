"""Verify the Python package and its Python 3.10 stable-ABI wheel tags."""

import argparse
from email.parser import BytesParser
from pathlib import Path
from zipfile import ZipFile


def check_wheel(path: Path, version: str) -> None:
  prefix = f"tokn_requests-{version}"
  assert path.name.startswith(f"{prefix}-cp310-abi3-"), path.name
  assert path.suffix == ".whl", path.name
  metadata_root = f"{prefix}.dist-info"
  with ZipFile(path) as archive:
    names = set(archive.namelist())
    metadata = BytesParser().parsebytes(archive.read(f"{metadata_root}/METADATA"))
    assert metadata["Name"] == "tokn-requests", metadata["Name"]
    assert metadata["Version"] == version, metadata["Version"]
    assert metadata["Requires-Python"] == ">=3.10", metadata["Requires-Python"]
    wheel = BytesParser().parsebytes(archive.read(f"{metadata_root}/WHEEL"))
    assert wheel["Root-Is-Purelib"] == "false", wheel["Root-Is-Purelib"]
    tags = wheel.get_all("Tag", [])
    assert tags and all(tag.startswith("cp310-abi3-") for tag in tags), tags
    for name in (
      "tokn_requests/__init__.py",
      "tokn_requests/py.typed",
      "tokn_requests/_native.pyi",
      f"{metadata_root}/licenses/LICENSE",
    ):
      assert name in names, name
    native = names & {"tokn_requests/_native.abi3.so", "tokn_requests/_native.pyd"}
    assert len(native) == 1, native
  print(f"Verified {path.name}: tokn-requests {version}, CPython 3.10 stable ABI")


def main() -> None:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("wheels", type=Path, nargs="+")
  args = parser.parse_args()
  root = Path(__file__).resolve().parents[3]
  version = (root / "VERSION").read_text().strip().removeprefix("v")
  for path in args.wheels:
    check_wheel(path, version)


if __name__ == "__main__":
  main()
