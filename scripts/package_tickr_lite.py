#!/usr/bin/env python3
"""Create a deterministic self-contained Tickr Lite release archive."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import os
import re
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$")
TARGET = re.compile(r"^[0-9A-Za-z_.-]+$")


def add_bytes(archive: tarfile.TarFile, name: str, data: bytes, mode: int, mtime: int) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mode = mode
    info.mtime = mtime
    info.uid = 0
    info.gid = 0
    info.uname = "root"
    info.gname = "root"
    archive.addfile(info, fileobj=io.BytesIO(data))

def append_file(
    members: list[tuple[str, Path, int]], destination: str, source: Path, mode: int
) -> None:
    if not source.is_file() or source.is_symlink():
        raise FileNotFoundError(f"release input is absent or not a regular file: {source}")
    members.append((destination, source, mode))


def append_dsl(members: list[tuple[str, Path, int]]) -> None:
    dsl_root = ROOT / "dsl"
    dsl_files = sorted(dsl_root.rglob("*.ncl"))
    if not dsl_files:
        raise FileNotFoundError(f"Core DSL is absent: {dsl_root}")
    for source in dsl_files:
        append_file(members, f"dsl/{source.relative_to(dsl_root)}", source, 0o644)


def append_examples(members: list[tuple[str, Path, int]]) -> None:
    examples = ROOT / "examples"
    for relative in [
        "flake.nix",
        "flake.lock",
        "hello-world.ncl",
        "runtime-patch.ncl",
        "runtime-patch/choose.sh",
        "runtime-patch/patch.sh",
        "runtime-patch/echo-pause.sh",
        "runtime-patch/summary.sh",
    ]:
        append_file(members, f"examples/{relative}", examples / relative, 0o644)




def package(
    binary: Path, ctx_binary: Path, version: str, target: str, output: Path
) -> tuple[Path, Path]:
    if not VERSION.fullmatch(version):
        raise ValueError(f"invalid release version: {version}")
    manifest = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    workspace_package = manifest.split("[workspace.package]", 1)[1].split("\n[", 1)[0]
    version_match = re.search(r'^version\s*=\s*"([^"]+)"', workspace_package, re.MULTILINE)
    if version_match is None:
        raise ValueError("workspace package version is absent from Cargo.toml")
    workspace_version = version_match.group(1)
    if version != workspace_version:
        raise ValueError(
            f"release version {version} does not match workspace version {workspace_version}"
        )
    if not TARGET.fullmatch(target):
        raise ValueError(f"invalid target name: {target}")

    members: list[tuple[str, Path, int]] = []
    append_file(members, "tickr", binary, 0o755)
    append_file(members, "tickr-ctx", ctx_binary, 0o755)
    append_file(members, "INSTALL.md", ROOT / "INSTALL.md", 0o644)
    append_file(members, "LICENSE", ROOT / "LICENSE", 0o644)
    append_file(members, "NOTICE", ROOT / "NOTICE", 0o644)
    append_file(
        members,
        "THIRD_PARTY_NOTICES.md",
        ROOT / "THIRD_PARTY_NOTICES.md",
        0o644,
    )
    append_dsl(members)
    append_examples(members)

    output.mkdir(parents=True, exist_ok=True)
    artifact = f"tickr-lite-v{version}-{target}"
    archive_path = output / f"{artifact}.tar.gz"
    mtime = int(os.environ.get("SOURCE_DATE_EPOCH", "0"))

    with archive_path.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
                directory = tarfile.TarInfo(artifact)
                directory.type = tarfile.DIRTYPE
                directory.mode = 0o755
                directory.mtime = mtime
                directory.uid = 0
                directory.gid = 0
                directory.uname = "root"
                directory.gname = "root"
                archive.addfile(directory)
                for destination, source, mode in sorted(members):
                    add_bytes(
                        archive,
                        f"{artifact}/{destination}",
                        source.read_bytes(),
                        mode,
                        mtime,
                    )

    digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    checksum_path = archive_path.with_suffix(archive_path.suffix + ".sha256")
    checksum_path.write_text(f"{digest}  {archive_path.name}\n", encoding="utf-8")
    return archive_path, checksum_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--ctx-binary", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    archive, checksum = package(
        args.binary, args.ctx_binary, args.version, args.target, args.output
    )
    print(archive)
    print(checksum)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
