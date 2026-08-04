#!/usr/bin/env python3
"""Behavioral tests for the deterministic Tickr Lite archive."""

from __future__ import annotations

import hashlib
import importlib.util
import tarfile
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("package_tickr_lite.py")
SPEC = importlib.util.spec_from_file_location("package_tickr_lite", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
PACKAGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PACKAGE)


class PackageTickrLiteTest(unittest.TestCase):
    def test_archive_is_deterministic_and_contains_the_runnable_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / "tickr"
            ctx_binary = root / "tickr-ctx"
            binary.write_bytes(b"tickr-binary")
            ctx_binary.write_bytes(b"tickr-ctx-binary")

            first, first_checksum = PACKAGE.package(
                binary, ctx_binary, "0.1.1", "test-target", root / "first"
            )
            second, second_checksum = PACKAGE.package(
                binary, ctx_binary, "0.1.1", "test-target", root / "second"
            )

            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertEqual(first_checksum.read_text(), second_checksum.read_text())
            digest = hashlib.sha256(first.read_bytes()).hexdigest()
            self.assertEqual(
                first_checksum.read_text(), f"{digest}  {first.name}\n"
            )

            prefix = "tickr-lite-v0.1.1-test-target"
            with tarfile.open(first, "r:gz") as archive:
                members = {member.name: member for member in archive.getmembers()}

            required = {
                f"{prefix}/tickr",
                f"{prefix}/tickr-ctx",
                f"{prefix}/INSTALL.md",
                f"{prefix}/dsl/lib.ncl",
                f"{prefix}/examples/flake.nix",
                f"{prefix}/examples/flake.lock",
                f"{prefix}/examples/hello-world.ncl",
                f"{prefix}/examples/runtime-patch.ncl",
                f"{prefix}/examples/runtime-patch/choose.sh",
                f"{prefix}/examples/runtime-patch/patch.sh",
                f"{prefix}/examples/runtime-patch/echo-pause.sh",
                f"{prefix}/examples/runtime-patch/summary.sh",
            }
            self.assertTrue(required.issubset(members))
            self.assertEqual(members[f"{prefix}/tickr"].mode, 0o755)
            self.assertEqual(members[f"{prefix}/tickr-ctx"].mode, 0o755)
            self.assertNotIn(f"{prefix}/README.txt", members)


if __name__ == "__main__":
    unittest.main()
