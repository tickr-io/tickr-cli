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
            cli_binary = root / "tickr-cli"
            lite_binary = root / "tickr-lite"
            ctx_binary = root / "tickr-ctx"
            polyglot_go_binary = root / "tickr-polyglot-go"
            polyglot_rust_binary = root / "tickr-polyglot-rust"
            cli_binary.write_bytes(b"tickr-cli-binary")
            lite_binary.write_bytes(b"tickr-lite-binary")
            ctx_binary.write_bytes(b"tickr-ctx-binary")
            polyglot_go_binary.write_bytes(b"polyglot-go-binary")
            polyglot_rust_binary.write_bytes(b"polyglot-rust-binary")

            first, first_checksum = PACKAGE.package(
                cli_binary,
                lite_binary,
                ctx_binary,
                polyglot_go_binary,
                polyglot_rust_binary,
                "0.1.5",
                "test-target",
                root / "first",
            )
            second, second_checksum = PACKAGE.package(
                cli_binary,
                lite_binary,
                ctx_binary,
                polyglot_go_binary,
                polyglot_rust_binary,
                "0.1.5",
                "test-target",
                root / "second",
            )

            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertEqual(first_checksum.read_text(), second_checksum.read_text())
            digest = hashlib.sha256(first.read_bytes()).hexdigest()
            self.assertEqual(
                first_checksum.read_text(), f"{digest}  {first.name}\n"
            )

            prefix = "tickr-lite-v0.1.5-test-target"
            with tarfile.open(first, "r:gz") as archive:
                members = {member.name: member for member in archive.getmembers()}

            required = {
                f"{prefix}/tickr-cli",
                f"{prefix}/tickr-lite",
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
                f"{prefix}/examples/polyglot.ncl",
                f"{prefix}/examples/polyglot/greet.py",
                f"{prefix}/examples/polyglot/greet.js",
                f"{prefix}/examples/polyglot/greet.go",
                f"{prefix}/examples/polyglot/greet.rs",
                f"{prefix}/examples/polyglot/bin/tickr-polyglot-go",
                f"{prefix}/examples/polyglot/bin/tickr-polyglot-rust",
            }
            self.assertTrue(required.issubset(members))
            self.assertEqual(members[f"{prefix}/tickr-cli"].mode, 0o755)
            self.assertEqual(members[f"{prefix}/tickr-lite"].mode, 0o755)
            self.assertEqual(members[f"{prefix}/tickr-ctx"].mode, 0o755)
            self.assertEqual(
                members[f"{prefix}/examples/polyglot/bin/tickr-polyglot-go"].mode,
                0o755,
            )
            self.assertEqual(
                members[f"{prefix}/examples/polyglot/bin/tickr-polyglot-rust"].mode,
                0o755,
            )
            self.assertNotIn(f"{prefix}/README.txt", members)


if __name__ == "__main__":
    unittest.main()
