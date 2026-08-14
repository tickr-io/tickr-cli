#!/usr/bin/env python3
"""Reject retired public Control-plane connection names outside classified exceptions."""
from __future__ import annotations

import argparse
import re
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LEGACY_VARIABLE = re.compile(r"TICKR_COORDINATOR_(?:HTTP|RELAY)_URL")
LEGACY_CLIENT = re.compile(r"\b(?:CoordinatorClient|coordinator_client)\b")

# The configuration regression test proves the deliberate clean break. Public
# migration guidance names the retired keys so operators can replace them.
ALLOWED_LEGACY_VARIABLE_FILES = {
    Path("src/proto/src/config.rs"),
    Path("INSTALL.md"),
    Path("docs-site/docs/get-started/install-lite.md"),
    Path("docs-site/docs/operate/configuration.md"),
    Path("docs-site/docs/reference/configuration.md"),
}
SCANNED_ROOTS = ("src", "tests", "docs", "docs-site/docs", ".github")


def inspected_files(root: Path):
    for relative in SCANNED_ROOTS:
        directory = root / relative
        if directory.is_dir():
            yield from sorted(path for path in directory.rglob("*") if path.is_file())
    for name in ("INSTALL.md", "README.md", ".env.example", "Procfile"):
        path = root / name
        if path.is_file():
            yield path


def findings(root: Path = ROOT) -> list[str]:
    result = []
    for path in inspected_files(root):
        relative = path.relative_to(root)
        text = path.read_text(encoding="utf-8")
        if relative not in ALLOWED_LEGACY_VARIABLE_FILES:
            result.extend(
                f"{relative}:{text.count(chr(10), 0, match.start()) + 1}: retired-variable"
                for match in LEGACY_VARIABLE.finditer(text)
            )
        result.extend(
            f"{relative}:{text.count(chr(10), 0, match.start()) + 1}: retired-client-name"
            for match in LEGACY_CLIENT.finditer(text)
        )
    return result


class Tests(unittest.TestCase):
    def test_rejects_retired_public_identifiers(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            (root / "src" / "bad.rs").write_text(
                'let url = "TICKR_COORDINATOR_HTTP_URL";\nstruct CoordinatorClient;\n',
                encoding="utf-8",
            )
            self.assertEqual(
                findings(root),
                [
                    "src/bad.rs:1: retired-variable",
                    "src/bad.rs:2: retired-client-name",
                ],
            )

    def test_allows_classified_migration_and_internal_coordination(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src/proto/src").mkdir(parents=True)
            (root / "src/proto/src/config.rs").write_text(
                'let retired = "TICKR_COORDINATOR_HTTP_URL";\nstruct IngressCoordinator;\n',
                encoding="utf-8",
            )
            self.assertEqual(findings(root), [])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    result = findings()
    for finding in result:
        print(finding)
    if result:
        print(f"control-plane terminology gate: FAILED ({len(result)} findings)")
        return 1
    print("control-plane terminology gate: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
