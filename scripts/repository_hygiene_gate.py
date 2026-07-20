#!/usr/bin/env python3
"""Keep public-facing documentation focused on the current product."""
import argparse
import re
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN = {
    "repository-history-document": re.compile(r"REPOSITORY_SPLIT|repository split", re.I),
    "private-sibling-identifier": re.compile(r"tickr-ctrl", re.I),
    "private-crate-identifier": re.compile(r"\btickr_(?:server|frontend|ctrl(?:_proto)?)\b", re.I),
    "publication-status": re.compile(r"do not make .* public|visibility remains private|private release", re.I),
    "workspace-path": re.compile(r"(?:~/tickr-io|/Users/[^/\s]+/tickr-io|/home/[^/\s]+/tickr-io)"),
    "bootstrap-constraint": re.compile(r"bootstrap constraints?", re.I),
}


def public_documents(root: Path):
    for name in ("README.md", "CONTRIBUTING.md", "SECURITY.md", "NOTICE"):
        path = root / name
        if path.is_file():
            yield path
    docs = root / "docs"
    if docs.is_dir():
        yield from sorted(docs.rglob("*.md"))


def inspected_files(root: Path):
    yield from public_documents(root)
    for directory in ("src", "proto", "console/src", "examples"):
        base = root / directory
        if base.is_dir():
            yield from (
                path for path in sorted(base.rglob("*"))
                if path.is_file() and path.suffix in {".rs", ".proto", ".ts", ".tsx", ".ncl"}
            )


def findings(root: Path = ROOT):
    result = []
    for path in inspected_files(root):
        text = path.read_text(encoding="utf-8")
        for rule, pattern in FORBIDDEN.items():
            for match in pattern.finditer(text):
                result.append((
                    path.relative_to(root).as_posix(),
                    text.count("\n", 0, match.start()) + 1,
                    rule,
                ))
    if (root / "REPOSITORY_SPLIT.md").exists():
        result.append(("REPOSITORY_SPLIT.md", 1, "repository-history-document"))
    return result


class Tests(unittest.TestCase):
    def test_internal_release_language_is_detected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "README.md").write_text(
                "See REPOSITORY_SPLIT.md; visibility remains private.", encoding="utf-8"
            )
            self.assertEqual(len(findings(root)), 2)

    def test_private_crate_name_is_detected_in_source(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "src").mkdir()
            (root / "src/lib.rs").write_text("use tickr_server::Thing;", encoding="utf-8")
            self.assertEqual(findings(root), [("src/lib.rs", 1, "private-crate-identifier")])

    def test_current_product_documentation_passes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "docs").mkdir()
            (root / "README.md").write_text("Tickr workflow runtime", encoding="utf-8")
            (root / "docs/architecture.md").write_text("Current components", encoding="utf-8")
            self.assertEqual(findings(root), [])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    problems = findings()
    for path, line, rule in problems:
        print(f"{path}:{line}: {rule}")
    if problems:
        print(f"repository hygiene gate: FAILED ({len(problems)} findings)")
        return 1
    print("repository hygiene gate: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
