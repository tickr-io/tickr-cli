#!/usr/bin/env python3
"""Non-destructive public-source and development-Compose security gate."""
import argparse
import json
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUST_FORBIDDEN = (
    ("compiled-development-credential", re.compile(r"minioadmin|postgres://postgres:postgres", re.I)),
    ("all-interface-api-bind", re.compile(r"0\.0\.0\.0:6000|\[0,\s*0,\s*0,\s*0\].*6000")),
    ("private-key-material", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
)


def source_findings(root: Path):
    findings = []
    for path in sorted((root / "src").rglob("*.rs")):
        text = path.read_text(encoding="utf-8")
        for rule, pattern in RUST_FORBIDDEN:
            if rule == "compiled-development-credential" and "tests" in path.parts:
                continue
            for match in pattern.finditer(text):
                findings.append((path.relative_to(root).as_posix(), text.count("\n", 0, match.start()) + 1, rule))
    return findings


def compose_findings(root: Path):
    proc = subprocess.run(
        ["docker", "compose", "--file", str(root / "docker-compose-infra.yml"), "config", "--format", "json"],
        check=True, capture_output=True, text=True,
    )
    data = json.loads(proc.stdout)
    findings = []
    for name, service in data.get("services", {}).items():
        image = service.get("image", "")
        if "@sha256:" not in image:
            findings.append(("docker-compose-infra.yml", 1, f"mutable-image:{name}"))
        for port in service.get("ports", []):
            if port.get("host_ip") != "127.0.0.1":
                findings.append(("docker-compose-infra.yml", 1, f"non-loopback-port:{name}"))
    return findings


class Tests(unittest.TestCase):
    def test_source_rules_detect_credentials_bind_and_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "src").mkdir()
            (root / "src" / "bad.rs").write_text(
                'const A: &str = "minioadmin";\nconst B: &str = "0.0.0.0:6000";\n'
                'const K: &str = "-----BEGIN PRIVATE KEY-----";\n', encoding="utf-8"
            )
            self.assertEqual(len(source_findings(root)), 3)

    def test_safe_config_names_and_loopback_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "src").mkdir()
            (root / "src" / "safe.rs").write_text(
                'let key = env("TICKR_LOG_STORAGE_SECRET_ACCESS_KEY");\n'
                'let bind = "127.0.0.1:6000";\n', encoding="utf-8"
            )
            self.assertEqual(source_findings(root), [])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return 0 if unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        ).wasSuccessful() else 1
    findings = source_findings(ROOT) + compose_findings(ROOT)
    for path, line, rule in findings:
        print(f"{path}:{line}: {rule}")
    if findings:
        print(f"security source gate: FAILED ({len(findings)} findings)")
        return 1
    print("security source gate: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
