#!/usr/bin/env python3
"""Fail npm audits except for narrow, expiring, inapplicable advisories."""

import argparse
import json
import subprocess
import unittest
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NPM_PACKAGES = (ROOT / "console", ROOT / "docs-site")


@dataclass(frozen=True)
class ExceptionPolicy:
    package: str
    expires: date
    reason: str


EXCEPTIONS = {
    "https://github.com/advisories/GHSA-qwww-vcr4-c8h2": ExceptionPolicy(
        package="react-router",
        expires=date(2026, 8, 18),
        reason=(
            "The advisory affects only unstable React Server Components APIs; "
            "Tickr Console is a client-rendered Vite SPA and uses none. React "
            "Router 8.3.0 is the declared fix but is not published to npm."
        ),
    ),
}


def resolved_advisories(package: str, vulnerabilities: dict, seen=None):
    seen = set() if seen is None else set(seen)
    if package in seen:
        return []
    seen.add(package)
    vulnerability = vulnerabilities.get(package, {})
    resolved = []
    for source in vulnerability.get("via", []):
        if isinstance(source, str):
            resolved.extend(resolved_advisories(source, vulnerabilities, seen))
        elif isinstance(source, dict):
            resolved.append((source.get("name"), source.get("url")))
    return resolved


def findings(report: dict, today: date):
    vulnerabilities = report.get("vulnerabilities", {})
    problems = set()
    for package in vulnerabilities:
        advisories = resolved_advisories(package, vulnerabilities)
        if not advisories:
            problems.add(f"{package}: vulnerability has no resolvable advisory")
        for source_package, url in advisories:
            policy = EXCEPTIONS.get(url)
            if policy is None:
                problems.add(f"{source_package}: unaccepted advisory {url}")
            elif source_package != policy.package:
                problems.add(
                    f"{source_package}: advisory {url} exception is scoped to "
                    f"{policy.package}"
                )
            elif today > policy.expires:
                problems.add(
                    f"{source_package}: advisory {url} exception expired "
                    f"{policy.expires.isoformat()}"
                )
    return sorted(problems)


class Tests(unittest.TestCase):
    def report(self, url="https://github.com/advisories/GHSA-qwww-vcr4-c8h2"):
        return {
            "vulnerabilities": {
                "react-router": {
                    "via": [{"name": "react-router", "url": url}],
                },
                "react-router-dom": {"via": ["react-router"]},
            }
        }

    def test_current_scoped_exception_passes(self):
        self.assertEqual(findings(self.report(), date(2026, 8, 4)), [])

    def test_unknown_advisory_fails(self):
        result = findings(
            self.report("https://github.com/advisories/GHSA-unknown"),
            date(2026, 8, 4),
        )
        self.assertEqual(len(result), 1)
        self.assertIn("unaccepted advisory", result[0])

    def test_expired_exception_fails(self):
        result = findings(self.report(), date(2026, 8, 19))
        self.assertEqual(len(result), 1)
        self.assertIn("expired", result[0])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1

    all_problems = []
    for package_dir in NPM_PACKAGES:
        audit = subprocess.run(
            ["npm", "audit", "--package-lock-only", "--json"],
            cwd=package_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        if audit.returncode not in (0, 1):
            print(
                audit.stderr.strip()
                or f"{package_dir.name}: npm audit failed with exit {audit.returncode}"
            )
            return audit.returncode
        try:
            report = json.loads(audit.stdout)
        except json.JSONDecodeError as error:
            print(f"{package_dir.name}: npm audit returned invalid JSON: {error}")
            return 2

        problems = [
            f"{package_dir.name}: {problem}"
            for problem in findings(report, date.today())
        ]
        all_problems.extend(problems)

        active = sorted(
            url
            for url, policy in EXCEPTIONS.items()
            if date.today() <= policy.expires
        )
        if report.get("vulnerabilities"):
            for url in active:
                policy = EXCEPTIONS[url]
                print(
                    f"{package_dir.name}: npm audit exception: {url} through "
                    f"{policy.expires.isoformat()} — {policy.reason}"
                )
        print(f"{package_dir.name}: npm audit policy checked")

    for problem in all_problems:
        print(problem)
    if all_problems:
        print(f"npm audit policy: FAILED ({len(all_problems)} findings)")
        return 1
    print("npm audit policy: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
