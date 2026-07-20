#!/usr/bin/env python3
"""Enforce a self-contained workspace with registry-only external dependencies."""
import argparse
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_WORKSPACE = {
    "tickr", "tickr_api", "tickr_conductor", "tickr_ctx", "tickr_executor",
    "tickr_migrations", "tickr_proto",
}


def metadata(root):
    result = subprocess.run(
        ["cargo", "metadata", "--locked", "--format-version", "1"],
        cwd=root, check=True, capture_output=True, text=True,
    )
    return json.loads(result.stdout)


def findings(data, root=ROOT):
    problems = []
    packages = {package["id"]: package for package in data["packages"]}
    workspace_ids = set(data["workspace_members"])
    workspace_names = {packages[package_id]["name"] for package_id in workspace_ids}
    if workspace_names != EXPECTED_WORKSPACE:
        problems.append(f"unexpected workspace packages: {sorted(workspace_names)}")

    resolved_ids = {
        node["id"] for node in data.get("resolve", {}).get("nodes", [])
    }
    for package_id in sorted(resolved_ids):
        package = packages[package_id]
        source = package.get("source")
        manifest = Path(package["manifest_path"]).resolve()
        if source is None:
            try:
                manifest.relative_to(root.resolve())
            except ValueError:
                problems.append(f"external path dependency: {package['name']} at {manifest}")
        elif not source.startswith("registry+"):
            problems.append(f"non-registry dependency: {package['name']} from {source}")
    return problems


class Tests(unittest.TestCase):
    def fixture(self, external_source="registry+https://example.invalid/index"):
        packages = [
            {"id": name, "name": name, "manifest_path": f"/repo/{name}/Cargo.toml", "source": None}
            for name in sorted(EXPECTED_WORKSPACE)
        ]
        packages.append({
            "id": "external", "name": "external", "manifest_path": "/cache/external/Cargo.toml",
            "source": external_source,
        })
        return {
            "packages": packages,
            "workspace_members": sorted(EXPECTED_WORKSPACE),
            "resolve": {"nodes": [{"id": p["id"]} for p in packages]},
        }

    def test_registry_dependency_is_allowed(self):
        self.assertEqual(findings(self.fixture(), Path("/repo")), [])

    def test_git_dependency_is_rejected(self):
        result = findings(self.fixture("git+https://example.invalid/private"), Path("/repo"))
        self.assertEqual(len(result), 1)
        self.assertIn("non-registry dependency", result[0])

    def test_external_path_dependency_is_rejected(self):
        data = self.fixture()
        data["packages"][-1]["source"] = None
        result = findings(data, Path("/repo"))
        self.assertEqual(len(result), 1)
        self.assertIn("external path dependency", result[0])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    problems = findings(metadata(ROOT))
    for problem in problems:
        print(problem)
    if problems:
        print(f"dependency policy gate: FAILED ({len(problems)} findings)")
        return 1
    print("dependency policy gate: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
