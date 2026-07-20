#!/usr/bin/env python3
"""Deterministic workspace policy and third-party notice generator."""
import argparse
import hashlib
import json
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "THIRD_PARTY_NOTICES.md"
NPM_ATTRIBUTION = ROOT / "third_party/npm-attribution.json"
LICENSE_NAMES = ("license", "copying", "notice", "copyright")
CANONICAL_LICENSES = {
    "Apache-2.0": ROOT / "LICENSE",
    **{
        name: ROOT / f"third_party/licenses/{name}.txt"
        for name in (
            "0BSD", "BSD-2-Clause", "BSD-3-Clause", "BSL-1.0",
            "BlueOak-1.0.0", "CC-BY-4.0", "CC0-1.0", "ISC",
            "LGPL-2.1-or-later", "MIT", "MIT-0", "Python-2.0",
        )
    },
}


def license_files(directory: Path):
    return sorted(
        p for p in directory.iterdir()
        if p.is_file() and p.name.lower().startswith(LICENSE_NAMES) and p.stat().st_size <= 500_000
    ) if directory.is_dir() else []


def manifest_policy_findings(root=ROOT):
    findings = []
    manifests = [root / "Cargo.toml", *sorted((root / "src").glob("*/Cargo.toml"))]
    for path in manifests:
        text = path.read_text()
        package = text.split("[package]", 1)[1].split("\n[", 1)[0]
        for key in ("authors", "license", "repository"):
            if f"{key}.workspace = true" not in package:
                findings.append(f"{path.relative_to(root)}: {key} must inherit workspace metadata")
        if "publish = false" not in package:
            findings.append(f"{path.relative_to(root)}: publish must be false")
    package = json.loads((root / "console/package.json").read_text())
    if package.get("private") is not True or package.get("license") != "Apache-2.0":
        findings.append("console/package.json: expected private Apache-2.0 package")
    if not (root / "LICENSE").is_file() or not (root / "NOTICE").is_file():
        findings.append("root LICENSE and NOTICE are required")
    return findings


def add_text(texts, content):
    content = "\n".join(
        line.rstrip() for line in content.replace("\r\n", "\n").split("\n")
    ).rstrip() + "\n"
    digest = hashlib.sha256(content.encode()).hexdigest()[:16]
    texts.setdefault(digest, content)
    return digest


def metadata_fallback_refs(texts, name, version, license_id, source, authors):
    """Attach canonical terms and an explicit locked-metadata record."""
    refs = []
    for spdx_id, path in CANONICAL_LICENSES.items():
        if spdx_id in license_id:
            refs.append(add_text(texts, path.read_text(encoding="utf-8")))
    if not refs:
        raise RuntimeError(
            f"{name} {version}: no shipped license text and unsupported expression {license_id!r}"
        )
    if isinstance(authors, list):
        owner = ", ".join(
            item.get("name") or json.dumps(item, sort_keys=True)
            if isinstance(item, dict) else str(item)
            for item in authors
        ) or "not declared"
    else:
        owner = str(authors or "not declared")
    record = (
        "Installed-package attribution record\n"
        f"Component: {name}\nVersion: {version}\nDeclared license: {license_id}\n"
        f"Upstream source: {source}\nDeclared author/owner metadata: {owner}\n"
        "The locked registry metadata contains no standalone license/notice text. "
        "Canonical SPDX terms named by its license declaration are included "
        "with this record; the source and owner metadata are retained here "
        "for per-component review.\n"
    )
    refs.append(add_text(texts, record))
    return refs


def cargo_components(texts):
    metadata = json.loads(subprocess.run(
        ["cargo", "metadata", "--locked", "--format-version", "1"],
        cwd=ROOT, check=True, capture_output=True, text=True,
    ).stdout)
    workspace = set(metadata["workspace_members"])
    components = []
    for package in metadata["packages"]:
        if package["id"] in workspace:
            continue
        directory = Path(package["manifest_path"]).parent
        source = package.get("repository") or package.get("source") or "workspace"
        license_id = package.get("license") or "UNKNOWN"
        refs = [add_text(texts, path.read_text(encoding="utf-8", errors="replace")) for path in license_files(directory)]
        if not refs:
            refs = metadata_fallback_refs(
                texts, package["name"], package["version"], license_id, source,
                package.get("authors") or [],
            )
        components.append((package["name"], package["version"], source, license_id, refs))
    return components


def npm_name(path):
    parts = path.split("node_modules/")[-1].split("/")
    return "/".join(parts[:2]) if parts[0].startswith("@") else parts[0]


def npm_components(texts):
    lock = json.loads((ROOT / "console/package-lock.json").read_text())["packages"]
    cache = json.loads(NPM_ATTRIBUTION.read_text(encoding="utf-8"))
    if cache.get("schema") != 1:
        raise RuntimeError("unsupported npm attribution cache schema")
    cached_packages = cache.get("packages", {})
    cached_texts = cache.get("texts", {})
    expected_paths = {p for p in lock if p and "node_modules/" in p}
    if set(cached_packages) != expected_paths:
        raise RuntimeError(
            "npm attribution cache does not cover package-lock.json; "
            "run: just refresh-npm-attribution"
        )

    components = []
    for path in sorted(expected_paths):
        package = lock[path]
        cached = cached_packages[path]
        name = npm_name(path)
        version = package.get("version", "UNKNOWN")
        license_id = package.get("license", "UNKNOWN")
        source = package.get("resolved") or f"https://registry.npmjs.org/{name}"
        pinned = (name, version, license_id, source, package.get("integrity"))
        recorded = tuple(cached.get(key) for key in (
            "name", "version", "license", "resolved", "integrity"
        ))
        if recorded != pinned:
            raise RuntimeError(
                f"{path}: npm attribution cache is stale; "
                "run: just refresh-npm-attribution"
            )

        refs = []
        for digest in cached.get("refs", []):
            content = cached_texts.get(digest)
            if content is None or add_text(texts, content) != digest:
                raise RuntimeError(f"{path}: invalid cached license-text digest {digest}")
            refs.append(digest)
        if not refs:
            refs = metadata_fallback_refs(
                texts, name, version, license_id, source, cached.get("authors", "")
            )
        components.append((name, version, source, license_id, refs))
    return components


def render():
    texts = {}
    components = cargo_components(texts) + npm_components(texts)
    font = (ROOT / "console/public/fonts/LICENSE.txt").read_text(encoding="utf-8")
    font_ref = add_text(texts, font)
    components.append(("IBM Plex fonts", "bundled", "https://github.com/IBM/plex", "OFL-1.1", [font_ref]))
    lines = [
        "# Third-Party Notices", "", "This file is generated by `scripts/license_gate.py` from locked dependency metadata.",
        "It is platform-independent and includes development and optional dependencies.", "", "## Components", "",
        "| Component | Version | License | Source | License texts |", "|---|---:|---|---|---|",
    ]
    for name, version, source, license_id, refs in sorted(components, key=lambda item: (item[0].lower(), item[1], item[2])):
        links = ", ".join(f"[{ref}](#{ref})" for ref in refs)
        safe_name = name.replace("|", "&#124;")
        safe_license = license_id.replace("|", "&#124;")
        safe_source = source.replace("|", "%7C")
        lines.append(f"| {safe_name} | {version} | {safe_license} | {safe_source} | {links} |")
    lines.extend(["", "## License and notice texts", ""])
    for digest, content in sorted(texts.items()):
        lines.extend([f"<a id=\"{digest}\"></a>", f"### {digest}", "", "```text", content.rstrip(), "```", ""])
    return "\n".join(lines).rstrip() + "\n"


class Tests(unittest.TestCase):
    def test_text_ids_are_stable_and_normalize_whitespace(self):
        one, two = {}, {}
        self.assertEqual(add_text(one, "a  \r\nb\t\n"), add_text(two, "a\nb"))
        self.assertEqual(one, two)

    def test_manifest_policy_passes_fixture_shape(self):
        self.assertEqual(manifest_policy_findings(), [])

    def test_missing_package_text_gets_canonical_terms_and_attribution_record(self):
        texts = {}
        refs = metadata_fallback_refs(
            texts, "fixture", "1.0.0", "MIT OR Apache-2.0",
            "https://example.invalid/fixture", ["Fixture Author"],
        )
        self.assertEqual(len(refs), 3)
        rendered = "\n".join(texts[ref] for ref in refs)
        self.assertIn("MIT License", rendered)
        self.assertIn("Apache License", rendered)
        self.assertIn("Fixture Author", rendered)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return 0 if unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests)).wasSuccessful() else 1
    findings = manifest_policy_findings()
    if findings:
        print("\n".join(findings))
        return 1
    generated = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text(encoding="utf-8") != generated:
            print("THIRD_PARTY_NOTICES.md is stale; run: just licenses")
            return 1
        print("license policy and attribution: ok")
    else:
        OUTPUT.write_text(generated, encoding="utf-8")
        print(f"wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
