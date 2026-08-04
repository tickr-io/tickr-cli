#!/usr/bin/env python3
"""Refresh the checked npm license/notice cache from the locked package tarballs."""
import base64
import hashlib
import io
import json
import tarfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCKFILES = (
    ROOT / "console/package-lock.json",
    ROOT / "docs-site/package-lock.json",
)
OUTPUT = ROOT / "third_party/npm-attribution.json"
LICENSE_NAMES = ("license", "copying", "notice", "copyright")


def normalize(content: str) -> str:
    return "\n".join(
        line.rstrip() for line in content.replace("\r\n", "\n").split("\n")
    ).rstrip() + "\n"


def add_text(texts, content):
    content = normalize(content)
    digest = hashlib.sha256(content.encode()).hexdigest()[:16]
    texts.setdefault(digest, content)
    return digest


def package_name(path):
    parts = path.split("node_modules/")[-1].split("/")
    return "/".join(parts[:2]) if parts[0].startswith("@") else parts[0]


def author_string(package):
    values = []
    author = package.get("author")
    if isinstance(author, str):
        values.append(author)
    elif isinstance(author, dict) and author.get("name"):
        value = author["name"]
        if author.get("email"):
            value += f" <{author['email']}>"
        values.append(value)
    for contributor in package.get("contributors") or []:
        if isinstance(contributor, str):
            values.append(contributor)
        elif isinstance(contributor, dict) and contributor.get("name"):
            values.append(contributor["name"])
    return ", ".join(dict.fromkeys(values))

def license_string(package, metadata):
    declaration = (
        metadata.get("license")
        or package.get("license")
        or package.get("licenses")
        or "UNKNOWN"
    )
    if isinstance(declaration, str):
        return declaration
    if isinstance(declaration, dict):
        return declaration.get("type") or declaration.get("name") or "UNKNOWN"
    if isinstance(declaration, list):
        identifiers = [
            item.get("type") or item.get("name") if isinstance(item, dict) else str(item)
            for item in declaration
        ]
        identifiers = list(dict.fromkeys(item for item in identifiers if item))
        return " OR ".join(identifiers) or "UNKNOWN"
    return "UNKNOWN"


def fetch_package(metadata, texts):
    url = metadata["resolved"]
    with urllib.request.urlopen(url, timeout=120) as response:
        blob = response.read()
    algorithm, expected = metadata["integrity"].split("-", 1)
    actual = base64.b64encode(hashlib.new(algorithm, blob).digest()).decode()
    if actual != expected:
        raise RuntimeError(f"integrity mismatch for {url}")

    package_json = {}
    refs = []
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as archive:
        for member in archive.getmembers():
            relative = member.name.removeprefix("package/")
            if "/" in relative or not member.isfile():
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                continue
            if relative == "package.json":
                package_json = json.loads(extracted.read().decode(errors="replace"))
            elif relative.lower().startswith(LICENSE_NAMES) and member.size <= 500_000:
                refs.append(add_text(texts, extracted.read().decode(errors="replace")))
    return package_json, sorted(set(refs))


def locked_packages():
    for lockfile in LOCKFILES:
        scope = lockfile.parent.name
        packages = json.loads(lockfile.read_text())["packages"]
        for path, metadata in packages.items():
            if path and "node_modules/" in path:
                yield f"{scope}/{path}", metadata




def main():
    texts = {}
    packages = {}
    fetched = {}
    entries = sorted(locked_packages())
    for index, (path, metadata) in enumerate(entries, 1):
        cache_key = (metadata["resolved"], metadata["integrity"])
        if cache_key not in fetched:
            fetched[cache_key] = fetch_package(metadata, texts)
        package, refs = fetched[cache_key]
        packages[path] = {
            "name": package_name(path),
            "version": metadata.get("version", "UNKNOWN"),
            "license": license_string(package, metadata),
            "resolved": metadata["resolved"],
            "integrity": metadata["integrity"],
            "authors": author_string(package),
            "refs": refs,
        }
        print(f"[{index}/{len(entries)}] {path}")
    payload = {"schema": 2, "texts": dict(sorted(texts.items())), "packages": packages}
    OUTPUT.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n")
    print(f"wrote {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
