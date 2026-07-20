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
LOCK = json.loads((ROOT / "console/package-lock.json").read_text())["packages"]
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


def main():
    texts = {}
    packages = {}
    entries = [
        (path, metadata) for path, metadata in sorted(LOCK.items())
        if path and "node_modules/" in path
    ]
    for index, (path, metadata) in enumerate(entries, 1):
        package, refs = fetch_package(metadata, texts)
        packages[path] = {
            "name": package_name(path),
            "version": metadata.get("version", "UNKNOWN"),
            "license": metadata.get("license", "UNKNOWN"),
            "resolved": metadata["resolved"],
            "integrity": metadata["integrity"],
            "authors": author_string(package),
            "refs": refs,
        }
        print(f"[{index}/{len(entries)}] {path}")
    payload = {"schema": 1, "texts": dict(sorted(texts.items())), "packages": packages}
    OUTPUT.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n")
    print(f"wrote {OUTPUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
