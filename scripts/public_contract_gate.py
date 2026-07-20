#!/usr/bin/env python3
"""Validate the committed public protobuf contract and relay wire identity."""
from __future__ import annotations

import argparse
import re
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROTO_IGNORED = re.compile(
    r'''//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*' ''',
    re.S | re.X,
)
PROTO_TOKEN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|[{};.]")
EXPECTED_ROOT_DECLARATIONS = {
    ("message", "ConductorRelayMessage"),
    ("enum", "EntityType"),
    ("service", "ConductorRelayService"),
}
EXPECTED_PROTO_PACKAGES = {
    "archive-union.proto": "tickr.archive",
    "conductor-relay.proto": "tickr",
    "instance-snapshot.proto": "tickr.instance",
    "patch.proto": "tickr.patch",
    "runnable-projection.proto": "tickr.runnable",
    "signal.proto": "tickr.signal",
    "task-coordination.proto": "tickr.task",
    "tickr-api.proto": "tickr.api",
    "workflow-definition.proto": "tickr.workflow",
}


def proto_syntax(text: str) -> str:
    return PROTO_IGNORED.sub(
        lambda match: re.sub(r"[^\n]", " ", match.group(0)), text
    )


def proto_package(text: str) -> str | None:
    match = re.search(
        r"\bpackage\s+([A-Za-z_][A-Za-z0-9_.]*)\s*;", proto_syntax(text)
    )
    return match.group(1) if match else None


def top_level_declarations(text: str) -> list[tuple[str, str]]:
    tokens = list(PROTO_TOKEN.finditer(proto_syntax(text)))
    declarations: list[tuple[str, str]] = []
    depth = 0
    for index, token_match in enumerate(tokens):
        token = token_match.group(0)
        if token == "{":
            depth += 1
        elif token == "}":
            depth = max(0, depth - 1)
        elif depth == 0 and token in {"message", "enum", "service"}:
            if index + 1 < len(tokens):
                declarations.append((token, tokens[index + 1].group(0)))
    return declarations


def findings(root: Path = ROOT) -> list[str]:
    proto_root = root / "proto"
    relay = proto_root / "conductor-relay.proto"
    if not relay.is_file():
        return ["proto/conductor-relay.proto is required"]

    root_declarations: list[tuple[str, str]] = []
    root_files: set[str] = set()
    actual_packages = {}
    services = []
    for path in sorted(proto_root.rglob("*.proto")):
        text = path.read_text(encoding="utf-8")
        relative = path.relative_to(proto_root).as_posix()
        actual_packages[relative] = proto_package(text)
        declarations = top_level_declarations(text)
        services.extend((relative, name) for kind, name in declarations if kind == "service")
        if proto_package(text) == "tickr":
            root_files.add(path.relative_to(root).as_posix())
            root_declarations.extend(declarations)

    result: list[str] = []
    if actual_packages != EXPECTED_PROTO_PACKAGES:
        result.append(f"unexpected protobuf package/file set: {actual_packages}")
    if services != [("conductor-relay.proto", "ConductorRelayService")]:
        result.append(f"unexpected protobuf services: {services}")
    if root_files != {"proto/conductor-relay.proto"}:
        result.append(f"package tickr must exist only in conductor-relay.proto: {sorted(root_files)}")
    if set(root_declarations) != EXPECTED_ROOT_DECLARATIONS or len(root_declarations) != 3:
        result.append(f"unexpected package tickr declarations: {root_declarations}")

    relay_syntax = proto_syntax(relay.read_text(encoding="utf-8"))
    rpc = re.compile(
        r"rpc\s+StreamConductorRelay\s*\(\s*stream\s+ConductorRelayMessage\s*\)"
        r"\s*returns\s*\(\s*stream\s+ConductorRelayMessage\s*\)\s*;",
        re.S,
    )
    if not rpc.search(relay_syntax):
        result.append("ConductorRelayService must retain its bidirectional streaming RPC")
    reserved_numbers = {
        int(number)
        for clause in re.findall(r"\breserved\s+([^;]+);", relay_syntax)
        for number in re.findall(r"\b\d+\b", clause)
    }
    if not {4, 5, 6}.issubset(reserved_numbers):
        result.append("EntityType must reserve numeric values 4, 5, and 6")
    return result


class Tests(unittest.TestCase):
    def test_comments_and_nested_declarations_do_not_change_root_contract(self):
        text = '''
            syntax = "proto3";
            package tickr;
            // message Hidden {}
            message ConductorRelayMessage { message Nested {} }
            enum EntityType { UNKNOWN = 0; reserved 4, 5, 6; }
            service ConductorRelayService {
              rpc StreamConductorRelay(stream ConductorRelayMessage)
                returns (stream ConductorRelayMessage);
            }
        '''
        self.assertEqual(set(top_level_declarations(text)), EXPECTED_ROOT_DECLARATIONS)

    def test_unexpected_root_declaration_is_detected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "proto").mkdir()
            (root / "proto/conductor-relay.proto").write_text(
                "package tickr; message Extra {}", encoding="utf-8"
            )
            self.assertTrue(findings(root))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    problems = findings()
    for problem in problems:
        print(problem)
    if problems:
        print(f"public contract gate: FAILED ({len(problems)} findings)")
        return 1
    print("public contract gate: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
