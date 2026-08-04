---
title: Release support
description: How Tickr documentation tracks supported and retired compatibility lines.
sidebar_position: 5
---

# Release support

Documentation is versioned by supported compatibility line, not by every patch tag.

## Current line

| Release line | Status | Documentation |
| --- | --- | --- |
| `0.1` | Supported | Current stable |

Patch releases within a line update that line's documentation. A new line is created when the public CLI, Core DSL, API, protocol, or formation contract requires separate instructions.

## Select the correct version

Match documentation to the release line reported by:

```bash
tickr --version
```

Keep these assets compatible:

- `tickr` and `tickr-ctx` executables;
- Core DSL;
- examples;
- migrations;
- Console assets;
- API and protobuf contracts;
- documentation.

## Retired lines

When a release line leaves support:

- its existing URLs remain available;
- every page displays an unsupported-version warning;
- it is removed from primary navigation and ordinary search results;
- search engines are instructed not to index it;
- it never redirects silently to current instructions.

Historical documentation explains historical behavior; it does not restore product support. Upgrade to a supported line before using current operational guidance.
