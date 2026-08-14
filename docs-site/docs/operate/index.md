---
title: Operate Tickr
description: Select, admit, observe, protect, and recover a complete Data-plane formation.
sidebar_position: 4
---

# Operate Tickr

A formation is a complete supported Data-plane profile. Select one profile and satisfy its full topology; do not assemble a mixed substrate from individual environment variables.

## Start with Tickr Lite

`lite-local` is the primary public path. It gives one operator a complete local Data plane with SQLite and local files, while retaining the same HTTP Command semantics used by distributed formations.

Use a distributed formation when you require a distributed Executor fleet, external Event ingress, Postgres, and object-store-backed final Logs.

## Operator responsibilities

- Supply tenant and Control-plane connection values.
- Keep binary, Core DSL, migrations, and documentation release-compatible.
- Protect local or distributed storage.
- Admit only the complete named profile.
- Expose the API through an authenticated private/TLS boundary.
- Monitor readiness and each health component.
- Back up and restore every durable substrate coherently.
- Stop on capability or identity disagreement instead of forcing startup.

## Guides

- [Formation profiles](./formations.md)
- [Configuration](./configuration.md)
- [Storage and durability](./storage-and-durability.md)
- [Production security](./security.md)
- [Troubleshooting](../troubleshooting.md)
