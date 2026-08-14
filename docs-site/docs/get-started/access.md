---
title: Obtain operator access
description: Values and host tools required before connecting Tickr Lite.
sidebar_position: 1
---

# Obtain operator access

A Tickr operator must give you the connection identity for the tenant you are joining. Keep values from different tenants and release installations separate.

## Values from your operator

| Value | Environment variable | Purpose |
| --- | --- | --- |
| Tenant slug | `TICKR_TENANT_SLUG` | Selects the tenant identity carried by this Data plane. |
| Control-plane HTTP subquery channel URL | `TICKR_CTRL_HTTP_URL` | Supplies live queries and Control-plane health reads. |
| Control-plane Conductor relay URL | `TICKR_CTRL_RELAY_URL` | Carries the bidirectional Conductor relay. |

Ask the operator which Tickr release line is supported. Use a Tickr Lite archive and documentation version from that line.

:::warning Do not mix installations
Do not reuse one environment file across tenants or release lines. A state directory belongs to one admitted formation identity.
:::

## Host requirements

Tickr Lite release archives support:

- Linux x86-64;
- Linux ARM64;
- Apple silicon.

Install these host tools:

- Nix with the `nix-command` and `flakes` features enabled;
- `curl`;
- `jq`.

Nix builds and runs workflow Tasks. Nickel evaluates workflow and runtime-Patch source before Tickr accepts it.

## Network expectations

By default, Tickr Lite binds the local API and embedded Console to `127.0.0.1`. Your machine must be able to reach both operator-supplied Control-plane endpoints. The Control plane does not become part of the local formation.

Continue to [Install Tickr Lite](./install-lite.md).
