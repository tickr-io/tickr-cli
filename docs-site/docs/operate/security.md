---
title: Production security
description: Protect the API, runtime substrates, credentials, and Task boundary.
sidebar_position: 4
---

# Production security

The repository-local Compose formation uses loopback bindings and development credentials. It is a development convenience, not a production deployment manifest.

## Required boundaries

Before production deployment:

- place API, SQL, Coordination, object storage, and Control-plane relay traffic on private networks;
- require authentication, authorization, and tenant isolation at every public API boundary;
- terminate TLS at every network boundary;
- use authenticated and authorized TLS for NATS, Redis, databases, and object stores;
- inject unique rotated secrets from a secret manager;
- restrict CORS to exact trusted origins when cross-origin access is required;
- use immutable, vulnerability-scanned images and locked dependencies;
- apply least-privilege service accounts, filesystem restrictions, resource limits, audit logging, and tested restores;
- redact credentials, storage keys, endpoints, subjects, URLs, and tenant data from diagnostics.

## Local defaults

Tickr Lite binds the API and Console to `127.0.0.1` by default. Keep the state directory private to the process user. The environment file contains tenant and endpoint configuration and must use mode `0600`.

Do not expose a loopback-oriented installation by changing only `TICKR_API_BIND_ADDR`. Network exposure requires the complete authentication, TLS, origin, and authorization boundary.

## Control-plane connection boundary

Data-plane API and Conductor processes authenticate every protected
Control-plane HTTP or relay connection with
`TICKR_CONTROL_PLANE_BEARER_TOKEN`. Remote endpoints require `https://` and
standard certificate-chain and hostname verification. The only plaintext
exception is an explicitly enabled development loopback endpoint; it does not
disable bearer authentication.

Deployment owns the public TLS endpoint, certificates, private networking, and
secret delivery. The Frontend keeps its HTTP and gRPC listeners private
plaintext, receives only decrypted application traffic plus the bearer
credential, and receives no certificate. Deployment must restrict the token and
the `TICKR_CTRL_CREDENTIALS_FILE` authority file to the relevant service
identities. Application code validates the exact token grammar, strict authority
schema, credential lifecycle, and Tenant binding; authority changes take effect
on a controlled Frontend restart.

## Task isolation

Task processes receive scoped Tickr context rather than direct access to SQLite or Redis. Preserve that boundary: workflow Tasks should not receive formation-level database, broker, object-store, or Coordination-role credentials.

## all-Redis

Redis connection descriptors must keep endpoints, credentials, and trust roots separate. Runtime Task grants do not expose Redis endpoints, credentials, commands, keys, or certificate material.

## Vulnerabilities

Report suspected vulnerabilities privately according to the repository [security policy](https://github.com/tickr-io/tickr-cli/blob/main/SECURITY.md). Do not open a public issue with exploit details or credentials.
