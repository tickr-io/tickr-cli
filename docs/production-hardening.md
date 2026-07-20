# Production security hardening

The repository-local Compose file is a development convenience, not a
production deployment manifest. Its credentials are deliberately trivial and
its services are safe only because published ports bind to loopback.

Before production deployment:

- place the API, PostgreSQL, NATS, object storage, and any control-plane relay on
  private networks behind authenticated ingress;
- require authentication, authorization, and tenant isolation at every public
  API boundary;
- terminate TLS at every network boundary and enable authenticated, authorized
  TLS for NATS and datastores;
- inject unique, rotated secrets from a secret manager; never reuse `.envrc` or
  Compose development values;
- configure `TICKR_API_BIND_ADDR` and all `TICKR_LOG_STORAGE_*` values explicitly;
- restrict CORS to exact trusted origins if cross-origin browser access is
  required; the default same-origin Console proxy needs no wildcard CORS;
- use immutable, vulnerability-scanned images and locked dependencies;
- apply least-privilege service accounts, filesystem/container restrictions,
  resource limits, backups, restore tests, and audit logging;
- ensure server-side diagnostics redact credentials, storage keys, subjects,
  URLs, and tenant data; expose stable public 5xx messages only;
- run dependency, secret, image, source-policy, and reachable-history scans on
  every release.

`just security-static` is non-destructive and validates repository policy.
`just security` additionally runs RustSec, cargo-deny, gitleaks, and npm audit;
it requires those tools to be installed. It is not a substitute for deployment
threat modeling or a Git-history scan.

## Reviewed dependency exceptions

The full audit is fail-closed except for these ID-specific, time-bounded cases.
Resolved-graph exceptions are recorded in `deny.toml`; Cargo.lock-only RSA is
recorded in the `just security` command. Reassess all by 2026-10-20:

- `RUSTSEC-2026-0194` and `RUSTSEC-2026-0195`: OpenDAL 0.57 currently pins
  `quick-xml` 0.39. Object-storage endpoints are operator-controlled and private;
  do not allow tenants to configure an arbitrary endpoint.
- `RUSTSEC-2025-0111`: `tokio-tar` is reachable only through the development/test
  `testcontainers` dependency and is absent from production artifacts.
- `RUSTSEC-2023-0071`: Cargo.lock contains RSA only through SQLx's inactive,
  optional MySQL backend. The workspace enables PostgreSQL only; cargo-deny's
  resolved graph confirms RSA is not active.
- Unmaintained-crate notices for bincode, paste, and rustls-pemfile are separately
  tracked in `deny.toml`; none currently carries a fixed vulnerability. Bincode
  replacement requires an explicit persisted-wire migration.

A 2026-07-20 Trivy scan of the immutable development images found fixable
high/critical findings in their upstream OS/Go layers, including the final
archived MinIO/MC images. These images are therefore restricted to the
loopback-only local formation and must never be promoted to production.
Production deployments must select currently maintained, freshly scanned
PostgreSQL, NATS, and S3-compatible services. The development pins should be
refreshed whenever upstream publishes a cleaner immutable image.
