# Security policy

## Reporting a vulnerability

Please do not report suspected vulnerabilities in a public issue. Use GitHub's
private vulnerability reporting for `tickr-io/tickr-cli` so maintainers can
triage and coordinate a fix before disclosure. Include affected versions,
impact, reproduction steps, and any suggested mitigation. Do not access data or
systems you do not own while testing.

The maintainers will acknowledge a complete report, assess severity, and
coordinate remediation and disclosure. Security fixes are provided for the
latest tagged release; older releases may be asked to upgrade before receiving
a patch.

## Deployment boundary

The repository's Compose formation and checked-in credentials are strictly for
loopback-only local development. They are not a production security profile.
See [docs/production-hardening.md](docs/production-hardening.md).
