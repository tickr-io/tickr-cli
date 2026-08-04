# Contributing

Thanks for contributing to Tickr CLI.

## Development setup

Install Docker Compose, Just, Overmind, Rust 1.92, `protoc`, Node.js 24, npm,
Nix, and the Nickel CLI. Then run:

```sh
just install-hooks
just console-install
just docs-install
just up-bg
just verify
```

The Console is available at <http://127.0.0.1:3000> and the API at
<http://127.0.0.1:6000>.

## Before opening a pull request

```sh
just check
just test
just security
just console-test
just console-build
just docs-typecheck
just docs-build
```

Keep generated OpenAPI and TypeScript artifacts current with
`npm run generate-contract --prefix console`. After changing either npm
lockfile, run `just refresh-npm-attribution` and review the updated license
evidence and notices. Do not commit `.env.local`, local runtime state, logs,
credentials, or generated build output.

Changes to protobuf contracts must preserve existing field numbers, enum values,
reservations, service names, and streaming shapes unless the change is an
explicitly reviewed breaking release.

## Documentation

The release-versioned static site lives in `docs-site/`. Run `just docs-start`
for local authoring. Public guides must describe supported product behavior,
use canonical Tickr vocabulary, and identify formation-specific constraints.
Internal runtime proofs remain in `docs/contracts/` and are not published as
user guidance.

## Reporting security issues

Do not open a public issue for a suspected vulnerability. Follow
[SECURITY.md](SECURITY.md).
