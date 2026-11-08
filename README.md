# buntrail-engine

Authoritative document and workspace orchestration.

## Features

Revisioned documents, background analysis, local/remote service routing and recovery.

## Usage

Add to your Cargo.toml after the initial registry release:

```toml
[dependencies]
buntrail-engine = "0.2.0"
```

```rust
let engine = buntrail_engine::Engine::new();
drop(engine);
```

Rust 1.99.1 (edition 2024). Unix integration checks need Git, Python 3, zsh, and ripgrep 15.1.0. Install rg beside your executable for bundled discovery, or use the explicit search constructor. SSH, Docker and Podman are required only for their respective routes. SSH askpass and remote bootstrap require matching helper resources supplied by the embedding application. Install buntrail-helper 0.1.0 or set BUNTRAIL_HELPER_BINARY to its absolute path before checks.

```sh
cargo fetch
scripts/check.sh
```

## Technologies and support

Rust, Cargo. Typed filesystem/remote/terminal implementations are primarily Unix-only. Windows currently uses command services and ConPTY; Unix supervisors are unavailable there. Native Windows acceptance and full interactive remote authentication/bootstrap acceptance remain unverified. This is a development release, not a promise of desktop parity.

API documentation is built with `cargo doc --no-deps`. Public dependency types
remain in their originating Buntrail crates; depend on those crates directly
when constructing those types. No API stability beyond the declared 0.x version
is promised. See [release instructions](docs/releasing.md).

## Collaborators and contributing

Maintained by Bunga Mungil for the [Buntrail organization](https://github.com/buntrail).
Contributions are welcome through issues and small pull requests in
https://github.com/buntrail/buntrail-engine. Read [CONTRIBUTING.md](CONTRIBUTING.md)
and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Contact: bungamungil@icloud.com.
Report vulnerabilities privately using [SECURITY.md](SECURITY.md).

## License

First-party code is [MIT licensed](LICENSE), Copyright (c) 2026 Bunga Mungil.
Third-party dependencies retain their own licenses. See
[third-party notices](THIRD_PARTY_NOTICES.md).

Terminal workspace layouts and launch descriptors are session-only. Workspace catalogs retain documents and verified ownership, but do not restore terminals after an application restart. Remote disconnect retention is independent of catalog persistence.
