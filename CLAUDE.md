# shirabe

A small, fast Rust API serving a subset of the MusicBrainz ws/2 web service
directly from a synced MusicBrainz Postgres mirror (the `musicbrainz` schema)
via `pg_trgm`. It replaces the slow official MusicBrainz Docker + SOLR stack for
consumer applications that only need a few ws/2 endpoints.

## Layout

- `src/main.rs` — binary entrypoint: parses `Cli`, dispatches `sync` / `migrate`
  or starts the axum server.
- `src/lib.rs` — `AppState`, `build_router` (mounts `/ws/2` + `/music`, the `/3`,
  `/v4`, `/v3` facades, the Cover Art proxy, health and the opt-in `/debug`).
- `src/config.rs` — clap `Cli` / `Config`; every env var, mirrored in
  `.env.example` (unit test enforces it).
- `src/db.rs` — Postgres pools.
- `src/error.rs` — `ApiError` and its JSON error contract.
- `src/query.rs` — Lucene-subset query parser (unit-tested).
- `src/date.rs` — release-date selection from MB date events (unit-tested).
- `src/models.rs` — serde response models (MB hyphenated-key JSON; the contract).
- `src/queries.rs` — the SQL catalog: every statement as a `pub const`.
- `src/repo.rs` — read-only sqlx runtime queries against the `musicbrainz` schema.
- `src/search.rs` — local-first search + ranking over the writable index DBs,
  session knobs (`statement_timeout`, `work_mem`).
- `src/handlers.rs` — the ws/2 routes (artist/release/recording/release-group
  search + lookups) and health.
- `src/facades/` — native-shape provider facades: `tmdb.rs` (`/3`), `tvdb.rs`
  (`/v4`), `fanart.rs` (`/v3`), `coverart.rs` (`/release`, `/release-group`, `/_ia`).
- `src/sources/` — the `Source` trait + `Registry`, one ingest source per
  provider (`musicbrainz`, `imdb` + `imdb_index`, `tmdb`, `tvdb`, `fanart`) and
  the `xref` cross-id store.
- `src/images.rs` — artwork URL rewriting through `caache` / `/_ia`.
- `src/migrate.rs` — `shirabe migrate <db>`: embedded, idempotent migrations for
  the writable DBs.
- `src/debug_ui.rs` — token-guarded SQL query explorer at `/debug/queries`.
- `migrations/` — `0001..` pg_trgm/index migrations layered on the MB mirror,
  plus one directory per writable DB (`shirabe`, `imdb`, `tmdb`, `tvdb`, `fanart`).
- `tests/` — integration tests over `tests/common/mod.rs` helpers. DB-free
  (`cargo test`): `routes.rs`, `facades.rs`, `coverart.rs`, most of `contract.rs`
  (wiremock upstreams). DB-gated `#[ignore]` (`make test-integration`, seeds
  `tests/fixtures/musicbrainz.sql` into throwaway DBs): `mb_api.rs`,
  `sql_catalog.rs`, the MB tier of `contract.rs`.
- `docs/shirabe-api-contract.md` — the consumer-facing API contract.

## Rules

- The JSON contract is defined by the consumer's MusicBrainz ws/2 parsing
  structs. Match the MusicBrainz hyphenated-key shapes exactly.
- Read-only DB: only `SELECT`. Never write to the mirror.
- Use sqlx **runtime** queries (`sqlx::query`), not compile-time macros — the
  build must not need a live DB.
- Keep clippy clean: `cargo clippy --all-targets -- -D warnings`.
- Format with nightly rustfmt (`cargo +nightly fmt`) — uses unstable options.

## Verify

`cargo build`, `cargo test`, `cargo +nightly fmt --check`, and
`cargo clippy --all-targets -- -D warnings` must all pass.
