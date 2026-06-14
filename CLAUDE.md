# shirabe

A small, fast Rust API serving a subset of the MusicBrainz ws/2 web service
directly from a synced MusicBrainz Postgres mirror (the `musicbrainz` schema)
via `pg_trgm`. It replaces the slow official MusicBrainz Docker + SOLR stack for
the consumer project [kusaritoi](https://github.com/DorskFR/kusaritoi).

## Layout

- `src/main.rs` — axum server + router.
- `src/config.rs` — env-var config (clap).
- `src/db.rs` — Postgres pool.
- `src/query.rs` — the Lucene-subset query parser (unit-tested).
- `src/date.rs` — release-date selection from MB date events (unit-tested).
- `src/models.rs` — serde response models (MB hyphenated-key JSON; the contract).
- `src/repo.rs` — read-only sqlx runtime queries against the `musicbrainz` schema.
- `src/handlers.rs` — the 5 endpoints + health.
- `migrations/` — idempotent pg_trgm + index migration layered on the mirror.

## Rules

- The JSON contract is defined by kusaritoi's parsing structs in
  `kusaritoi/src/search/providers/musicbrainz.rs`. Match those shapes exactly.
- Read-only DB: only `SELECT`. Never write to the mirror.
- Use sqlx **runtime** queries (`sqlx::query`), not compile-time macros — the
  build must not need a live DB.
- Keep clippy clean: `cargo clippy --all-targets -- -D warnings`.
- Format with nightly rustfmt (`cargo +nightly fmt`) — uses unstable options.

## Verify

`cargo build`, `cargo test`, `cargo +nightly fmt --check`, and
`cargo clippy --all-targets -- -D warnings` must all pass.
