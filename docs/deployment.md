# Deploying shirabe

## The MusicBrainz mirror

shirabe expects the standard MusicBrainz Postgres schema in a schema named
`musicbrainz`, the layout produced by
[musicbrainz-docker](https://github.com/metabrainz/musicbrainz-docker) and the
replication mirror. It opens a connection pool to `DATABASE_URL` and runs
`SELECT`-only queries, so give it a read-only role.

Before first use, apply the mirror migrations once (`migrations/000*.sql`) with
a role that can create extensions and indexes:

```sh
DATABASE_URL=postgres://admin@mirror.example.com/musicbrainz_db \
  sqlx migrate run --source migrations
# or: make db/migrate/up
```

They create the `pg_trgm` and `unaccent` extensions, GIN trigram and full-text
indexes on the searched name columns, and btree FK indexes on the join paths.
They are idempotent (`CREATE ... IF NOT EXISTS`) and additive: they never touch
replicated data and can be dropped without consequence.

## Writable per-provider databases

The provider facades and ingest sources use their own writable databases
(`shirabe`, `imdb`, `tmdb`, `tvdb`, `fanart`), each optional. Bootstrap them
with the embedded migrations:

```sh
shirabe migrate all      # every DB whose URL is configured
shirabe migrate tmdb     # or one: shirabe | imdb | tmdb | tvdb | fanart
```

The SQL ships inside the binary, so nothing beyond the image is needed. The
storage model is described in
[shirabe-api-contract.md](shirabe-api-contract.md#8-storage-model--one-postgres-database-per-provider).

## Ingest jobs

`shirabe sync <source>` runs one source's refresh and exits; schedule it as a
cron job. Source ids are listed by `GET /health/sources` (`musicbrainz`,
`imdb`, `imdb-fts`, `imdb-trgm`, `tmdb`, `tvdb`, `fanart`).

## Container

The image is built from `deploy/Dockerfile` (multi-stage, `debian:bookworm-slim`
runtime) and published to `ghcr.io/dorskfr/shirabe:<version>` by the
tag-driven `release` workflow, or by `make image/release`. Images are tagged
with the `Cargo.toml` version only; there is no `latest`.

The container exposes and listens on port 8800 (override with `SHIRABE_BIND`).
Point a Kubernetes `Service` or `HTTPRoute` at that port.

shirabe is configured only through environment variables (see
[configuration.md](configuration.md)). Supply the `SHIRABE_*` tunables and
`RUST_LOG` however your platform prefers (ConfigMap, `.env`, plain env) and
inject `DATABASE_URL` and the API keys from your secret store.

## Probes

- `GET /health` (also `GET /ws/2`) pings the mirror and returns
  `{"status":"ok"}`. Use it for readiness and liveness.
- `GET /health/sources` probes every backing store (each capped at 2 s, run
  concurrently) and reports per-source health and staleness. It is diagnostic
  only; do not wire it as a probe.

If the `/_ia` image proxy is used, mount persistent storage at
`SHIRABE_COVERART_CACHE_DIR` and size it to `SHIRABE_COVERART_CACHE_MAX_BYTES`.
