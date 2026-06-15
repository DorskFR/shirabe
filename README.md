# shirabe

A small, fast, self-hosted Rust API that implements a subset of the
[MusicBrainz ws/2](https://musicbrainz.org/doc/MusicBrainz_API) web service. It
queries a **synced MusicBrainz Postgres mirror** directly via `pg_trgm` trigram
search, replacing the slow official MusicBrainz Docker stack + Apache SOLR.

`調べ` (shirabe) — "to look up / investigate".

## Why

A typical consumer only needs ~5 ws/2 endpoints (3 query shapes). The official
MB search server is heavy (SOLR + the full ws/2 app). shirabe answers those
exact requests straight from the replicated `musicbrainz` Postgres schema,
emitting MusicBrainz-compatible hyphenated-key JSON, so an existing client parser
and confidence re-scoring work unchanged.

## Endpoints

All responses are JSON with MB's hyphenated keys (`artist-credit`,
`track-count`, `release-group`, `primary-type`, `sort-name`, ...).

| Method & path | Query shape | Notes |
| --- | --- | --- |
| `GET /ws/2/artist?query=&limit=&inc=aliases` | bare artist name | `id, name, score, aliases[].{name,sort-name}` |
| `GET /ws/2/release?query=&limit=` | `release:(title) AND artist:(name) [AND date:(YYYY*)]` | `id, title, date, score, status, disambiguation, artist-credit, track-count, release-group` |
| `GET /ws/2/recording?query=&limit=&inc=releases+artist-credits+media` | `recording:"title" AND artist:"name"` | recordings + full release shapes (incl. media/tracks) |
| `GET /ws/2/release/{mbid}?inc=...media+recordings+...rels` | — | full album: media[] (ordered), tracks, release-group, relations[] |
| `GET /ws/2/recording/{mbid}?inc=releases+artist-credits+aliases` | — | recording + releases |
| `GET /health`, `GET /ws/2` | — | DB ping, `{"status":"ok"}` |

### Scoring

`score` (0-100) is synthesized from `pg_trgm` `similarity()` (0.0-1.0 scaled to
0-100), so a client's own confidence re-scoring keeps working. Results are
ordered by similarity descending and capped at `limit`.

### Query parser

The `query=` string is **not** parsed as full Lucene. A small hand-rolled parser
(`src/query.rs`) extracts the known fields (`release:`, `artist:`, `recording:`,
`date:`), handling `"..."` quotes, `(...)` groups, `AND`, escaped characters, and
the `date:(YYYY*)` year-prefix wildcard. A bare query (no `field:` markers) is
treated as the whole artist name.

### Dates

Release dates live in `release_country` / `release_unknown_country` as per-country
date events, not on the release row. shirabe picks the earliest event (preferring
worldwide `XW` on ties) and renders it as `"YYYY"`, `"YYYY-MM"`, `"YYYY-MM-DD"`,
or `""` (see `src/date.rs`).

## How it connects to the MB mirror

shirabe expects the standard MusicBrainz Postgres schema in a schema named
`musicbrainz` (the layout produced by
[musicbrainz-docker](https://github.com/metabrainz/musicbrainz-docker) /
the replication mirror). It opens a read-only connection pool to `DATABASE_URL`
and runs `SELECT`-only queries — use a read-only DB role.

Before first use, apply the index migration once (`migrations/0001_*.sql`):

```sh
DATABASE_URL=postgres://readonly@mirror.example.com/musicbrainz_db \
  sqlx migrate run --source migrations
# or: make db/migrate/up
```

It creates `pg_trgm` + `unaccent` extensions and GIN trigram indexes on the
searched name columns plus btree FK indexes on the join paths. It is idempotent
(`CREATE ... IF NOT EXISTS`) and additive — it never touches replicated data and
can be dropped without consequence.

## Environment variables

A ready-to-copy [`.env.example`](.env.example) lists every variable, its
default, and meaning. `DATABASE_URL` is the only **required** one; startup fails
fast with a clear error if it is unset.

| Var | Required | Default | Purpose |
| --- | --- | --- | --- |
| `DATABASE_URL` | **yes** | _(none)_ | Postgres DSN for the MB mirror (read-only role) |
| `SHIRABE_BIND` | no | `0.0.0.0:8800` | HTTP bind address:port (server listens on **8800**) |
| `SHIRABE_DB_POOL_SIZE` | no | `8` | Max Postgres connections |
| `SHIRABE_DEFAULT_LIMIT` | no | `25` | Default search `limit` |
| `SHIRABE_MAX_LIMIT` | no | `100` | Hard cap on requested `limit` |
| `SHIRABE_SIMILARITY_THRESHOLD` | no | `0.2` | Min `pg_trgm` similarity to keep a row |
| `RUST_LOG` | no | `info` | tracing/`EnvFilter` filter |

## Deployment

The container `EXPOSE`s and the server listens on **port 8800** (override with
`SHIRABE_BIND`) — this is the port a Kubernetes `Service`/`HTTPRoute` should
target. The image is built from `deploy/Dockerfile` (multi-stage,
`debian:bookworm-slim` runtime) and published to GitHub Container Registry via
`make image/release` (or the tag-driven `release` GitHub Actions workflow).

shirabe is fully configured through environment variables (see above). Supply
the `SHIRABE_*` tunables + `RUST_LOG` however your platform prefers (configMap,
`.env`, plain env) and inject `DATABASE_URL` — the read-only MB-mirror DSN —
from your secret store. shirabe reads it straight from the environment, no app
changes needed.

## Pointing a consumer at shirabe

shirabe serves the exact same paths (`/ws/2/...`) and JSON shapes a MusicBrainz
ws/2 client already parses. Point your client's base URL at the shirabe instance
(e.g. `http://shirabe:8800`) instead of `https://musicbrainz.org`. Since shirabe
talks to your own DB, the official 1 req/s courtesy rate limit is unnecessary —
no consumer code changes are needed beyond config.

## Development

```sh
make build      # cargo build --release
make test       # cargo test (unit tests, no DB needed)
make lint       # cargo clippy -D warnings
make fmt        # cargo +nightly fmt
make run        # cargo run (needs DATABASE_URL)
make image/build
```

`docker-compose.yaml` starts an **empty** local postgres for smoke-testing the
migration + server boot. Real data requires loading a MusicBrainz Postgres dump
(see musicbrainz-docker) or pointing `DATABASE_URL` at an existing mirror.

## License

WTFPL.
