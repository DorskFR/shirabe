# shirabe

A small, fast, self-hosted Rust API that implements a subset of the
[MusicBrainz ws/2](https://musicbrainz.org/doc/MusicBrainz_API) web service. It
queries a **synced MusicBrainz Postgres mirror** directly via `pg_trgm` trigram
search, replacing the slow official MusicBrainz Docker stack + Apache SOLR.

`調べ` (shirabe) — "to look up / investigate".

## Why

A typical consumer only needs a handful of ws/2 endpoints. The official
MB search server is heavy (SOLR + the full ws/2 app). shirabe answers those
exact requests straight from the replicated `musicbrainz` Postgres schema,
emitting MusicBrainz-compatible hyphenated-key JSON, so an existing client parser
and confidence re-scoring work unchanged.

## Endpoints

All responses are JSON with MB's hyphenated keys (`artist-credit`,
`track-count`, `release-group`, `primary-type`, `sort-name`, ...). Search
responses carry the ws/2 envelope: `{"count": <total>, "offset": <offset>,
"<plural>": [...]}`; `limit=` and `offset=` page through it.

| Method & path | Query shape | Notes |
| --- | --- | --- |
| `GET /ws/2/artist?query=&limit=&offset=&inc=aliases` | bare artist name | `id, name, score, aliases[].{name,sort-name}` |
| `GET /ws/2/artist/{mbid}?inc=url-rels+genres+tags+annotation` | — | artist lookup; `inc` tokens `url-rels`, `genres`, `tags`, `annotation` |
| `GET /ws/2/release?query=&limit=&offset=` | `release:(title) AND artist:(name) [AND date:(YYYY*)]` — or `arid:<mbid>` (+ optional `primarytype:`, `status:`) for an artist browse | `id, title, date, score, status, disambiguation, artist-credit, track-count, release-group` |
| `GET /ws/2/recording?query=&limit=&offset=&inc=releases+artist-credits+media` | `recording:"title" AND artist:"name"` | recordings + full release shapes (incl. media/tracks) |
| `GET /ws/2/release/{mbid}?inc=...media+recordings+...rels` | — | full album: media[] (ordered), tracks, release-group, relations[] |
| `GET /ws/2/recording/{mbid}?inc=releases+artist-credits+aliases` | — | recording + releases |
| `GET /ws/2/release-group?artist=<mbid>&limit=&offset=` | browse by artist MBID | `{"release-group-count", "release-group-offset", "release-groups": [...]}` |
| `GET /ws/2/release-group/{mbid}` | — | release-group lookup |
| `GET /health`, `GET /ws/2` | — | DB ping, `{"status":"ok"}` |
| `GET /health/sources` | — | per-source health/staleness report |

Unknown paths and wrong methods get JSON errors from every mount:
`404 {"error":"shirabe: no such route: GET /x"}` /
`405 {"error":"shirabe: method not allowed: POST /health"}`.

### Provider prefixes and aliases

Beyond ws/2, shirabe fronts several providers under their native API prefixes.
Each native prefix is also served under a self-describing provider alias (the same
handlers); embedded paths in responses stay canonical (native).

| Provider | Native prefix | Aliases |
| --- | --- | --- |
| MusicBrainz | `/ws/2/*` | `/musicbrainz/ws/2/*`, `/music/ws/2/*`, `/music/*` (version segment stripped) |
| TMDB | `/3/*` | `/tmdb/3/*` |
| TheTVDB | `/v4/*` | `/tvdb/v4/*` |
| fanart.tv | `/v3/*` | `/fanart/v3/*` |
| Cover Art Archive | `/release/*`, `/release-group/*`, `/_ia/*` | `/coverart/release/*`, `/coverart/release-group/*`, `/coverart/_ia/*` |

`/music` accepts both the stripped form (`GET /music/artist`) and the full tree
(`GET /music/ws/2/artist`), so a ws/2 client can point its base URL at either
`/music` or `/music/ws/2`. The former `/tv`, `/movie`, `/movies` category roots
and the `/cover` namespace are retired and now 404.

TMDB and TheTVDB are full cache-first facades: each request is served from the
per-provider Postgres cache when fresh, otherwise fetched once upstream with the
server-side API key and cached. fanart.tv works the same way. Image URLs in
facade payloads are rewritten to route through the `/_ia/<host>/<path>` byte
proxy (SSRF-guarded, on-disk cached), so the consumer never streams large images
straight from upstream. See
[docs/shirabe-api-contract.md](docs/shirabe-api-contract.md) for the full route
inventory and JSON shapes.

### Scoring

`score` (0-100) is synthesized from `pg_trgm` `similarity()` (0.0-1.0 scaled to
0-100), so a client's own confidence re-scoring keeps working. Results are
ordered by similarity descending and capped at `limit`.

### Query parser

The `query=` string is **not** parsed as full Lucene. A small hand-rolled parser
(`src/query.rs`) extracts the known fields (`release:`, `artist:`, `recording:`,
`date:`, `arid:`, `primarytype:`, `status:`), handling `"..."` quotes, `(...)`
groups, `AND`, escaped characters, and the `date:(YYYY*)` year-prefix wildcard.
The Lucene fuzzy suffix (`term~` / `term~2`) is stripped — the trigram search is
already fuzzy. A bare query (no `field:` markers) is treated as the whole artist
name.

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

Before first use, apply the mirror migrations once (`migrations/000*.sql`):

```sh
DATABASE_URL=postgres://readonly@mirror.example.com/musicbrainz_db \
  sqlx migrate run --source migrations
# or: make db/migrate/up
```

They create `pg_trgm` + `unaccent` extensions and GIN trigram/FTS indexes on the
searched name columns plus btree FK indexes on the join paths. They are
idempotent (`CREATE ... IF NOT EXISTS`) and additive — they never touch
replicated data and can be dropped without consequence.

The optional writable per-provider databases (`shirabe`, `imdb`, `tmdb`, `tvdb`,
`fanart`) are bootstrapped with `shirabe migrate <db>` (or `shirabe migrate all`)
— the SQL is embedded in the binary, so nothing extra ships to the cluster.

## Environment variables

A ready-to-copy [`.env.example`](.env.example) lists every variable, its
default, and meaning. `DATABASE_URL` is the only **required** one; startup fails
fast with a clear error if it is unset.

| Var | Required | Default | Purpose |
| --- | --- | --- | --- |
| `DATABASE_URL` | **yes** | _(none)_ | Postgres DSN for the MB mirror (read-only role) |
| `SHIRABE_DATABASE_URL` | no | _(unset)_ | Writable `shirabe` coordination DB (source registry, xref, image_cache) |
| `IMDB_DATABASE_URL` | no | _(unset)_ | Writable `imdb` bulk-mirror DB (IMDb TSV tables) |
| `TMDB_DATABASE_URL` | no | _(unset)_ | Writable `tmdb` cache/index DB (`tmdb_cache` + `tmdb_id_index`) |
| `TVDB_DATABASE_URL` | no | _(unset)_ | Writable `tvdb` cache DB (`tvdb_cache`) |
| `FANART_DATABASE_URL` | no | _(unset)_ | Writable `fanart` cache DB (`fanart_cache`) |
| `SHIRABE_BIND` | no | `0.0.0.0:8800` | HTTP bind address:port (server listens on **8800**) |
| `SHIRABE_DB_POOL_SIZE` | no | `8` | Max Postgres connections per pool |
| `SHIRABE_DEFAULT_LIMIT` | no | `25` | Default search `limit` |
| `SHIRABE_MAX_LIMIT` | no | `100` | Hard cap on requested `limit` |
| `SHIRABE_SIMILARITY_THRESHOLD` | no | `0.3` | Min `pg_trgm` similarity to keep a row |
| `SHIRABE_STATEMENT_TIMEOUT_MS` | no | `10000` | Per-connection `statement_timeout` on trigram search sessions |
| `SHIRABE_SEARCH_WORK_MEM` | no | `256MB` | Per-connection `work_mem` on trigram search sessions |
| `TMDB_API_KEY` | no | _(unset)_ | Server-side TMDB v3 key; unset → `/3` degrades to cache-only/503 |
| `TMDB_CACHE_TTL_DAYS` | no | `7` | TTL for cached TMDB payloads |
| `TVDB_API_KEY` | no | _(unset)_ | Server-side TheTVDB v4 key; unset → `/v4` degrades to cache-only/503 |
| `TVDB_PIN` | no | _(unset)_ | Optional operator PIN paired with `TVDB_API_KEY` |
| `TVDB_CACHE_TTL_DAYS` | no | `7` | TTL for cached TheTVDB payloads |
| `FANART_API_KEY` | no | _(unset)_ | Server-side fanart.tv key; unset → `/v3` degrades to cache-only/503 |
| `FANART_PERSONAL_API_KEY` | no | _(unset)_ | Optional personal fanart.tv key (sent as `client_key`) |
| `FANART_CACHE_TTL_DAYS` | no | `7` | TTL for cached fanart.tv payloads |
| `SHIRABE_CAACHE_BASE_URL` | no | _(empty)_ | External image-proxy base for rewritten artwork URLs; empty → shirabe's own relative `/_ia` route |
| `SHIRABE_COVERART_CACHE_DIR` | no | `/var/cache/shirabe/coverart` | On-disk byte cache for the `/_ia` proxy (put on a PVC) |
| `SHIRABE_COVERART_CACHE_MAX_BYTES` | no | `9663676416` | Byte-cache soft budget (~9 GiB; oldest-mtime eviction) |
| `SHIRABE_COVERART_POSITIVE_TTL_SECS` | no | `2592000` | Byte-cache TTL for 200 responses (30 days) |
| `SHIRABE_COVERART_NEGATIVE_TTL_SECS` | no | `21600` | Byte-cache TTL for 404 responses (6 hours) |
| `SHIRABE_COVERART_UPSTREAM_BASE` | no | `https://coverartarchive.org` | Upstream base for the `/release`, `/release-group` redirect layer |
| `SHIRABE_DEBUG_UI` | no | `false` | Opt-in SQL query explorer at `/debug/queries`; keep off in exposed deployments |
| `RUST_LOG` | no | `info` | tracing/`EnvFilter` filter |

Testing-only (hidden from `--help`; never set in a real deployment):
`TMDB_API_BASE`, `TVDB_API_BASE`, `FANART_API_BASE` point a facade at a mock
upstream, and `SHIRABE_COVERART_INSECURE_IA` disables the `/_ia` SSRF guard so a
local mock is reachable.

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
