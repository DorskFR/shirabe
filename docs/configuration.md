# Configuration reference

Every setting is an environment variable and also a `--flag` (`shirabe --help`).
[`.env.example`](../.env.example) lists all of them with comments; `src/config.rs`
is the source of truth and a unit test fails if `.env.example` misses one.

`DATABASE_URL` is the only required variable; startup fails with a clear error
if it is unset.

## Databases

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | required | Postgres DSN for the MusicBrainz mirror (use a read-only role) |
| `SHIRABE_DATABASE_URL` | unset | Writable `shirabe` coordination DB (source registry, xref, image_cache) |
| `IMDB_DATABASE_URL` | unset | Writable `imdb` bulk-mirror DB (IMDb TSV tables) |
| `TMDB_DATABASE_URL` | unset | Writable `tmdb` cache/index DB (`tmdb_cache` + `tmdb_id_index`) |
| `TVDB_DATABASE_URL` | unset | Writable `tvdb` cache DB (`tvdb_cache`) |
| `FANART_DATABASE_URL` | unset | Writable `fanart` cache DB (`fanart_cache`) |

## HTTP server and search

| Variable | Default | Purpose |
| --- | --- | --- |
| `SHIRABE_BIND` | `0.0.0.0:8800` | HTTP bind address:port |
| `SHIRABE_DB_POOL_SIZE` | `8` | Max Postgres connections per pool |
| `SHIRABE_DEFAULT_LIMIT` | `25` | Default search `limit` |
| `SHIRABE_MAX_LIMIT` | `100` | Hard cap on requested `limit` |
| `SHIRABE_SIMILARITY_THRESHOLD` | `0.3` | Min `pg_trgm` similarity to keep a row |
| `SHIRABE_STATEMENT_TIMEOUT_MS` | `10000` | Per-connection `statement_timeout` on trigram search sessions |
| `SHIRABE_SEARCH_WORK_MEM` | `256MB` | Per-connection `work_mem` on trigram search sessions |
| `SHIRABE_SEARCH_CACHE_TTL_SECS` | `300` | TTL of the in-process ws/2 search response cache; `0` disables storage (identical concurrent searches still share one query) |
| `SHIRABE_SEARCH_CACHE_MAX_ENTRIES` | `4096` | Max cached search responses, oldest evicted first; `0` disables storage |

## Provider facades

| Variable | Default | Purpose |
| --- | --- | --- |
| `TMDB_API_KEY` | unset | Server-side TMDB v3 key; unset means `/3` serves cache only, else 503 |
| `TMDB_CACHE_TTL_DAYS` | `7` | TTL for cached TMDB payloads |
| `TVDB_API_KEY` | unset | Server-side TheTVDB v4 key; unset means `/v4` serves cache only, else 503 |
| `TVDB_PIN` | unset | Optional operator PIN paired with `TVDB_API_KEY` |
| `TVDB_CACHE_TTL_DAYS` | `7` | TTL for cached TheTVDB payloads |
| `FANART_API_KEY` | unset | Server-side fanart.tv key; unset means `/v3` serves cache only, else 503 |
| `FANART_PERSONAL_API_KEY` | unset | Optional personal fanart.tv key (sent as `client_key`) |
| `FANART_CACHE_TTL_DAYS` | `7` | TTL for cached fanart.tv payloads |

## Cover art and image proxy

| Variable | Default | Purpose |
| --- | --- | --- |
| `SHIRABE_CAACHE_BASE_URL` | empty | External image-proxy base for rewritten artwork URLs; empty means shirabe's own relative `/_ia` route |
| `SHIRABE_COVERART_CACHE_DIR` | `/var/cache/shirabe/coverart` | On-disk byte cache for the `/_ia` proxy; mount persistent storage here |
| `SHIRABE_COVERART_CACHE_MAX_BYTES` | `9663676416` | Byte-cache soft budget (9 GiB, oldest-mtime eviction) |
| `SHIRABE_COVERART_POSITIVE_TTL_SECS` | `2592000` | Byte-cache TTL for 200 responses (30 days) |
| `SHIRABE_COVERART_NEGATIVE_TTL_SECS` | `21600` | Byte-cache TTL for 404 responses (6 hours) |
| `SHIRABE_COVERART_UPSTREAM_BASE` | `https://coverartarchive.org` | Upstream for the `/release`, `/release-group` redirect layer |

## Debug UI

| Variable | Default | Purpose |
| --- | --- | --- |
| `SHIRABE_DEBUG_UI` | `false` | Opt-in SQL query explorer at `/debug/queries`; keep it off on exposed deployments |
| `SHIRABE_DEBUG_UI_TOKEN` | unset | Bearer token required on every `/debug/*` request (else 401). Without it the explorer is not mounted and a WARN is logged at boot |

## Logging and telemetry

| Variable | Default | Purpose |
| --- | --- | --- |
| `RUST_LOG` | `info` | `EnvFilter` log filter; `info` includes one access-log line per request (method, path, status, latency ms). ANSI colours are off when stdout is not a tty |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | OTLP/HTTP collector; trace export is off unless this (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) is set |

The other standard `OTEL_*` variables (`OTEL_SERVICE_NAME`,
`OTEL_RESOURCE_ATTRIBUTES`, `OTEL_TRACES_SAMPLER[_ARG]`, `OTEL_SDK_DISABLED`)
apply.

With an OTLP endpoint set, shirabe exports one trace per request and per
`sync` / `migrate` run: a server span per route (W3C `traceparent` honoured),
internal spans on handlers, search, facades and ingest phases, a client span
per upstream HTTP call (trace context propagated, secret query values
redacted) and a `db.query` client span per SQL statement with its text and row
count. Request query strings and API keys are never recorded.

## Test-only variables

Hidden from `--help`; never set them in a real deployment. `TMDB_API_BASE`,
`TVDB_API_BASE` and `FANART_API_BASE` point a facade at a mock upstream, and
`SHIRABE_COVERART_INSECURE_IA` disables the `/_ia` SSRF guard so a local mock
is reachable.
