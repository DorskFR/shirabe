# Shirabe API contract — native-shape facades

This document is the source of truth for the HTTP surface Shirabe exposes and
the JSON shapes downstream consumers parse. The route inventory below is derived
from `build_router` in `src/lib.rs` and the facade routers in `src/facades/`.

## 1. The facade approach

Shirabe exposes each upstream provider's **native** API surface, under that
provider's version-native prefix, on **one host** (e.g. in-cluster
`shirabe:8800`):

| Prefix   | Provider          | Upstream emulated             | Backing |
|----------|-------------------|-------------------------------|---------|
| `/ws/2`  | MusicBrainz       | musicbrainz.org ws/2 subset   | Postgres mirror + `pg_trgm`; no upstream calls |
| `/v4`    | TheTVDB           | api4.thetvdb.com **v4**       | cache-first facade (`tvdb_cache`, live fallback) |
| `/3`     | TMDB              | api.themoviedb.org **v3**     | cache-first facade (`tmdb_cache` + local index, live fallback) |
| `/v3`    | fanart.tv         | webservice.fanart.tv **v3**   | cache-first facade (`fanart_cache`, live fallback) |
| `/release`, `/release-group`, `/_ia` | Cover Art Archive | coverartarchive.org + archive.org bytes | redirect proxy + on-disk byte cache |

Because each facade emits the *native* upstream JSON, a consumer adopts Shirabe
by setting that provider's `base_url` to Shirabe — **zero client code change**:

```
musicbrainz.base_url = http://shirabe:8800/ws/2
tvdb.base_url        = http://shirabe:8800/v4
tmdb.base_url        = http://shirabe:8800/3
fanart.base_url      = http://shirabe:8800/v3
coverart.base_url    = http://shirabe:8800
```

For the keyed providers (TVDB/TMDB/fanart.tv) the inbound client key may be
empty or a dummy: **Shirabe ignores the inbound key and uses its own server-side
key** (`TMDB_API_KEY`, `TVDB_API_KEY`/`TVDB_PIN`, `FANART_API_KEY`). The real
keys are never re-exposed to clients.

Cross-provider IDs are surfaced **inside** the native shapes (TMDB
`external_ids.imdb_id`, TVDB `remoteIds`), backed by `shirabe.xref` — existing
client parsing picks them up with no new endpoint.

### 1.1 Alias matrix

Every native prefix is also served under a self-describing provider alias — the
same handlers back both forms. Embedded URLs inside payloads stay canonical
(native).

| Provider | Canonical | Aliases |
|---|---|---|
| MusicBrainz | `/ws/2/...` | `/musicbrainz/ws/2/...`, `/music/ws/2/...`, `/music/...` (version segment stripped: `/music/artist`, `/music/release-group`, ...) |
| TMDB | `/3/...` | `/tmdb/3/...` |
| TheTVDB | `/v4/...` | `/tvdb/v4/...` |
| fanart.tv | `/v3/...` | `/fanart/v3/...` |
| Cover Art Archive | `/release/...`, `/release-group/...`, `/_ia/...` | `/coverart/release/...`, `/coverart/release-group/...`, `/coverart/_ia/...` |

- `/music` carries both the stripped shortcuts and the full `/ws/2` tree, so a
  ws/2 client can use `/music` or `/music/ws/2` as its base URL.
- The former `/tv`, `/movie`, `/movies` category roots and the `/cover`
  namespace are **retired** and 404.
- `/health` and `/health/sources` exist at the root only.
- `/debug/queries` (+ `/debug/run`) exists only when `SHIRABE_DEBUG_UI` is set.

### 1.2 Error shapes

The router fallback answers every unknown path or wrong method — on every
mount — with JSON:

- `404` → `{ "error": "shirabe: no such route: GET /x" }`
- `405` → `{ "error": "shirabe: method not allowed: POST /health" }`

Each facade keeps its upstream's native error dialect for handler-level
failures:

| Facade | Shape | Statuses |
|---|---|---|
| MusicBrainz `/ws/2` | `{ "error": "<message>" }` (MB's shape) | 400 bad query/MBID, 404 not found, 500 DB error |
| TMDB `/3` | `{ "status_code": <n>, "status_message": "…" }` | 400 (code 22/34), 502 upstream (code 11), 503 not configured (code 7) |
| TheTVDB `/v4` | `{ "status": "failure", "message": "…" }` | 400 invalid id, 502 upstream, 503 not configured |
| fanart.tv `/v3` | `{ "status": "error", "error message": "…" }` — but an upstream **404 passes through** with the upstream body (authoritative "no artwork", safe to negative-cache) | 404 passthrough, 502 upstream, 503 not configured |
| Cover Art `/release`, `/_ia` | plain-text bodies | 403 forbidden host (SSRF guard), 502 upstream |

"Not configured" (503) means the server-side key is unset and the request could
not be served from cache; the server still boots and serves everything else.

## 2. MusicBrainz ws/2 facade (`/ws/2`)

Served from the read-only `musicbrainz` mirror via `pg_trgm`. `score` (0–100) is
synthesized from `similarity()`.

Search endpoints return the ws/2 envelope
`{ "count": <total matches>, "offset": <offset>, "<plural>": [...] }` and accept
`limit=` / `offset=`:

- `GET /ws/2/artist?query=&fmt=json` → `{ "count", "offset", "artists": [...] }`
- `GET /ws/2/release?query=&fmt=json` → `{ "count", "offset", "releases": [...] }`
- `GET /ws/2/recording?query=&fmt=json` → `{ "count", "offset", "recordings": [...] }`

Lookups:

- `GET /ws/2/artist/{mbid}?inc=url-rels+genres+tags+annotation` — honoured `inc`
  tokens: `url-rels`, `genres`, `tags`, `annotation` (`+` or space separated).
- `GET /ws/2/release/{mbid}` — full album: ordered media[]/tracks,
  release-group, relations[].
- `GET /ws/2/recording/{mbid}` — recording + releases.
- `GET /ws/2/release-group/{mbid}` — release-group lookup.

Release-group browse:

- `GET /ws/2/release-group?artist=<mbid>&limit=&offset=` →
  `{ "release-group-count", "release-group-offset", "release-groups": [...] }`.
  400 without a valid `artist` MBID.

Ping: `GET /ws/2` and `GET /health`. `GET /health/sources` reports per-source
health/staleness for every registered ingest source.

Shapes use MusicBrainz hyphenated keys (`artist-credit`, `release-group`,
`track-count`, `sort-name`, …) exactly. The `query=` string accepts the fielded
Lucene subset `release:`, `artist:`, `recording:`, `date:(YYYY*)`, `arid:`,
`primarytype:`, `status:` — quotes, `(...)` groups, `AND`, escapes. The fuzzy
suffix (`term~` / `term~2`) is stripped (trigram search is already fuzzy). A
release search with `arid:<mbid>` becomes an artist browse, optionally filtered
by `primarytype:` and `status:`.

## 3. TheTVDB v4 facade (`/v4`) — implemented, cache-first

Default upstream base `https://api4.thetvdb.com/v4`. Auth is faked: callers may
send any apikey/pin; Shirabe mints its own opaque token and uses the server-side
key upstream. Any `Authorization: Bearer <token>` is accepted on non-login
calls. Each data handler serves a fresh row from `tvdb_cache`
(TTL `TVDB_CACHE_TTL_DAYS`, default 7d) when present, otherwise fetches upstream
once, caches, and self-links `remoteIds` into `shirabe.xref`.

| Endpoint | Shape consumers parse |
|---|---|
| `POST /v4/login` `{apikey,pin}` | `{ "data": { "token": "<minted>" } }` (any body accepted; 503 failure shape when no server key) |
| `GET /v4/search?type=series&query=` | `{ "status": "success", "data": [ { "tvdb_id": "series-1396", "name", "year", "aliases": [], "translations": { "<lang>": "<name>" } } ] }` — local-first probe, live results merged/deduped by `tvdb_id`; `name`/`aliases`/`translations` preserved verbatim (non-latin scoring); the upstream `links` block is dropped (search is not paged) |
| `GET /v4/series/{id}` | series record |
| `GET /v4/series/{id}/extended` | `{ "data": { "name", "firstAired", "seasons": [ { …, "type": { "type": "official"\|"dvd"\|… } } ], "remoteIds": [...] } }` |
| `GET /v4/series/{id}/episodes/{season-type}?season=&page=` | `{ "data": { "episodes": [ { "number", "name", "seasonNumber", "aired", "runtime" } ] }, "links": { "next": "<url>"\|null } }` — pure passthrough of `links`; paginate until `links.next` is null/absent |
| `GET /v4/movies/{id}` | movie record; served from the upstream **extended** record so `remoteIds` cross-IDs are present |

Ids are accepted as bare integers (`1396`) or prefixed slugs (`series-1396`,
`movie-42`). Artwork URLs (`image`, `thumbnail`, `poster`, `banner`, `fanart`,
…) are rewritten through the `/_ia/<host>/<path>` byte proxy (or the external
`SHIRABE_CAACHE_BASE_URL` when configured).

## 4. TMDB v3 facade (`/3`) — implemented, cache-first

Default upstream base `https://api.themoviedb.org/3`. The `api_key` query param
is **accepted and ignored**. Each handler is cache-first against `tmdb_cache`
(TTL `TMDB_CACHE_TTL_DAYS`, default 7d); detail fetches self-link
`external_ids` into `shirabe.xref`.

| Endpoint | Shape consumers parse |
|---|---|
| `GET /3/configuration` | static TMDB image-config block, answered locally (no upstream call) |
| `GET /3/search/tv?query=` | `{ "page": 1, "results": [ { "id", "name", "original_name", "first_air_date", "overview", "popularity", … } ], "total_pages": 1, "total_results": <n> }` |
| `GET /3/search/movie?query=` | same envelope with `{ "id", "title", "original_title", "release_date", "overview", … }` results |
| `GET /3/tv/{id}` | `{ "name", "first_air_date", "seasons": [ { "season_number", "name" } ], "external_ids": { "imdb_id" }, … }` |
| `GET /3/tv/{id}/season/{n}` | `{ "episodes": [ { "episode_number", "name", "runtime" } ] }` |
| `GET /3/movie/{id}?append_to_response=external_ids` | `{ "title", "release_date", "runtime", "imdb_id", "external_ids": { "imdb_id" }, "overview" }` |

Search is **local-first**: the deployed index (`tmdb_id_index` + IMDb akas —
non-latin resolution) is probed first; on a thin/empty local result the live API
is consulted and merged (deduped by `id`, ranked by `popularity`). Results
always carry the full TMDB search envelope (single-page semantics) and the
score-bearing fields (`original_title`/`original_name`, dates, `overview`) are
guaranteed present.

Detail lookups honour `append_to_response`: the client's sections are unioned
with the facade's own per-kind set (movie: `external_ids,release_dates`; tv:
`external_ids,content_ratings`), so `external_ids.imdb_id` is always present.
One cache row serves every append combination once hydrated; a cached row
lacking a requested section forces a re-fetch. `imdb_id` is the cross-bridge:
consumers prefer the top-level `imdb_id` then fall back to
`external_ids.imdb_id`.

Relative image paths (`poster_path`, `backdrop_path`, `profile_path`,
`still_path`, `logo_path`, `file_path`) are rewritten through the image proxy.

## 5. fanart.tv v3 facade (`/v3`) — implemented, cache-first

Default upstream base `https://webservice.fanart.tv/v3`. Client-supplied
credentials (an `api-key` header or `?api_key=`) are accepted and ignored; the
server-side `FANART_API_KEY` (+ optional `FANART_PERSONAL_API_KEY` as
`client_key`) is used upstream. Cache-first against `fanart_cache`
(TTL `FANART_CACHE_TTL_DAYS`, default 7d).

| Endpoint | Shape |
|---|---|
| `GET /v3/music/{mbid}` | artist artwork (`artistthumb`, `artistbackground`, `musiclogo`, `hdmusiclogo`, `musicbanner`, …) |
| `GET /v3/music/albums/{mbid}` | album artwork (`albumcover`, `cdart`) keyed by artist MBID |
| `GET /v3/movies/{id}` | movie artwork keyed by TMDB or IMDb id |
| `GET /v3/tv/{id}` | TV artwork keyed by TheTVDB id |

An upstream 404 **passes through as 404** with the upstream body — an
authoritative "no artwork for this id" consumers may negative-cache. Asset URLs
(`url`, `preview`) are rewritten through the image proxy. The `fanart` source
appears in `GET /health/sources`.

## 6. Cover Art Archive facade (`/release`, `/release-group`, `/_ia`)

Two layers:

- **Redirect layer** — `GET /release/{*}` and `GET /release-group/{*}` proxy to
  `SHIRABE_COVERART_UPSTREAM_BASE` (default `https://coverartarchive.org`). CAA
  answers image requests with a 3xx to archive.org; the redirect is NOT followed
  server-side — its `Location` is rewritten to the local `/_ia/<host>/<path>`
  form, so the client comes back through the proxy for the bytes. Non-redirect
  200/404 bodies (JSON manifests) are disk-cached with a short TTL (300 s).
- **Byte layer** — `GET /_ia/{host}/{*path}` streams `https://<host>/<path>`,
  bouncing any further CDN redirect back through `/_ia/`, and caches bytes on
  disk (30 d positive / 6 h negative TTL, ~9 GiB budget with oldest-mtime
  eviction, single-flight per key). An `X-Cache-Status: HIT|MISS` header reports
  disposition. SSRF guard: HTTPS-only, explicit ports refused, and any host
  resolving to a private/loopback/link-local/unique-local/unspecified address is
  rejected with 403.

Both layers are mounted at the root **and** under `/coverart`; the cache key is
mount-invariant, and the `/coverart` prefix is stripped before proxying upstream
so CAA never sees it.

## 7. Cross-ID model

Cross-provider IDs are populated from `shirabe.xref` (Wikidata-bridged:
IMDb P345 ↔ TMDB P4947/P4983 ↔ TVDB P12196/P4835 ↔ MusicBrainz P434/5/6) plus
per-record `remote_ids`/`external_ids` returned during TVDB/TMDB hydration, and
surfaced inside the native shapes above (TMDB `external_ids.imdb_id`, TVDB
`remoteIds`). No dedicated xref endpoint is part of the consumer-facing
contract.

## 8. Storage model — one Postgres database per provider

Shirabe uses **separate databases per provider**, not one shared database with
multiple schemas:

| Database      | Env var                | Mode      | Holds |
|---------------|------------------------|-----------|-------|
| `musicbrainz` | `DATABASE_URL`         | read-only | the synced MB mirror (`musicbrainz` schema); never written |
| `shirabe`     | `SHIRABE_DATABASE_URL` | writable  | coordination data: `shirabe.source`, `shirabe.xref`, `shirabe.image_cache` |
| `imdb`        | `IMDB_DATABASE_URL`    | writable  | the bulk IMDb TSV mirror (`imdb_*` tables) |
| `tmdb`        | `TMDB_DATABASE_URL`    | writable  | `tmdb_cache` + `tmdb_id_index` |
| `tvdb`        | `TVDB_DATABASE_URL`    | writable  | `tvdb_cache` |
| `fanart`      | `FANART_DATABASE_URL`  | writable  | `fanart_cache` |

Only the writable databases are ever written; `musicbrainz` stays strictly
read-only. The API pod boots with only `DATABASE_URL` set; the writable URLs are
optional, and `shirabe sync <source>` errors clearly when a source needs a pool
whose URL is missing. Because each provider lives in its own Postgres, the
local-first search (`tmdb_id_index` + IMDb akas) is assembled per-pool in Rust
and merged — the databases cannot be SQL-joined.

Migrations are per-database. The writable DBs are bootstrapped in-cluster by
`shirabe migrate <db>` (`shirabe` | `imdb` | `tmdb` | `tvdb` | `fanart` | `all`;
the SQL is embedded in the binary via `include_str!`). The read-only
`musicbrainz` mirror migrations (`migrations/000*.sql`) are applied to the
mirror out of band and are NOT part of `shirabe migrate`:

- `migrations/000*.sql` → the `musicbrainz` mirror (out of band).
- `migrations/shirabe/*.sql` → the `shirabe` database (`shirabe migrate shirabe`).
- `migrations/imdb/*.sql` → the `imdb` database (`shirabe migrate imdb`).
- `migrations/tmdb/*.sql` → the `tmdb` database (`shirabe migrate tmdb`).
- `migrations/tvdb/*.sql` → the `tvdb` database (`shirabe migrate tvdb`).
- `migrations/fanart/*.sql` → the `fanart` database (`shirabe migrate fanart`).

`shirabe migrate all` applies every writable DB whose URL is configured (absent
ones are skipped). All migrations are forward-only and idempotent
(`CREATE … IF NOT EXISTS`).

Base tables in the `shirabe` database (`migrations/shirabe/0001_init.sql`; the
`shirabe.` schema prefix is kept, matching the code's references):

- `shirabe.source(name PK, ingest_mode, last_refresh_at, status, detail jsonb)` —
  per-source registry/health.
- `shirabe.xref(wikidata_qid, source, external_id, PK(source, external_id))` +
  index on `wikidata_qid`.
- `shirabe.image_cache(source, external_id, kind, remote_url, caache_url,
  fetched_at)` — artwork → proxy URL mapping.

The per-provider cache tables (`tmdb_cache`, `tmdb_id_index`, `tvdb_cache`,
`fanart_cache`) and the IMDb bulk dump tables (`imdb_title_*`,
`imdb_name_basics`, …) live unprefixed in their dedicated databases — the
database itself scopes them.

## 9. Decisions carried into this contract

- **DB topology (one database per provider):** separate databases — the
  read-only `musicbrainz` mirror plus writable `shirabe`, `imdb`, `tmdb`,
  `tvdb`, and `fanart` databases — not one shared DB with schemas.
- **Facade strictness:** implement the subset downstream consumers parse today;
  pass extra upstream fields through from cached payloads where cheap.
- **One host, native prefixes** (`/ws/2`, `/v4`, `/3`, `/v3`, CAA at the root) —
  matches how consumers set `base_url` per provider; provider aliases
  (`/musicbrainz`, `/tvdb`, `/tmdb`, `/fanart`, `/coverart`, `/music`) are
  sugar over the same handlers.
- **IMDb** is enrichment behind the TMDB/TVDB facades (akas → non-latin search,
  ratings, episode hierarchy), not a separate consumer-facing provider.
- **Images:** artwork URLs are rewritten through the `/_ia` byte proxy
  (Shirabe's own on-disk cache, or an external proxy via
  `SHIRABE_CAACHE_BASE_URL`); large image bytes never stream straight from
  upstream to the client.
