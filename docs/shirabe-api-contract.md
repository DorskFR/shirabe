# Shirabe API contract — native-shape facades

Status: SHIB-2 (architecture + unified contract). This document is the source of
truth for the HTTP surface Shirabe exposes and the JSON shapes Kusaritoi parses.
It distills the design spec (`shirabe-spec.md` §3, §4.6, §7) and the shapes the
Kusaritoi provider clients consume.

## 1. The facade approach

Shirabe exposes each upstream provider's **native** API surface, under that
provider's version-native prefix, on **one host** (`shirabe.dorsk.dev` /
in-cluster `shirabe:8800`):

| Prefix   | Provider          | Upstream emulated            | Status      |
|----------|-------------------|------------------------------|-------------|
| `/ws/2`  | MusicBrainz       | musicbrainz.org ws/2 subset  | implemented |
| `/v4`    | TheTVDB           | api4.thetvdb.com **v4**      | skeleton    |
| `/3`     | TMDB              | api.themoviedb.org **v3**    | skeleton    |

Because each facade emits the *native* upstream JSON, Kusaritoi consumes Shirabe
by setting that provider's `base_url` to Shirabe — **zero client code change**:

```
musicbrainz.base_url = http://shirabe:8800/ws/2
tvdb.base_url        = http://shirabe:8800/v4
tmdb.base_url        = http://shirabe:8800/3
```

Kusaritoi resolves `base_url`/`api_key` per provider from its DB `Provider` row
(empty → hardcoded upstream default). For the keyed providers (TVDB/TMDB) the key
may be empty or a dummy: **Shirabe ignores the inbound key and uses its own
server-side key.** For TVDB the PIN rides on `Provider.password`. So pointing
Kusaritoi at Shirabe is pure config.

Cross-provider IDs are surfaced **inside** the native shapes (TMDB
`external_ids.imdb_id`, TVDB `remoteIds`), backed by `shirabe.xref` — so
Kusaritoi's existing parsing picks them up with no new endpoint.

## 2. MusicBrainz ws/2 facade (`/ws/2`) — implemented

Already served from the read-only `musicbrainz` mirror via `pg_trgm`. `score`
(0–100) is synthesized from `similarity()`.

- `GET /ws/2/artist?query=&fmt=json` → `{ "artists": [...] }`
- `GET /ws/2/artist/{mbid}?inc=url-rels`
- `GET /ws/2/release?query=&fmt=json` → `{ "releases": [...] }`
- `GET /ws/2/release/{mbid}`
- `GET /ws/2/recording?query=&fmt=json` → `{ "recordings": [...] }`
- `GET /ws/2/recording/{mbid}`
- `GET /ws/2` and `GET /health` — ping.

Shapes use MusicBrainz hyphenated keys (`artist-credit`, `release-group`,
`track-count`, `sort-name`, …) exactly. The query string accepts the Lucene
subset (`release:`, `artist:`, `recording:`, `date:(YYYY*)`).

## 3. TheTVDB v4 facade (`/v4`) — skeleton

Default upstream base `https://api4.thetvdb.com/v4`. Auth is faked: callers may
send any apikey/pin; Shirabe mints its own token and uses the server-side key.

| Endpoint | Shape Kusaritoi parses |
|---|---|
| `POST /v4/login` `{apikey,pin}` | `{ "data": { "token": "<minted>" } }` |
| `GET /v4/search?type=series&query=` | `{ "data": [ { "tvdb_id": "series-1396", "name", "year", "aliases": [], "translations": { "<lang>": "<name>" } } ] }` — scored over name + aliases + translations (non-latin path). |
| `GET /v4/series/{id}` | series record. |
| `GET /v4/series/{id}/extended` | `{ "data": { "name", "firstAired", "seasons": [ { …, "type": { "type": "official"\|"dvd"\|… } } ], "remoteIds": [...] } }` |
| `GET /v4/series/{id}/episodes/{season-type}?season=&page=` | `{ "data": { "episodes": [ { "number", "name", "seasonNumber", "aired", "runtime" } ] }, "links": { "next": "<url>"\|null } }` — paginate until `links.next` is absent. |
| `GET /v4/movies/{id}` | movie record (cross-IDs via `remoteIds`). |

`Authorization: Bearer <token>` is accepted on every non-login call.

## 4. TMDB v3 facade (`/3`) — skeleton

Default upstream base `https://api.themoviedb.org/3`. The `api_key` query param
is **accepted and ignored**. Upstream image base is
`https://image.tmdb.org/t/p/original`; Shirabe rewrites artwork URLs to caache.

| Endpoint | Shape Kusaritoi parses |
|---|---|
| `GET /3/search/tv?query=` | `{ "results": [ { "id", "name", "first_air_date" } ] }` |
| `GET /3/search/movie?query=` | `{ "results": [ { "id", "title", "release_date", "overview" } ] }` |
| `GET /3/tv/{id}` | `{ "name", "first_air_date", "seasons": [ { "season_number", "name" } ] }` |
| `GET /3/tv/{id}/season/{n}` | `{ "episodes": [ { "episode_number", "name", "runtime" } ] }` |
| `GET /3/movie/{id}?append_to_response=external_ids` | `{ "title", "release_date", "runtime", "imdb_id", "external_ids": { "imdb_id" }, "overview" }` |

`imdb_id` is the cross-bridge: Kusaritoi prefers the top-level `imdb_id` then
falls back to `external_ids.imdb_id`. Shirabe must honor
`append_to_response=external_ids` so `external_ids.imdb_id` is present.

## 5. Cross-ID model

Cross-provider IDs are populated from `shirabe.xref` (Wikidata-bridged:
IMDb P345 ↔ TMDB P4947/P4983 ↔ TVDB P12196/P4835 ↔ MusicBrainz P434/5/6) plus
per-record `remote_ids`/`external_ids` returned during TVDB/TMDB hydration, and
surfaced inside the native shapes above (TMDB `external_ids.imdb_id`, TVDB
`remoteIds`). An optional internal `GET /xref?source=&id=` may be added later;
it is not part of the Kusaritoi-facing contract.

## 6. Storage model (`shirabe` schema)

The writable `shirabe` schema lives in the **same database** as the read-only
`musicbrainz` mirror (reuse `musicbrainz-database` with a RW role). The mirror's
`musicbrainz` schema is never written. Base tables (migration
`0003_shirabe_schema.sql`):

- `shirabe.source(name PK, ingest_mode, last_refresh_at, status, detail jsonb)` —
  per-source registry/health.
- `shirabe.xref(wikidata_qid, source, external_id, PK(source, external_id))` +
  index on `wikidata_qid`.
- `shirabe.tmdb_cache(id, kind, payload jsonb, fetched_at)` — lazy-hydrate cache.
- `shirabe.tvdb_cache(id, kind, payload jsonb, fetched_at)` — lazy-fetch cache.
- `shirabe.tmdb_id_index(id, kind, name, popularity, adult)` — daily ID-export
  enumeration.
- `shirabe.image_cache(source, external_id, kind, remote_url, caache_url,
  fetched_at)` — artwork → caache URL mapping.

Per-source bulk dump tables (IMDb `title.*`/`name.basics`, etc.) land in later
migrations alongside their source implementations. Migrations are forward-only
and idempotent.

## 7. Decisions carried into this contract (spec §7)

- **DB topology:** reuse the existing `musicbrainz-database` Postgres with a new
  `shirabe` schema + RW role (orchestrator decision), not a dedicated DB.
- **Facade strictness:** implement the subset Kusaritoi parses today; pass extra
  upstream fields through from cached payloads where cheap.
- **One host, native prefixes** (`/ws/2`, `/v4`, `/3`) — matches how Kusaritoi
  sets `base_url` per provider.
- **IMDb** is enrichment behind the TMDB/TVDB facades (akas → non-latin search,
  ratings, episode hierarchy), not a separate Kusaritoi provider initially.
- **Images:** rewrite URLs to caache; Shirabe stays stateless on image bytes.
