-- TMDB cache/index schema — dedicated `tmdb` database (five-DB layout).
--
-- This migration targets the dedicated WRITABLE `tmdb` database (connected via
-- TMDB_DATABASE_URL), NOT the `shirabe`, `imdb`, or read-only `musicbrainz`
-- databases. The tmdb_cache + tmdb_id_index tables previously lived in the
-- shirabe DB; they now live here, in this DB's default (public) schema — no
-- schema prefix, since the database itself scopes them. Forward-only and
-- idempotent (CREATE … IF NOT EXISTS): safe to re-run, never edited once applied.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ── TMDB lazy-hydrate cache ─────────────────────────────────
-- Raw upstream TMDB API payloads keyed by (id, kind). `kind` distinguishes the
-- endpoint family (e.g. 'movie', 'tv', 'tv_season', 'search_*'). fetched_at
-- drives TTL/LRU prune.
CREATE TABLE IF NOT EXISTS tmdb_cache (
    id          bigint NOT NULL,
    kind        text NOT NULL,
    payload     jsonb NOT NULL,
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, kind)
);

-- ── TMDB ID-export enumeration index ───────────────────────
-- Populated from TMDB's daily id exports (no full dump exists). Tells us what
-- exists + popularity for ranking ties; the cache holds the hydrated detail.
CREATE TABLE IF NOT EXISTS tmdb_id_index (
    id          bigint NOT NULL,
    kind        text NOT NULL,
    name        text,
    popularity  real,
    adult       boolean,
    PRIMARY KEY (id, kind)
);

-- pg_trgm GIN index on the name (the local-search path: 銀魂 → Gintama, etc.).
CREATE INDEX IF NOT EXISTS tmdb_id_index_name_trgm_idx
    ON tmdb_id_index USING gin (name gin_trgm_ops);
