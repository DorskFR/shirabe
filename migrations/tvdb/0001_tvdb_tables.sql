-- TheTVDB cache schema — dedicated `tvdb` database (five-DB layout).
--
-- This migration targets the dedicated WRITABLE `tvdb` database (connected via
-- TVDB_DATABASE_URL), NOT the `shirabe`, `imdb`, or read-only `musicbrainz`
-- databases. The tvdb_cache table previously lived in the shirabe DB; it now
-- lives here, in this DB's default (public) schema — no schema prefix, since the
-- database itself scopes it. Forward-only and idempotent (CREATE … IF NOT
-- EXISTS): safe to re-run, never edited once applied.

-- ── TheTVDB lazy-fetch cache ────────────────────────────────
-- Raw upstream TheTVDB v4 API payloads keyed by (id, kind), same shape as the
-- TMDB cache. fetched_at drives TTL/LRU prune.
CREATE TABLE IF NOT EXISTS tvdb_cache (
    id          bigint NOT NULL,
    kind        text NOT NULL,
    payload     jsonb NOT NULL,
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, kind)
);
