-- fanart.tv cache schema — dedicated `fanart` database (five-DB layout).
--
-- This migration targets the dedicated WRITABLE `fanart` database (connected via
-- FANART_DATABASE_URL), NOT the `shirabe`, `imdb`, `tmdb`, `tvdb`, or read-only
-- `musicbrainz` databases. It lives in this DB's default (public) schema — no
-- schema prefix, since the database itself scopes it. Forward-only and idempotent
-- (CREATE … IF NOT EXISTS): safe to re-run, never edited once applied.

-- ── fanart.tv lazy-fetch cache ──────────────────────────────
-- Raw upstream fanart.tv v3 API payloads keyed by (cache_key, kind). The key is a
-- MusicBrainz MBID (music) or a provider id (movies/tv), so it is text rather than
-- bigint. fetched_at drives TTL/LRU prune.
CREATE TABLE IF NOT EXISTS fanart_cache (
    cache_key   text NOT NULL,
    kind        text NOT NULL,
    payload     jsonb NOT NULL,
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cache_key, kind)
);
