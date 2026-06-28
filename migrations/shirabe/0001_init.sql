-- shirabe writable schema — dedicated `shirabe` database (Option A multi-DB).
--
-- This migration targets the NEW, dedicated WRITABLE `shirabe` database
-- (connected via SHIRABE_DATABASE_URL), NOT the read-only `musicbrainz` mirror.
-- The MB-mirror migrations (../0001, ../0002) target the `musicbrainz` database
-- and must never run here; likewise this file must never run against the mirror.
-- We keep the `shirabe.` schema prefix for clarity and to match the code's table
-- references, even though it now lives in its own database. Forward-only and
-- idempotent (CREATE … IF NOT EXISTS): safe to re-run, never edited once applied.
--
-- This migration creates only the base tables (source registry, cross-ID xref,
-- TMDB/TVDB caches + the TMDB id index, and the image cache). The bulk
-- per-source dump tables (IMDb, etc.) land in their own later migrations
-- (SHIB-3..) alongside the source implementations.

CREATE SCHEMA IF NOT EXISTS shirabe;

-- ── Source registry / health ───────────────────────────────
-- One row per ingest source. Workers update last_refresh_at/status/detail so
-- /health/sources can report freshness, row counts, token validity, etc.
CREATE TABLE IF NOT EXISTS shirabe.source (
    name            text PRIMARY KEY,
    ingest_mode     text NOT NULL,
    last_refresh_at timestamptz,
    status          text,
    detail          jsonb
);

-- ── Cross-ID map (Wikidata-bridged + API remote_ids) ───────
-- (wikidata_qid, source, external_id): a title's id in each provider, keyed by
-- its Wikidata entity. PK on (source, external_id) so an upsert per provider id
-- is idempotent; the wikidata_qid index supports fan-out from one provider id to
-- the sibling ids across providers.
CREATE TABLE IF NOT EXISTS shirabe.xref (
    wikidata_qid    text,
    source          text NOT NULL,
    external_id     text NOT NULL,
    PRIMARY KEY (source, external_id)
);

CREATE INDEX IF NOT EXISTS shirabe_xref_wikidata_qid_idx
    ON shirabe.xref (wikidata_qid);

-- ── TMDB lazy-hydrate cache ─────────────────────────────────
-- Raw upstream TMDB API payloads keyed by (id, kind). `kind` distinguishes the
-- endpoint family (e.g. 'movie', 'tv', 'tv_season', 'search_*'). fetched_at
-- drives TTL/LRU prune.
CREATE TABLE IF NOT EXISTS shirabe.tmdb_cache (
    id          bigint NOT NULL,
    kind        text NOT NULL,
    payload     jsonb NOT NULL,
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, kind)
);

-- ── TheTVDB lazy-fetch cache ────────────────────────────────
-- Same shape as the TMDB cache for the v4 facade.
CREATE TABLE IF NOT EXISTS shirabe.tvdb_cache (
    id          bigint NOT NULL,
    kind        text NOT NULL,
    payload     jsonb NOT NULL,
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, kind)
);

-- ── TMDB ID-export enumeration index ───────────────────────
-- Populated from TMDB's daily id exports (no full dump exists). Tells us what
-- exists + popularity for ranking ties; the cache holds the hydrated detail.
CREATE TABLE IF NOT EXISTS shirabe.tmdb_id_index (
    id          bigint NOT NULL,
    kind        text NOT NULL,
    name        text,
    popularity  real,
    adult       boolean,
    PRIMARY KEY (id, kind)
);

-- ── Image cache (URLs rewritten to caache) ─────────────────
-- Maps a provider artwork to its caache-proxied URL. Mostly Shirabe just
-- rewrites remote_url → caache_url; the row records the mapping + fetch time.
-- RESERVED / UNUSED (SHIB-9): the image flow is stateless URL-rewrite — facades
-- rewrite TMDB/TVDB image URLs through caache at serialize time (see src/images.rs)
-- and never persist a row here. This table is kept for optional future URL
-- bookkeeping only.
CREATE TABLE IF NOT EXISTS shirabe.image_cache (
    source      text NOT NULL,
    external_id text NOT NULL,
    kind        text NOT NULL,
    remote_url  text,
    caache_url  text,
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, external_id, kind)
);
