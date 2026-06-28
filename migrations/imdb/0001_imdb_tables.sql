-- IMDb bulk-mirror tables — dedicated, WRITABLE `imdb` database (Option A multi-DB).
--
-- This migration targets the dedicated WRITABLE `imdb` database (connected via
-- IMDB_DATABASE_URL), NOT the read-only `musicbrainz` mirror and NOT the
-- `shirabe` coordination database. Forward-only and idempotent
-- (CREATE … IF NOT EXISTS): safe to re-run, never edited once applied.
--
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ LICENSE / ATTRIBUTION — IMDb Non-Commercial Datasets                     │
-- │                                                                          │
-- │ The data loaded into these tables comes from the IMDb Non-Commercial     │
-- │ Datasets (https://datasets.imdbws.com/). It is licensed for PERSONAL     │
-- │ and NON-COMMERCIAL use ONLY. See https://www.imdb.com/interfaces/ and    │
-- │ IMDb's conditions of use. Do NOT use this data commercially.             │
-- └─────────────────────────────────────────────────────────────────────────┘
--
-- The IMDb source (`src/sources/imdb.rs`, SHIB-5) ingests via the staging-and-
-- swap pattern: it COPY-loads each `imdb_*_new` staging table, then in one
-- transaction DROPs the live table and RENAMEs the staging table (and its
-- indexes) into place, so live reads never observe a half-loaded dataset. This
-- migration creates the initial EMPTY base tables + indexes so the database is
-- valid before the first ingest; the source recreates/swaps them thereafter.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ── title.basics ───────────────────────────────────────────────────────────
-- One row per title (movie, short, tvSeries, tvEpisode, …). primary_title /
-- original_title carry the searchable names (non-latin originals included).
CREATE TABLE IF NOT EXISTS imdb_title_basics (
    tconst          text PRIMARY KEY,
    title_type      text,
    primary_title   text,
    original_title  text,
    is_adult        boolean,
    start_year      integer,
    end_year        integer,
    runtime_minutes integer,
    genres          text
);

CREATE INDEX IF NOT EXISTS imdb_title_basics_primary_title_trgm
    ON imdb_title_basics USING gin (primary_title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS imdb_title_basics_original_title_trgm
    ON imdb_title_basics USING gin (original_title gin_trgm_ops);

-- ── title.episode ──────────────────────────────────────────────────────────
-- episode <-> series link with season + episode numbers.
CREATE TABLE IF NOT EXISTS imdb_title_episode (
    tconst          text PRIMARY KEY,
    parent_tconst   text,
    season_number   integer,
    episode_number  integer
);

CREATE INDEX IF NOT EXISTS imdb_title_episode_parent_idx
    ON imdb_title_episode (parent_tconst);

-- ── title.akas ─────────────────────────────────────────────────────────────
-- Alternate titles per region/language — the non-latin search path
-- (e.g. 銀魂 / Gintama). `ordering` disambiguates rows for one title.
CREATE TABLE IF NOT EXISTS imdb_title_akas (
    title_id          text NOT NULL,
    ordering          integer NOT NULL,
    title             text,
    region            text,
    language          text,
    types             text,
    attributes        text,
    is_original_title boolean,
    PRIMARY KEY (title_id, ordering)
);

CREATE INDEX IF NOT EXISTS imdb_title_akas_title_trgm
    ON imdb_title_akas USING gin (title gin_trgm_ops);

-- ── title.ratings ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS imdb_title_ratings (
    tconst          text PRIMARY KEY,
    average_rating  real,
    num_votes       integer
);

-- ── title.crew ─────────────────────────────────────────────────────────────
-- directors / writers as comma-separated nconst lists (as IMDb ships them).
CREATE TABLE IF NOT EXISTS imdb_title_crew (
    tconst          text PRIMARY KEY,
    directors       text,
    writers         text
);

-- ── title.principals ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS imdb_title_principals (
    tconst          text NOT NULL,
    ordering        integer NOT NULL,
    nconst          text,
    category        text,
    job             text,
    characters      text,
    PRIMARY KEY (tconst, ordering)
);

CREATE INDEX IF NOT EXISTS imdb_title_principals_nconst_idx
    ON imdb_title_principals (nconst);

-- ── name.basics ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS imdb_name_basics (
    nconst              text PRIMARY KEY,
    primary_name        text,
    birth_year          integer,
    death_year          integer,
    primary_profession  text,
    known_for_titles    text
);

CREATE INDEX IF NOT EXISTS imdb_name_basics_primary_name_trgm
    ON imdb_name_basics USING gin (primary_name gin_trgm_ops);
