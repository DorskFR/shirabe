-- SHIB-24: unaccent full-text-search fast path for TMDB id-index name search.
--
-- Targets the dedicated WRITABLE `tmdb` database (TMDB_DATABASE_URL); tables live
-- in the default (public) schema. Forward-only and idempotent.
--
-- 0001 created a gin_trgm_ops index on tmdb_id_index.name. Trigram is the right
-- tool for FUZZY matching (typos, partials — kept as the fallback), but the wrong
-- primary filter for whole-word title search: a query like 'dune' shares common
-- 3-grams with a large slice of the 1.4M-row id index, all of which pg_trgm must
-- score (~97ms warm, over the 100ms objective at the tail).
--
-- Postgres FTS indexes whole words (lexemes) and intersects their posting lists,
-- so `to_tsvector(name) @@ websearch_to_tsquery(q)` reduces the query to the rows
-- that actually contain every query word. FTS matches lexemes EXACTLY, so 'dune'
-- would miss accented titles; we fold accents on both sides with an IMMUTABLE
-- unaccent wrapper (usable in an index expression). Mirrors the MusicBrainz
-- migration 0003_search_fts.sql.
--
-- The gin_trgm_ops index from 0001 is KEPT: the search falls back to a trigram `%`
-- scan when FTS returns nothing, so recall never regresses.

CREATE EXTENSION IF NOT EXISTS unaccent;

-- unaccent()'s 2-arg form (regdictionary, text) is IMMUTABLE, so wrapping it lets
-- the result be used in an index expression (the 1-arg form is only STABLE).
CREATE OR REPLACE FUNCTION public.f_unaccent(text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS
$$ SELECT public.unaccent('public.unaccent', $1) $$;

CREATE INDEX IF NOT EXISTS tmdb_id_index_name_fts_ua
    ON tmdb_id_index USING gin (to_tsvector('simple', public.f_unaccent(name)));
