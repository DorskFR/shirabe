-- SHIB-24: unaccent full-text-search fast path for IMDb title search.
--
-- Targets the dedicated WRITABLE `imdb` database (IMDB_DATABASE_URL); tables live
-- in the default (public) schema. Forward-only and idempotent.
--
-- 0001/0002 built gist_trgm_ops indexes so SEARCH_IMDB_TITLES could stream each
-- branch's top-N via the KNN `<->` operator. Measured on the live 58M-row mirror,
-- that KNN scan is pathologically slow for short, common query words: 'dune'
-- reads ~113k index buffers (~880MB) to return 25 rows — ~44 seconds. gist_trgm
-- distance-ordered traversal simply does not bound for common trigrams.
--
-- Postgres FTS indexes whole words (lexemes) and intersects their posting lists,
-- so `to_tsvector(title) @@ websearch_to_tsquery(q)` reduces 'dune' to the handful
-- of rows that actually contain the word (single-digit ms). FTS matches lexemes
-- EXACTLY, so accented originals (e.g. 'Amélie') would miss; we fold accents on
-- both sides with an IMMUTABLE unaccent wrapper (usable in an index expression).
-- Mirrors the MusicBrainz migration 0003_search_fts.sql.
--
-- The gist_trgm_ops indexes from 0001 are KEPT as the fuzzy fallback (typos /
-- partials) when FTS returns nothing, so recall never regresses.

CREATE EXTENSION IF NOT EXISTS unaccent;

-- unaccent()'s 2-arg form (regdictionary, text) is IMMUTABLE, so wrapping it lets
-- the result be used in an index expression (the 1-arg form is only STABLE).
CREATE OR REPLACE FUNCTION public.f_unaccent(text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS
$$ SELECT public.unaccent('public.unaccent', $1) $$;

CREATE INDEX IF NOT EXISTS imdb_title_basics_primary_title_fts_ua
    ON imdb_title_basics USING gin (to_tsvector('simple', public.f_unaccent(primary_title)));
CREATE INDEX IF NOT EXISTS imdb_title_basics_original_title_fts_ua
    ON imdb_title_basics USING gin (to_tsvector('simple', public.f_unaccent(original_title)));
CREATE INDEX IF NOT EXISTS imdb_title_akas_title_fts_ua
    ON imdb_title_akas USING gin (to_tsvector('simple', public.f_unaccent(title)));
