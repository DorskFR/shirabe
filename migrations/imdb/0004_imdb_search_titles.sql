-- SHIB-26: popularity-ranked compact search table for IMDb title search.
--
-- Targets the dedicated WRITABLE `imdb` database (IMDB_DATABASE_URL); tables live
-- in the default (public) schema. Forward-only and idempotent (safe to re-run).
--
-- 0003's FTS fast path still similarity-ranks EVERY lexeme match: for a common
-- single word each branch bitmap-fetches every matching heap row from the wide
-- basics/akas tables to compute similarity(f_unaccent(title), f_unaccent($q)),
-- then top-N sorts. Work scales with word frequency, not answer size ('love' =
-- ~150k akas rows). This denormalized table carries ONE narrow row per distinct
-- (tconst, unaccented title), with title_ua and tsv precomputed (no per-row
-- function calls at query time) and num_votes as the popularity prior. The query
-- becomes: FTS-filter on tsv -> top-K by num_votes -> similarity-rank only those
-- K -> LIMIT. Work is bounded at K regardless of word frequency.
--
-- The imdb-fts follow-up step (src/sources/imdb_index.rs) rebuilds and repopulates
-- this table after each bulk-dump swap, since the swap drops the base tables and
-- leaves this derived table stale; this migration seeds it for a fresh database.

CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE OR REPLACE FUNCTION public.f_unaccent(text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS
$$ SELECT public.unaccent('public.unaccent', $1) $$;

CREATE TABLE IF NOT EXISTS imdb_search_titles (
    tconst     text NOT NULL,
    title_ua   text NOT NULL,
    title_type text,
    num_votes  integer NOT NULL DEFAULT 0,
    tsv        tsvector NOT NULL
);

TRUNCATE imdb_search_titles;

INSERT INTO imdb_search_titles (tconst, title_ua, title_type, num_votes, tsv)
SELECT DISTINCT ON (tconst, title_ua)
       tconst, title_ua, title_type, num_votes,
       to_tsvector('simple', title_ua)
FROM (
    SELECT b.tconst,
           public.f_unaccent(b.primary_title) AS title_ua,
           b.title_type,
           COALESCE(r.num_votes, 0) AS num_votes
    FROM imdb_title_basics b
    LEFT JOIN imdb_title_ratings r ON r.tconst = b.tconst
    WHERE b.primary_title IS NOT NULL
    UNION ALL
    SELECT b.tconst,
           public.f_unaccent(b.original_title),
           b.title_type,
           COALESCE(r.num_votes, 0)
    FROM imdb_title_basics b
    LEFT JOIN imdb_title_ratings r ON r.tconst = b.tconst
    WHERE b.original_title IS NOT NULL
    UNION ALL
    SELECT a.title_id,
           public.f_unaccent(a.title),
           b.title_type,
           COALESCE(r.num_votes, 0)
    FROM imdb_title_akas a
    JOIN imdb_title_basics b ON b.tconst = a.title_id
    LEFT JOIN imdb_title_ratings r ON r.tconst = a.title_id
    WHERE a.title IS NOT NULL
) s
WHERE title_ua <> ''
ORDER BY tconst, title_ua, num_votes DESC;

CREATE INDEX IF NOT EXISTS imdb_search_titles_tsv_gin
    ON imdb_search_titles USING gin (tsv);

CREATE INDEX IF NOT EXISTS imdb_search_titles_tconst_idx
    ON imdb_search_titles (tconst);
