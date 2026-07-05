-- SHIB-21: drop the superseded gin_trgm_ops indexes on the IMDb title columns.
--
-- 0001 originally created gin_trgm_ops indexes on
-- imdb_title_basics.primary_title / original_title and imdb_title_akas.title.
-- GIN cannot answer the KNN distance operator `<->`, so SEARCH_IMDB_TITLES had to
-- materialise the entire `%` candidate set before its LIMIT — a short,
-- common-trigram query like "dune" pulled a huge slice of the 58M-row
-- imdb_title_akas and timed out.
--
-- 0001 now builds gist_trgm_ops indexes (named `*_gist`) on the same columns;
-- gist_trgm_ops answers BOTH `%` and `<->`, so each search branch streams its
-- top-N straight out of the index (mirrors the MusicBrainz KNN migrations 0003 /
-- 0004). The old gin indexes are redundant and only add write/maintenance cost on
-- ingest, so they are dropped here. Forward-only and idempotent (safe to re-run).

DROP INDEX IF EXISTS imdb_title_basics_primary_title_trgm;
DROP INDEX IF EXISTS imdb_title_basics_original_title_trgm;
DROP INDEX IF EXISTS imdb_title_akas_title_trgm;
