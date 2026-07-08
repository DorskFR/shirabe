-- SHIB-22: full-text-search fast path for release / recording title search.
--
-- 0001 created gin_trgm_ops indexes on the searched name columns. Trigram is the
-- right tool for FUZZY matching, but the wrong primary filter for multi-word
-- title search: a query like 'ok computer' shares common 3-grams with tens of
-- thousands of rows, all of which pg_trgm must score — ~350ms warm on release
-- (5M rows), and a statement-timeout on recording (36M rows).
--
-- Postgres FTS indexes whole words (lexemes) and intersects their posting lists,
-- so `to_tsvector(name) @@ websearch_to_tsquery(q)` reduces 'ok computer' to the
-- ~dozens of rows containing BOTH words. Ranking those few by similarity() is
-- then free: measured 0.4ms warm on release vs ~350ms for the trigram path, on a
-- SMALLER index (release FTS 127MB vs trigram GIN 201MB / GiST 534MB).
--
-- The gin_trgm_ops indexes from 0001 are KEPT: search_releases / search_recordings
-- fall back to a trigram `%` + similarity() scan when FTS returns nothing (a
-- typo'd / partial query that matches no whole lexeme), so recall never regresses.
--
-- Same additive/idempotent, safe-to-re-run contract as 0001 / 0002. Applied only
-- via an explicit `shirabe migrate musicbrainz` (never `migrate all`), so this
-- non-CONCURRENTLY build is a deliberate, operator-timed action; on a mirror that
-- already carries the index (built CONCURRENTLY by hand) it no-ops.

CREATE INDEX IF NOT EXISTS shirabe_release_name_fts
    ON musicbrainz.release USING gin (to_tsvector('simple', name));
CREATE INDEX IF NOT EXISTS shirabe_recording_name_fts
    ON musicbrainz.recording USING gin (to_tsvector('simple', name));
