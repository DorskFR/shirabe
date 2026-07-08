-- SHIB-22 / SHIB-23: full-text-search fast path for artist / release / recording
-- title search, accent-folded via unaccent.
--
-- 0001 created gin_trgm_ops indexes on the searched name columns. Trigram is the
-- right tool for FUZZY matching (typos, partials — kept as the fallback), but the
-- wrong primary filter for multi-word title search: a query like 'ok computer'
-- shares common 3-grams with tens of thousands of rows, all of which pg_trgm must
-- score (~350ms warm on release, a timeout on 36M-row recording, and a 4.5s cold
-- artist search for 'bjork').
--
-- Postgres FTS indexes whole words (lexemes) and intersects their posting lists,
-- so `to_tsvector(name) @@ websearch_to_tsquery(q)` reduces the query to the
-- handful of rows containing every word (0.4ms release, ~3ms recording). FTS
-- matches lexemes EXACTLY, so 'bjork' would miss 'Björk' — we fold accents on both
-- sides with an IMMUTABLE unaccent wrapper (usable in an index expression), so
-- 'bjork'/'royksopp'/'sigur ros' match 'Björk'/'Röyksopp'/'Sigur Rós'.
--
-- The gin_trgm_ops indexes from 0001 are KEPT: search_* fall back to a trigram `%`
-- scan when FTS returns nothing, so recall never regresses.
--
-- Idempotent (`CREATE … IF NOT EXISTS`, `CREATE OR REPLACE`, `DROP … IF EXISTS`),
-- applied only via an explicit `shirabe migrate musicbrainz` (never `migrate all`)
-- so the non-CONCURRENTLY builds are deliberate, operator-timed; on a mirror that
-- already carries the indexes (built CONCURRENTLY by hand) each statement no-ops.

CREATE EXTENSION IF NOT EXISTS unaccent;

-- unaccent()'s 2-arg form (regdictionary, text) is IMMUTABLE, so wrapping it lets
-- the result be used in an index expression (the 1-arg form is only STABLE).
CREATE OR REPLACE FUNCTION musicbrainz.f_unaccent(text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS
$$ SELECT public.unaccent('public.unaccent', $1) $$;

CREATE INDEX IF NOT EXISTS shirabe_release_name_fts_ua
    ON musicbrainz.release USING gin (to_tsvector('simple', musicbrainz.f_unaccent(name)));
CREATE INDEX IF NOT EXISTS shirabe_recording_name_fts_ua
    ON musicbrainz.recording USING gin (to_tsvector('simple', musicbrainz.f_unaccent(name)));
CREATE INDEX IF NOT EXISTS shirabe_artist_name_fts_ua
    ON musicbrainz.artist USING gin (to_tsvector('simple', musicbrainz.f_unaccent(name)));
CREATE INDEX IF NOT EXISTS shirabe_artist_sortname_fts_ua
    ON musicbrainz.artist USING gin (to_tsvector('simple', musicbrainz.f_unaccent(sort_name)));
CREATE INDEX IF NOT EXISTS shirabe_artist_alias_name_fts_ua
    ON musicbrainz.artist_alias USING gin (to_tsvector('simple', musicbrainz.f_unaccent(name)));

-- The plain-'simple' FTS indexes (no unaccent) shipped in v0.4.0 are superseded by
-- the _ua ones above; drop them so the mirror carries one FTS index per column.
DROP INDEX IF EXISTS musicbrainz.shirabe_release_name_fts;
DROP INDEX IF EXISTS musicbrainz.shirabe_recording_name_fts;
