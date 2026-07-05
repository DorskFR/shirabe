-- SHIB-18: KNN GiST top-N trigram ranking for the search endpoints.
--
-- 0001 created gin_trgm_ops indexes on the searched name columns. GIN cannot
-- return rows in similarity order, so `... WHERE name % $1 ORDER BY
-- similarity(name,$1) DESC LIMIT n` forced Postgres to materialise every `%`
-- candidate, compute similarity(), full-sort, and discard all but LIMIT.
--
-- gist_trgm_ops supports the KNN distance operator `<->` (defined as
-- `1 - similarity`), so `ORDER BY name <-> $1 LIMIT n` streams the top-N straight
-- out of the index. It also answers the `%` containment operator, so these GiST
-- indexes fully replace the gin_trgm_ops indexes on the same columns; those GIN
-- indexes are dropped here to avoid double index maintenance on the replicated
-- mirror (same additive/idempotent, safe-to-re-run, safe-to-drop contract as
-- 0001 / 0002).

CREATE INDEX IF NOT EXISTS shirabe_artist_name_trgm_gist
    ON musicbrainz.artist USING gist (name gist_trgm_ops);
CREATE INDEX IF NOT EXISTS shirabe_artist_sortname_trgm_gist
    ON musicbrainz.artist USING gist (sort_name gist_trgm_ops);
CREATE INDEX IF NOT EXISTS shirabe_release_name_trgm_gist
    ON musicbrainz.release USING gist (name gist_trgm_ops);
CREATE INDEX IF NOT EXISTS shirabe_recording_name_trgm_gist
    ON musicbrainz.recording USING gist (name gist_trgm_ops);
CREATE INDEX IF NOT EXISTS shirabe_artist_credit_name_trgm_gist
    ON musicbrainz.artist_credit USING gist (name gist_trgm_ops);

DROP INDEX IF EXISTS musicbrainz.shirabe_artist_name_trgm;
DROP INDEX IF EXISTS musicbrainz.shirabe_artist_sortname_trgm;
DROP INDEX IF EXISTS musicbrainz.shirabe_release_name_trgm;
DROP INDEX IF EXISTS musicbrainz.shirabe_recording_name_trgm;
DROP INDEX IF EXISTS musicbrainz.shirabe_artist_credit_name_trgm;
