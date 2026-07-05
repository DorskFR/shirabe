-- SHIB-20: fold artist_alias.name into the KNN artist search.
--
-- 0001 created a gin_trgm_ops index on artist_alias.name, but search_artists
-- only trigram-matched artist.name / artist.sort_name — aliases were loaded
-- per-row by FK afterward, never trigram-searched. Alias-only name matches (the
-- common MusicBrainz recall case: localised / alternate names) were therefore
-- missed. search_artists now adds a third KNN scan over artist_alias.name, which
-- needs the KNN distance operator `<->` — only gist_trgm_ops answers it (GIN
-- cannot). So we add the GiST index and drop the now-superseded GIN index on the
-- same column (gist_trgm_ops also answers the `%` containment operator, so the
-- GIN index is redundant — same additive/idempotent, safe-to-re-run,
-- safe-to-drop contract as 0001 / 0002 / 0003).

CREATE INDEX IF NOT EXISTS shirabe_artist_alias_name_trgm_gist
    ON musicbrainz.artist_alias USING gist (name gist_trgm_ops);

DROP INDEX IF EXISTS musicbrainz.shirabe_artist_alias_name_trgm;
