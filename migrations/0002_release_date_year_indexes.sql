-- Speed up the date-filtered release search (`date:(YYYY*)`).
--
-- repo::search_releases filters the year via a correlated EXISTS:
--     EXISTS (SELECT 1 FROM release_country rc
--             WHERE rc.release = r.id AND rc.date_year = $3)
--   UNION ALL ... release_unknown_country ruc WHERE ruc.release = r.id AND ...
--
-- 0001 only indexed these tables on (release), so each EXISTS fetched every date
-- event for the release and filtered date_year in memory — the dominant cost in
-- the ~2s date-filtered query seen in production logs. A composite
-- (release, date_year) index turns each EXISTS into a single index probe and
-- also covers the plain (release) join path, so the old single-column indexes
-- from 0001 are redundant and dropped here to cut write/replication overhead.
--
-- Same idempotent/additive contract as 0001: layered on a mirror shirabe does
-- not own, safe to re-run, safe to drop without touching replicated data.

CREATE INDEX IF NOT EXISTS shirabe_release_country_rel_year_idx
    ON musicbrainz.release_country (release, date_year);
CREATE INDEX IF NOT EXISTS shirabe_release_unknown_country_rel_year_idx
    ON musicbrainz.release_unknown_country (release, date_year);

DROP INDEX IF EXISTS musicbrainz.shirabe_release_country_release_idx;
DROP INDEX IF EXISTS musicbrainz.shirabe_release_unknown_country_release_idx;
