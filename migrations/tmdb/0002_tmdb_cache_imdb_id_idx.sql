-- TMDB cache: imdb_id cross-ref lookup index (SHIB-15).
--
-- Local search resolves strong IMDb-tconst hits (from the akas trigram probe,
-- e.g. "gintama" → tt0988818) to their TMDB ids by looking up previously
-- hydrated detail payloads in `tmdb_cache` — a tv/movie detail carries
-- `external_ids.imdb_id` (and movies also a top-level `imdb_id`). This
-- expression index makes that reverse lookup (kind + coalesced imdb_id) an
-- index scan instead of a jsonb table scan.
--
-- Forward-only and idempotent (CREATE INDEX IF NOT EXISTS): safe to re-run,
-- never edited once applied. Additive — drops cleanly without data loss.

CREATE INDEX IF NOT EXISTS tmdb_cache_kind_imdb_id_idx
    ON tmdb_cache (
        kind,
        (COALESCE(payload -> 'external_ids' ->> 'imdb_id', payload ->> 'imdb_id'))
    );
