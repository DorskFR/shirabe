-- shirabe search indexes.
--
-- These layer on top of a replicated MusicBrainz mirror (the `musicbrainz`
-- schema) that shirabe does NOT own. Everything here is idempotent and
-- additive: extensions + GIN trigram indexes for fuzzy name search, plus btree
-- FK indexes for the join paths the handlers walk. Safe to re-run; safe to drop
-- without touching replicated data.
--
-- pg_trgm + unaccent live in the default extension schema (usually `public`);
-- the functions (similarity(), unaccent()) resolve via search_path, so the DB
-- role shirabe connects with must have both extensions visible.

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- ── Trigram indexes for fuzzy search ───────────────────────
-- We search over unaccent(name); a plain gin_trgm_ops index on the raw column
-- still accelerates similarity() because the planner can use it as a candidate
-- filter, but to fully use the index for unaccent(...) we'd need an expression
-- index. unaccent() is only IMMUTABLE when schema-qualified, so we index the
-- raw columns (good enough: pg_trgm tolerates accents as extra trigrams) and
-- rely on the similarity threshold.

CREATE INDEX IF NOT EXISTS shirabe_artist_name_trgm
    ON musicbrainz.artist USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS shirabe_artist_sortname_trgm
    ON musicbrainz.artist USING gin (sort_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS shirabe_artist_alias_name_trgm
    ON musicbrainz.artist_alias USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS shirabe_release_name_trgm
    ON musicbrainz.release USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS shirabe_recording_name_trgm
    ON musicbrainz.recording USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS shirabe_artist_credit_name_trgm
    ON musicbrainz.artist_credit USING gin (name gin_trgm_ops);

-- ── Btree FK indexes for join paths ────────────────────────
-- The MusicBrainz schema already indexes many of these, but IF NOT EXISTS makes
-- this safe and self-documenting for the hot joins shirabe relies on.

CREATE INDEX IF NOT EXISTS shirabe_medium_release_idx
    ON musicbrainz.medium (release);
CREATE INDEX IF NOT EXISTS shirabe_track_medium_idx
    ON musicbrainz.track (medium);
CREATE INDEX IF NOT EXISTS shirabe_track_recording_idx
    ON musicbrainz.track (recording);
CREATE INDEX IF NOT EXISTS shirabe_release_release_group_idx
    ON musicbrainz.release (release_group);
CREATE INDEX IF NOT EXISTS shirabe_artist_credit_name_ac_idx
    ON musicbrainz.artist_credit_name (artist_credit);
CREATE INDEX IF NOT EXISTS shirabe_artist_alias_artist_idx
    ON musicbrainz.artist_alias (artist);
CREATE INDEX IF NOT EXISTS shirabe_release_country_release_idx
    ON musicbrainz.release_country (release);
CREATE INDEX IF NOT EXISTS shirabe_release_unknown_country_release_idx
    ON musicbrainz.release_unknown_country (release);
CREATE INDEX IF NOT EXISTS shirabe_l_release_release_e0_idx
    ON musicbrainz.l_release_release (entity0);
CREATE INDEX IF NOT EXISTS shirabe_l_release_release_e1_idx
    ON musicbrainz.l_release_release (entity1);
