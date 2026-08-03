//! The single source of truth for every SQL statement shirabe runs.
//!
//! Each statement lives here as a `pub const` string. `src/repo.rs` (MusicBrainz
//! mirror) and `src/search.rs` (imdb/tmdb DBs) reference these consts in their
//! `sqlx::query(...)` calls instead of inlining the SQL, so the debug query
//! explorer ([`crate::debug_ui`]) renders and runs the *exact* text the handlers
//! execute — the catalog can never drift from the code.
//!
//! [`catalog`] additionally attaches UI metadata (target DB, ordered params with
//! example values, whether the query needs a trigram search session) so the page
//! is runnable out of the box.

// ── SQL statements (shared with repo.rs / search.rs) ──────────

// MusicBrainz mirror — search entry points
//
// SHIB-23: FTS + f_unaccent fast path (whole-word, accent-folded so 'bjork'
// matches 'Björk'), ranked by unaccented similarity(). Three UNION branches keep
// the wave-2 recall: artist.name, artist.sort_name, and artist_alias.name (a
// localised/alternate name is a common MB recall case), de-duped by id with the
// MAX score. Trigram `%` fallback (SEARCH_ARTISTS_FUZZY) runs only when FTS
// matches nothing (typo'd / partial query).
pub const SEARCH_ARTISTS: &str = r"
        SELECT c.id, c.gid, c.name, MAX(c.score) AS score
        FROM (
            ( SELECT a.id, a.gid, a.name,
                     GREATEST(similarity(musicbrainz.f_unaccent(a.name), musicbrainz.f_unaccent($1)),
                              similarity(musicbrainz.f_unaccent(a.sort_name), musicbrainz.f_unaccent($1))) AS score
              FROM musicbrainz.artist a
              WHERE to_tsvector('simple', musicbrainz.f_unaccent(a.name))
                    @@ websearch_to_tsquery('simple', musicbrainz.f_unaccent($1)) )
            UNION ALL
            ( SELECT a.id, a.gid, a.name,
                     GREATEST(similarity(musicbrainz.f_unaccent(a.name), musicbrainz.f_unaccent($1)),
                              similarity(musicbrainz.f_unaccent(a.sort_name), musicbrainz.f_unaccent($1))) AS score
              FROM musicbrainz.artist a
              WHERE to_tsvector('simple', musicbrainz.f_unaccent(a.sort_name))
                    @@ websearch_to_tsquery('simple', musicbrainz.f_unaccent($1)) )
            UNION ALL
            ( SELECT a.id, a.gid, a.name,
                     similarity(musicbrainz.f_unaccent(aa.name), musicbrainz.f_unaccent($1)) AS score
              FROM musicbrainz.artist_alias aa
              JOIN musicbrainz.artist a ON a.id = aa.artist
              WHERE to_tsvector('simple', musicbrainz.f_unaccent(aa.name))
                    @@ websearch_to_tsquery('simple', musicbrainz.f_unaccent($1)) )
        ) c
        GROUP BY c.id, c.gid, c.name
        ORDER BY score DESC, c.id ASC
        LIMIT $2
        ";

// Trigram fallback for SEARCH_ARTISTS: used only when FTS matches nothing. The
// per-branch KNN `<->`/`%` bounds each candidate set to $2 (gin_trgm_ops); trigram
// is naturally accent-tolerant, so no f_unaccent here.
pub const SEARCH_ARTISTS_FUZZY: &str = r"
        SELECT c.id, c.gid, c.name, MAX(c.score) AS score
        FROM (
            ( SELECT a.id, a.gid, a.name,
                     GREATEST(similarity(a.name, $1), similarity(a.sort_name, $1)) AS score
              FROM musicbrainz.artist a
              WHERE a.name % $1
              ORDER BY a.name <-> $1
              LIMIT $2 )
            UNION ALL
            ( SELECT a.id, a.gid, a.name,
                     GREATEST(similarity(a.name, $1), similarity(a.sort_name, $1)) AS score
              FROM musicbrainz.artist a
              WHERE a.sort_name % $1
              ORDER BY a.sort_name <-> $1
              LIMIT $2 )
            UNION ALL
            ( SELECT a.id, a.gid, a.name, similarity(aa.name, $1) AS score
              FROM musicbrainz.artist_alias aa
              JOIN musicbrainz.artist a ON a.id = aa.artist
              WHERE aa.name % $1
              ORDER BY aa.name <-> $1
              LIMIT $2 )
        ) c
        GROUP BY c.id, c.gid, c.name
        ORDER BY score DESC, c.id ASC
        LIMIT $2
        ";

// SHIB-23: FTS + f_unaccent fast path. `to_tsvector(f_unaccent(name)) @@
// websearch_to_tsquery(f_unaccent(q))` reduces a multi-word title to the handful
// of rows containing every word (accent-folded, so 'sigur ros' matches 'Sigur
// Rós'), ranked by similarity(). The artist_credit table is touched ONLY when an
// artist filter ($2) is given — via EXISTS for the filter and a scalar subquery
// for the ranking bonus — so a title-only query never joins artist_credit for its
// whole candidate set (that join was O(candidates): 8s for 'over the rainbow').
pub const SEARCH_RELEASES: &str = r"
        SELECT c.id, c.gid, c.name, c.artist_credit, c.release_group,
               similarity(musicbrainz.f_unaccent(c.name), musicbrainz.f_unaccent($1))::real AS title_score
        FROM (
            SELECT r.id, r.gid, r.name, r.artist_credit, r.release_group
            FROM musicbrainz.release r
            WHERE to_tsvector('simple', musicbrainz.f_unaccent(r.name))
                  @@ websearch_to_tsquery('simple', musicbrainz.f_unaccent($1))
              AND ($2::text IS NULL OR EXISTS (
                    SELECT 1 FROM musicbrainz.artist_credit ac
                    WHERE ac.id = r.artist_credit AND ac.name % $2))
              AND ($3::int IS NULL OR EXISTS (
                    SELECT 1 FROM musicbrainz.release_country rc
                    WHERE rc.release = r.id AND rc.date_year = $3
                    UNION ALL
                    SELECT 1 FROM musicbrainz.release_unknown_country ruc
                    WHERE ruc.release = r.id AND ruc.date_year = $3))
        ) c
        ORDER BY (similarity(musicbrainz.f_unaccent(c.name), musicbrainz.f_unaccent($1))
                  + CASE WHEN $2::text IS NULL THEN 0::real
                         ELSE COALESCE((SELECT similarity(ac.name, $2)
                                        FROM musicbrainz.artist_credit ac
                                        WHERE ac.id = c.artist_credit), 0::real) * 0.5::real END) DESC,
                 c.id ASC
        LIMIT $4
        ";

// Trigram fallback for SEARCH_RELEASES: used only when FTS matches nothing (a
// typo'd / partial query with no whole-word lexeme hit). Uses the gin_trgm_ops `%`
// filter — slower, but the rare path — so recall never regresses vs pure FTS.
pub const SEARCH_RELEASES_FUZZY: &str = r"
        SELECT c.id, c.gid, c.name, c.artist_credit, c.release_group,
               similarity(c.name, $1)::real AS title_score
        FROM (
            SELECT r.id, r.gid, r.name, r.artist_credit, r.release_group
            FROM musicbrainz.release r
            WHERE r.name % $1
              AND ($2::text IS NULL OR EXISTS (
                    SELECT 1 FROM musicbrainz.artist_credit ac
                    WHERE ac.id = r.artist_credit AND ac.name % $2))
              AND ($3::int IS NULL OR EXISTS (
                    SELECT 1 FROM musicbrainz.release_country rc
                    WHERE rc.release = r.id AND rc.date_year = $3
                    UNION ALL
                    SELECT 1 FROM musicbrainz.release_unknown_country ruc
                    WHERE ruc.release = r.id AND ruc.date_year = $3))
        ) c
        ORDER BY (similarity(c.name, $1)
                  + CASE WHEN $2::text IS NULL THEN 0::real
                         ELSE COALESCE((SELECT similarity(ac.name, $2)
                                        FROM musicbrainz.artist_credit ac
                                        WHERE ac.id = c.artist_credit), 0::real) * 0.5::real END) DESC,
                 c.id ASC
        LIMIT $4
        ";

pub const SEARCH_RECORDINGS: &str = r"
        SELECT c.id, c.gid, c.name, c.length, c.artist_credit,
               similarity(musicbrainz.f_unaccent(c.name), musicbrainz.f_unaccent($1))::real AS title_score
        FROM (
            SELECT rec.id, rec.gid, rec.name, rec.length, rec.artist_credit
            FROM musicbrainz.recording rec
            WHERE to_tsvector('simple', musicbrainz.f_unaccent(rec.name))
                  @@ websearch_to_tsquery('simple', musicbrainz.f_unaccent($1))
              AND ($2::text IS NULL OR EXISTS (
                    SELECT 1 FROM musicbrainz.artist_credit ac
                    WHERE ac.id = rec.artist_credit AND ac.name % $2))
        ) c
        ORDER BY (similarity(musicbrainz.f_unaccent(c.name), musicbrainz.f_unaccent($1))
                  + CASE WHEN $2::text IS NULL THEN 0::real
                         ELSE COALESCE((SELECT similarity(ac.name, $2)
                                        FROM musicbrainz.artist_credit ac
                                        WHERE ac.id = c.artist_credit), 0::real) * 0.5::real END) DESC,
                 c.id ASC
        LIMIT $3
        ";

// Trigram fallback for SEARCH_RECORDINGS (see SEARCH_RELEASES_FUZZY).
pub const SEARCH_RECORDINGS_FUZZY: &str = r"
        SELECT c.id, c.gid, c.name, c.length, c.artist_credit,
               similarity(c.name, $1)::real AS title_score
        FROM (
            SELECT rec.id, rec.gid, rec.name, rec.length, rec.artist_credit
            FROM musicbrainz.recording rec
            WHERE rec.name % $1
              AND ($2::text IS NULL OR EXISTS (
                    SELECT 1 FROM musicbrainz.artist_credit ac
                    WHERE ac.id = rec.artist_credit AND ac.name % $2))
        ) c
        ORDER BY (similarity(c.name, $1)
                  + CASE WHEN $2::text IS NULL THEN 0::real
                         ELSE COALESCE((SELECT similarity(ac.name, $2)
                                        FROM musicbrainz.artist_credit ac
                                        WHERE ac.id = c.artist_credit), 0::real) * 0.5::real END) DESC,
                 c.id ASC
        LIMIT $3
        ";

// MusicBrainz mirror — release-group browse
pub const BROWSE_RELEASE_GROUPS_COUNT: &str = r"
        SELECT COUNT(*)::bigint AS total
        FROM musicbrainz.release_group rg
        WHERE EXISTS (
            SELECT 1 FROM musicbrainz.artist_credit_name acn
            JOIN musicbrainz.artist a ON a.id = acn.artist
            WHERE acn.artist_credit = rg.artist_credit AND a.gid = $1)
        ";

pub const BROWSE_RELEASE_GROUPS: &str = r"
        SELECT rg.id, rg.gid, rg.name, rg.comment, rg.artist_credit,
               rgpt.name AS primary_type,
               rgm.first_release_date_year::int AS y,
               rgm.first_release_date_month::int AS m,
               rgm.first_release_date_day::int AS d
        FROM musicbrainz.release_group rg
        LEFT JOIN musicbrainz.release_group_primary_type rgpt ON rgpt.id = rg.type
        LEFT JOIN musicbrainz.release_group_meta rgm ON rgm.id = rg.id
        WHERE EXISTS (
            SELECT 1 FROM musicbrainz.artist_credit_name acn
            JOIN musicbrainz.artist a ON a.id = acn.artist
            WHERE acn.artist_credit = rg.artist_credit AND a.gid = $1)
        ORDER BY rgm.first_release_date_year ASC NULLS LAST,
                 rgm.first_release_date_month ASC NULLS LAST,
                 rgm.first_release_date_day ASC NULLS LAST,
                 rg.gid ASC
        LIMIT $2 OFFSET $3
        ";

// MusicBrainz mirror — lookups
pub const LOOKUP_ARTIST: &str = r"
        SELECT a.id, a.gid, a.name, a.sort_name, a.comment, at.name AS type_name
        FROM musicbrainz.artist a
        LEFT JOIN musicbrainz.artist_type at ON at.id = a.type
        WHERE a.gid = $1
        ";

pub const LOOKUP_RELEASE: &str = r"
        SELECT r.id, r.gid, r.name, r.artist_credit, r.release_group
        FROM musicbrainz.release r
        WHERE r.gid = $1
        ";

pub const LOOKUP_RELEASE_GROUP: &str = r"
        SELECT rg.id, rg.gid, rg.name, rg.comment, rg.artist_credit,
               rgpt.name AS primary_type,
               rgm.first_release_date_year::int AS y,
               rgm.first_release_date_month::int AS m,
               rgm.first_release_date_day::int AS d
        FROM musicbrainz.release_group rg
        LEFT JOIN musicbrainz.release_group_primary_type rgpt ON rgpt.id = rg.type
        LEFT JOIN musicbrainz.release_group_meta rgm ON rgm.id = rg.id
        WHERE rg.gid = $1
        ";

pub const LOOKUP_RECORDING: &str = r"
        SELECT rec.id, rec.gid, rec.name, rec.length, rec.artist_credit
        FROM musicbrainz.recording rec
        WHERE rec.gid = $1
        ";

// MusicBrainz mirror — batched hydration
pub const BATCH_ARTIST_ALIASES: &str = r"
        SELECT artist, name, sort_name
        FROM musicbrainz.artist_alias
        WHERE artist = ANY($1)
        ORDER BY artist, id ASC
        ";

pub const BATCH_ARTIST_CREDITS: &str = r"
        SELECT acn.artist_credit AS ac_id, a.id AS artist_id, a.gid AS artist_gid,
               acn.name AS credit_name
        FROM musicbrainz.artist_credit_name acn
        JOIN musicbrainz.artist a ON a.id = acn.artist
        WHERE acn.artist_credit = ANY($1)
        ORDER BY acn.artist_credit, acn.position ASC
        ";

pub const BATCH_RELEASE_GROUPS: &str = r"
        SELECT rg.id, rg.gid, rgpt.name AS primary_type
        FROM musicbrainz.release_group rg
        LEFT JOIN musicbrainz.release_group_primary_type rgpt ON rgpt.id = rg.type
        WHERE rg.id = ANY($1)
        ";

pub const BATCH_RELEASE_GROUP_SECONDARY_TYPES: &str = r"
        SELECT j.release_group AS rg, st.name
        FROM musicbrainz.release_group_secondary_type_join j
        JOIN musicbrainz.release_group_secondary_type st ON st.id = j.secondary_type
        WHERE j.release_group = ANY($1)
        ORDER BY j.release_group, st.name ASC
        ";

pub const BATCH_RELEASE_DATES: &str = r"
        SELECT rc.release AS rel, rc.date_year::int AS y, rc.date_month::int AS m,
               rc.date_day::int AS d, (iso.code = 'XW') AS is_xw
        FROM musicbrainz.release_country rc
        LEFT JOIN musicbrainz.iso_3166_1 iso ON iso.area = rc.country
        WHERE rc.release = ANY($1)
        UNION ALL
        SELECT release, date_year::int, date_month::int, date_day::int, false
        FROM musicbrainz.release_unknown_country
        WHERE release = ANY($1)
        ";

pub const BATCH_RELEASE_STATUSES: &str = r"
        SELECT r.id, rs.name
        FROM musicbrainz.release r
        JOIN musicbrainz.release_status rs ON rs.id = r.status
        WHERE r.id = ANY($1)
        ";

pub const BATCH_RELEASE_COMMENTS: &str =
    "SELECT id, comment FROM musicbrainz.release WHERE id = ANY($1)";

pub const BATCH_RELEASE_TRACK_COUNTS: &str = r"
        SELECT release AS rel, COALESCE(SUM(track_count), 0)::bigint AS total
        FROM musicbrainz.medium
        WHERE release = ANY($1)
        GROUP BY release
        ";

pub const BATCH_TRACKS: &str = r"
        SELECT t.medium AS mid, t.gid AS track_gid, t.name AS track_name, t.position,
               t.number, t.artist_credit AS track_ac,
               rec.gid AS rec_gid, rec.name AS rec_name, rec.length AS rec_length
        FROM musicbrainz.track t
        JOIN musicbrainz.recording rec ON rec.id = t.recording
        WHERE t.medium = ANY($1)
        ORDER BY t.medium, t.position ASC
        ";

pub const BATCH_MEDIA: &str = r"
        SELECT m.release AS rel, m.id, m.position, m.track_count, m.name AS title,
               mf.name AS format
        FROM musicbrainz.medium m
        LEFT JOIN musicbrainz.medium_format mf ON mf.id = m.format
        WHERE m.release = ANY($1)
        ORDER BY m.release, m.position ASC
        ";

pub const BATCH_RECORDING_RELEASES: &str = r"
        SELECT DISTINCT t.recording AS rec, r.id, r.gid, r.name, r.artist_credit,
               r.release_group
        FROM musicbrainz.release r
        JOIN musicbrainz.medium m ON m.release = r.id
        JOIN musicbrainz.track t ON t.medium = m.id
        WHERE t.recording = ANY($1)
        ORDER BY t.recording, r.id ASC
        ";

// MusicBrainz mirror — per-row loaders (lookup path)
pub const LOAD_ARTIST_URL_RELATIONS: &str = r"
        SELECT lt.name AS rel_type, u.url AS resource
        FROM musicbrainz.l_artist_url l
        JOIN musicbrainz.link lk ON lk.id = l.link
        JOIN musicbrainz.link_type lt ON lt.id = lk.link_type
        JOIN musicbrainz.url u ON u.id = l.entity1
        WHERE l.entity0 = $1
        ORDER BY l.id ASC
        ";

pub const LOAD_ARTIST_ALIASES: &str = r"
        SELECT name, sort_name
        FROM musicbrainz.artist_alias
        WHERE artist = $1
        ORDER BY id ASC
        ";

pub const LOAD_ARTIST_CREDIT: &str = r"
        SELECT a.id AS artist_id, a.gid AS artist_gid, acn.name AS credit_name
        FROM musicbrainz.artist_credit_name acn
        JOIN musicbrainz.artist a ON a.id = acn.artist
        WHERE acn.artist_credit = $1
        ORDER BY acn.position ASC
        ";

pub const RELEASE_DATE: &str = r"
        SELECT rc.date_year::int AS y, rc.date_month::int AS m, rc.date_day::int AS d,
               (iso.code = 'XW') AS is_xw
        FROM musicbrainz.release_country rc
        LEFT JOIN musicbrainz.iso_3166_1 iso ON iso.area = rc.country
        WHERE rc.release = $1
        UNION ALL
        SELECT date_year::int, date_month::int, date_day::int, false
        FROM musicbrainz.release_unknown_country
        WHERE release = $1
        ";

pub const LOAD_RELEASE_GROUP: &str = r"
        SELECT rg.gid, rgpt.name AS primary_type
        FROM musicbrainz.release_group rg
        LEFT JOIN musicbrainz.release_group_primary_type rgpt ON rgpt.id = rg.type
        WHERE rg.id = $1
        ";

pub const LOAD_RELEASE_GROUP_RELEASES: &str = r"
        SELECT r.id, r.gid, rs.name AS status
        FROM musicbrainz.release r
        LEFT JOIN musicbrainz.release_status rs ON rs.id = r.status
        WHERE r.release_group = $1
        ORDER BY r.id ASC
        ";

pub const LOAD_RELEASE_STATUS: &str = r"
        SELECT rs.name
        FROM musicbrainz.release r
        JOIN musicbrainz.release_status rs ON rs.id = r.status
        WHERE r.id = $1
        ";

pub const LOAD_RELEASE_COMMENT: &str = "SELECT comment FROM musicbrainz.release WHERE id = $1";

pub const LOAD_MEDIA: &str = r"
        SELECT m.id, m.position, m.track_count, m.name AS title, mf.name AS format
        FROM musicbrainz.medium m
        LEFT JOIN musicbrainz.medium_format mf ON mf.id = m.format
        WHERE m.release = $1
        ORDER BY m.position ASC
        ";

pub const LOAD_TRACKS: &str = r"
        SELECT t.gid AS track_gid, t.name AS track_name, t.position, t.number,
               t.artist_credit AS track_ac,
               rec.gid AS rec_gid, rec.name AS rec_name, rec.length AS rec_length
        FROM musicbrainz.track t
        JOIN musicbrainz.recording rec ON rec.id = t.recording
        WHERE t.medium = $1
        ORDER BY t.position ASC
        ";

pub const LOAD_RELEASE_RELATIONS: &str = r"
        SELECT 'forward' AS direction, r1.gid AS gid, r1.name AS name
        FROM musicbrainz.l_release_release l
        JOIN musicbrainz.release r1 ON r1.id = l.entity1
        WHERE l.entity0 = $1
        UNION ALL
        SELECT 'backward' AS direction, r0.gid AS gid, r0.name AS name
        FROM musicbrainz.l_release_release l
        JOIN musicbrainz.release r0 ON r0.id = l.entity0
        WHERE l.entity1 = $1
        ";

pub const PING: &str = "SELECT 1";

// tmdb DB
//
// SHIB-24: FTS + f_unaccent fast path (whole-word, accent-folded). Trigram `%` on
// the 1.4M-row id index shares common 3-grams with a large candidate slice for a
// short query like 'dune' (~340ms, scoring 30k rows); FTS intersects lexeme
// posting lists down to the rows that actually contain the word (~2ms). Falls back
// to the trigram version (SEARCH_TMDB_ID_INDEX_FUZZY) only when FTS matches none.
pub const SEARCH_TMDB_ID_INDEX: &str = r"
        SELECT id, name, popularity, adult,
               similarity(public.f_unaccent(name), public.f_unaccent($1)) AS score
        FROM tmdb_id_index
        WHERE kind = $2
          AND to_tsvector('simple', public.f_unaccent(name))
              @@ websearch_to_tsquery('simple', public.f_unaccent($1))
        ORDER BY score DESC, popularity DESC NULLS LAST, id ASC
        LIMIT $3
        ";

// Trigram fuzzy fallback for TMDB id-index search — used only when the FTS primary
// matched nothing (typos / partials). Bounded by the session statement_timeout.
pub const SEARCH_TMDB_ID_INDEX_FUZZY: &str = r"
        SELECT id, name, popularity, adult,
               similarity(name, $1) AS score
        FROM tmdb_id_index
        WHERE kind = $2 AND name % $1
        ORDER BY score DESC, popularity DESC NULLS LAST, id ASC
        LIMIT $3
        ";

pub const RESOLVE_TCONSTS_VIA_CACHE: &str = r"
        SELECT COALESCE(payload -> 'external_ids' ->> 'imdb_id', payload ->> 'imdb_id') AS imdb_id,
               id
        FROM tmdb_cache
        WHERE kind = $1
          AND COALESCE(payload -> 'external_ids' ->> 'imdb_id', payload ->> 'imdb_id') = ANY($2)
        ";

pub const INDEX_META_FOR_IDS: &str =
    "SELECT id, name, popularity, adult FROM tmdb_id_index WHERE kind = $1 AND id = ANY($2)";

// imdb DB
//
pub const SEARCH_IMDB_TITLES: &str = r"
        WITH matched AS (
            SELECT tconst, title_ua, num_votes
            FROM imdb_search_titles
            WHERE tsv @@ websearch_to_tsquery('simple', public.f_unaccent($1))
              AND (cardinality($3::text[]) = 0 OR title_type = ANY($3))
            ORDER BY num_votes DESC
            LIMIT 500
        ),
        ranked AS (
            SELECT tconst,
                   max(similarity(title_ua, public.f_unaccent($1))) AS score
            FROM matched
            GROUP BY tconst
        )
        SELECT r.tconst,
               r.score,
               b.primary_title AS name
        FROM ranked r
        JOIN imdb_title_basics b ON b.tconst = r.tconst
        ORDER BY r.score DESC, r.tconst ASC
        LIMIT $2
        ";

// Trigram fuzzy fallback for IMDb title search — used only when the FTS primary
// matched nothing (typos / partials). Each branch streams its top-N straight out
// of a `gist_trgm_ops` index via the KNN `<->` operator; bounded by the session
// statement_timeout (a pathological common-token fallback yields no rows rather
// than erroring — the FTS primary already returned none).
pub const SEARCH_IMDB_TITLES_FUZZY: &str = r"
        WITH primary_hit AS (
            SELECT b.tconst, similarity(b.primary_title, $1) AS s
            FROM imdb_title_basics b
            WHERE b.primary_title % $1
              AND (cardinality($3::text[]) = 0 OR b.title_type = ANY($3))
            ORDER BY b.primary_title <-> $1
            LIMIT $2
        ),
        original_hit AS (
            SELECT b.tconst, similarity(coalesce(b.original_title, ''), $1) AS s
            FROM imdb_title_basics b
            WHERE b.original_title % $1
              AND (cardinality($3::text[]) = 0 OR b.title_type = ANY($3))
            ORDER BY b.original_title <-> $1
            LIMIT $2
        ),
        akas_hit AS (
            SELECT a.title_id AS tconst, similarity(a.title, $1) AS s
            FROM imdb_title_akas a
            WHERE a.title % $1
            ORDER BY a.title <-> $1
            LIMIT $2
        ),
        unioned AS (
            SELECT tconst, s FROM primary_hit
            UNION ALL
            SELECT tconst, s FROM original_hit
            UNION ALL
            SELECT tconst, s FROM akas_hit
        )
        SELECT u.tconst,
               max(u.s) AS score,
               b.primary_title AS name
        FROM unioned u
        JOIN imdb_title_basics b ON b.tconst = u.tconst
        WHERE (cardinality($3::text[]) = 0 OR b.title_type = ANY($3))
        GROUP BY u.tconst, b.primary_title
        ORDER BY score DESC, u.tconst ASC
        LIMIT $2
        ";

// ── Catalog metadata for the debug UI ─────────────────────────

/// Which Postgres database a query runs against. Maps to a pool on
/// [`crate::db::Pools`]; `Imdb`/`Tmdb` may be unconfigured (`None`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetDb {
    Musicbrainz,
    Imdb,
    Tmdb,
}

/// The Postgres type a bind param is decoded as, so the runner can parse the
/// UI-supplied string into the right Rust type before binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamType {
    Text,
    Int,
    BigInt,
    Uuid,
    IntArray,
    BigIntArray,
    TextArray,
}

/// One `$n` bind parameter, in positional order, with a prefilled example.
#[derive(Debug, Clone, Copy)]
pub struct Param {
    pub name: &'static str,
    pub ty: ParamType,
    /// When true, a blank UI field binds SQL `NULL` instead of a parsed value.
    pub nullable: bool,
    /// Example value (as typed in the UI) so the page is runnable out of the box.
    /// Arrays are entered comma-separated (e.g. `movie,tvMovie`).
    pub example: &'static str,
}

/// A runnable query: the exact SQL a handler executes, plus UI metadata.
#[derive(Debug, Clone)]
pub struct QuerySpec {
    pub id: &'static str,
    pub title: &'static str,
    pub endpoint: &'static str,
    pub db: TargetDb,
    /// Whether to apply a trigram search session (`set_limit` + `work_mem`)
    /// before running — matches how the handler runs it.
    pub trigram: bool,
    pub sql: &'static str,
    pub params: Vec<Param>,
}

use ParamType::{BigInt, BigIntArray, Int, IntArray, Text, TextArray, Uuid};

const fn p(name: &'static str, ty: ParamType, example: &'static str) -> Param {
    Param { name, ty, nullable: false, example }
}
const fn pn(name: &'static str, ty: ParamType, example: &'static str) -> Param {
    Param { name, ty, nullable: true, example }
}

/// Every SQL statement shirabe runs, with UI metadata. The `sql` field points at
/// the same const the handler executes, so this list cannot drift from the code.
#[must_use]
#[allow(clippy::too_many_lines)] // a flat data table — splitting it hurts readability
pub fn catalog() -> Vec<QuerySpec> {
    vec![
        // ── search (the interesting, trigram-driven ones) ──
        QuerySpec {
            id: "search_artists",
            title: "Artist search (FTS)",
            endpoint: "GET /ws/2/artist",
            db: TargetDb::Musicbrainz,
            trigram: true,
            sql: SEARCH_ARTISTS,
            params: vec![p("name", Text, "björk"), p("limit", BigInt, "25")],
        },
        QuerySpec {
            id: "search_artists_fuzzy",
            title: "Artist search (trigram fallback)",
            endpoint: "GET /ws/2/artist (fallback)",
            db: TargetDb::Musicbrainz,
            trigram: true,
            sql: SEARCH_ARTISTS_FUZZY,
            params: vec![p("name", Text, "björk"), p("limit", BigInt, "25")],
        },
        QuerySpec {
            id: "search_releases",
            title: "Release search (FTS)",
            endpoint: "GET /ws/2/release",
            db: TargetDb::Musicbrainz,
            trigram: true,
            sql: SEARCH_RELEASES,
            params: vec![
                p("title", Text, "ok computer"),
                pn("artist", Text, ""),
                pn("year", Int, ""),
                p("limit", BigInt, "25"),
            ],
        },
        QuerySpec {
            id: "search_releases_fuzzy",
            title: "Release search (trigram fallback)",
            endpoint: "GET /ws/2/release (fallback)",
            db: TargetDb::Musicbrainz,
            trigram: true,
            sql: SEARCH_RELEASES_FUZZY,
            params: vec![
                p("title", Text, "ok computer"),
                pn("artist", Text, ""),
                pn("year", Int, ""),
                p("limit", BigInt, "25"),
            ],
        },
        QuerySpec {
            id: "search_recordings",
            title: "Recording search (FTS)",
            endpoint: "GET /ws/2/recording",
            db: TargetDb::Musicbrainz,
            trigram: true,
            sql: SEARCH_RECORDINGS,
            params: vec![
                p("title", Text, "paranoid android"),
                pn("artist", Text, ""),
                p("limit", BigInt, "25"),
            ],
        },
        QuerySpec {
            id: "search_recordings_fuzzy",
            title: "Recording search (trigram fallback)",
            endpoint: "GET /ws/2/recording (fallback)",
            db: TargetDb::Musicbrainz,
            trigram: true,
            sql: SEARCH_RECORDINGS_FUZZY,
            params: vec![
                p("title", Text, "paranoid android"),
                pn("artist", Text, ""),
                p("limit", BigInt, "25"),
            ],
        },
        QuerySpec {
            id: "search_tmdb_id_index",
            title: "TMDB id-index search (FTS)",
            endpoint: "GET /3/search/{movie,tv} (local)",
            db: TargetDb::Tmdb,
            trigram: true,
            sql: SEARCH_TMDB_ID_INDEX,
            params: vec![
                p("query", Text, "dune"),
                p("kind", Text, "movie"),
                p("limit", BigInt, "25"),
            ],
        },
        QuerySpec {
            id: "search_tmdb_id_index_fuzzy",
            title: "TMDB id-index search (trigram fallback)",
            endpoint: "GET /3/search/{movie,tv} (local, fallback)",
            db: TargetDb::Tmdb,
            trigram: true,
            sql: SEARCH_TMDB_ID_INDEX_FUZZY,
            params: vec![
                p("query", Text, "dune"),
                p("kind", Text, "movie"),
                p("limit", BigInt, "25"),
            ],
        },
        QuerySpec {
            id: "search_imdb_titles",
            title: "IMDb title search (FTS) — the \"dune\" path",
            endpoint: "GET /3/search/{movie,tv} (local)",
            db: TargetDb::Imdb,
            trigram: true,
            sql: SEARCH_IMDB_TITLES,
            params: vec![
                p("query", Text, "dune"),
                p("limit", BigInt, "25"),
                p("title_types", TextArray, "movie,tvMovie,short,video"),
            ],
        },
        QuerySpec {
            id: "search_imdb_titles_fuzzy",
            title: "IMDb title search (trigram fallback)",
            endpoint: "GET /3/search/{movie,tv} (local, fallback)",
            db: TargetDb::Imdb,
            trigram: true,
            sql: SEARCH_IMDB_TITLES_FUZZY,
            params: vec![
                p("query", Text, "dune"),
                p("limit", BigInt, "25"),
                p("title_types", TextArray, "movie,tvMovie,short,video"),
            ],
        },
        // ── browse ──
        QuerySpec {
            id: "browse_release_groups_count",
            title: "Release-group browse: total count for an artist",
            endpoint: "GET /ws/2/release-group?artist=",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BROWSE_RELEASE_GROUPS_COUNT,
            params: vec![p("artist_gid", Uuid, "a74b1b7f-71a5-4011-9441-d0b5e4122711")],
        },
        QuerySpec {
            id: "browse_release_groups",
            title: "Release-group browse by artist (paged)",
            endpoint: "GET /ws/2/release-group?artist=",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BROWSE_RELEASE_GROUPS,
            params: vec![
                p("artist_gid", Uuid, "a74b1b7f-71a5-4011-9441-d0b5e4122711"),
                p("limit", BigInt, "100"),
                p("offset", BigInt, "0"),
            ],
        },
        // ── lookups ──
        QuerySpec {
            id: "lookup_artist",
            title: "Artist lookup by MBID",
            endpoint: "GET /ws/2/artist/{mbid}",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOOKUP_ARTIST,
            params: vec![p("gid", Uuid, "a74b1b7f-71a5-4011-9441-d0b5e4122711")],
        },
        QuerySpec {
            id: "lookup_release",
            title: "Release lookup by MBID",
            endpoint: "GET /ws/2/release/{mbid}",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOOKUP_RELEASE,
            params: vec![p("gid", Uuid, "b1392450-e666-3926-a536-22c65f834433")],
        },
        QuerySpec {
            id: "lookup_recording",
            title: "Recording lookup by MBID",
            endpoint: "GET /ws/2/recording/{mbid}",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOOKUP_RECORDING,
            params: vec![p("gid", Uuid, "b1a9c0e9-d987-4042-ae91-78d6a3267d69")],
        },
        QuerySpec {
            id: "lookup_release_group",
            title: "Release-group lookup by MBID",
            endpoint: "GET /ws/2/release-group/{mbid}",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOOKUP_RELEASE_GROUP,
            params: vec![p("gid", Uuid, "b1392450-e666-3926-a536-22c65f834433")],
        },
        // ── batched hydration ──
        QuerySpec {
            id: "batch_artist_aliases",
            title: "Batch: artist aliases",
            endpoint: "(hydration)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BATCH_ARTIST_ALIASES,
            params: vec![p("artist_ids", IntArray, "1,2,3")],
        },
        QuerySpec {
            id: "batch_artist_credits",
            title: "Batch: artist credits",
            endpoint: "(hydration)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BATCH_ARTIST_CREDITS,
            params: vec![p("ac_ids", IntArray, "1,2,3")],
        },
        QuerySpec {
            id: "batch_release_groups",
            title: "Batch: release groups",
            endpoint: "(hydration)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BATCH_RELEASE_GROUPS,
            params: vec![p("rg_ids", IntArray, "1,2,3")],
        },
        QuerySpec {
            id: "batch_release_group_secondary_types",
            title: "Batch: release-group secondary types",
            endpoint: "(hydration)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BATCH_RELEASE_GROUP_SECONDARY_TYPES,
            params: vec![p("rg_ids", IntArray, "1,2,3")],
        },
        QuerySpec {
            id: "batch_release_dates",
            title: "Batch: release dates",
            endpoint: "(hydration)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BATCH_RELEASE_DATES,
            params: vec![p("release_ids", IntArray, "1,2,3")],
        },
        QuerySpec {
            id: "batch_release_statuses",
            title: "Batch: release statuses",
            endpoint: "(hydration)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BATCH_RELEASE_STATUSES,
            params: vec![p("release_ids", IntArray, "1,2,3")],
        },
        QuerySpec {
            id: "batch_release_comments",
            title: "Batch: release comments",
            endpoint: "(hydration)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BATCH_RELEASE_COMMENTS,
            params: vec![p("release_ids", IntArray, "1,2,3")],
        },
        QuerySpec {
            id: "batch_release_track_counts",
            title: "Batch: release track counts",
            endpoint: "(hydration)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BATCH_RELEASE_TRACK_COUNTS,
            params: vec![p("release_ids", IntArray, "1,2,3")],
        },
        QuerySpec {
            id: "batch_tracks",
            title: "Batch: tracks for media",
            endpoint: "(hydration)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BATCH_TRACKS,
            params: vec![p("medium_ids", IntArray, "1,2,3")],
        },
        QuerySpec {
            id: "batch_media",
            title: "Batch: media for releases",
            endpoint: "(hydration)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BATCH_MEDIA,
            params: vec![p("release_ids", IntArray, "1,2,3")],
        },
        QuerySpec {
            id: "batch_recording_releases",
            title: "Batch: releases per recording",
            endpoint: "(hydration)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: BATCH_RECORDING_RELEASES,
            params: vec![p("recording_ids", IntArray, "1,2,3")],
        },
        // ── per-row loaders (lookup path) ──
        QuerySpec {
            id: "load_artist_url_relations",
            title: "Load: artist URL relations",
            endpoint: "GET /ws/2/artist/{mbid}?inc=url-rels",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOAD_ARTIST_URL_RELATIONS,
            params: vec![p("artist_id", Int, "1")],
        },
        QuerySpec {
            id: "load_artist_aliases",
            title: "Load: artist aliases (one artist)",
            endpoint: "(lookup)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOAD_ARTIST_ALIASES,
            params: vec![p("artist_id", Int, "1")],
        },
        QuerySpec {
            id: "load_artist_credit",
            title: "Load: artist credit (one credit)",
            endpoint: "(lookup)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOAD_ARTIST_CREDIT,
            params: vec![p("artist_credit_id", Int, "1")],
        },
        QuerySpec {
            id: "release_date",
            title: "Load: release date events (one release)",
            endpoint: "(lookup)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: RELEASE_DATE,
            params: vec![p("release_id", Int, "1")],
        },
        QuerySpec {
            id: "load_release_group",
            title: "Load: release group (one)",
            endpoint: "(lookup)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOAD_RELEASE_GROUP,
            params: vec![p("rg_id", Int, "1")],
        },
        QuerySpec {
            id: "load_release_group_releases",
            title: "Load: releases in a release group",
            endpoint: "GET /ws/2/release-group/{mbid}",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOAD_RELEASE_GROUP_RELEASES,
            params: vec![p("rg_id", Int, "1")],
        },
        QuerySpec {
            id: "load_release_status",
            title: "Load: release status (one)",
            endpoint: "(lookup)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOAD_RELEASE_STATUS,
            params: vec![p("release_id", Int, "1")],
        },
        QuerySpec {
            id: "load_release_comment",
            title: "Load: release comment (one)",
            endpoint: "(lookup)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOAD_RELEASE_COMMENT,
            params: vec![p("release_id", Int, "1")],
        },
        QuerySpec {
            id: "load_media",
            title: "Load: media for a release",
            endpoint: "(lookup)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOAD_MEDIA,
            params: vec![p("release_id", Int, "1")],
        },
        QuerySpec {
            id: "load_tracks",
            title: "Load: tracks for a medium",
            endpoint: "(lookup)",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOAD_TRACKS,
            params: vec![p("medium_id", Int, "1")],
        },
        QuerySpec {
            id: "load_release_relations",
            title: "Load: release-release relations",
            endpoint: "GET /ws/2/release/{mbid}",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: LOAD_RELEASE_RELATIONS,
            params: vec![p("release_id", Int, "1")],
        },
        // ── tmdb cache resolution ──
        QuerySpec {
            id: "resolve_tconsts_via_cache",
            title: "Resolve IMDb tconsts → TMDB ids (cache)",
            endpoint: "(local movie/tv search)",
            db: TargetDb::Tmdb,
            trigram: false,
            sql: RESOLVE_TCONSTS_VIA_CACHE,
            params: vec![p("kind", Text, "movie"), p("tconsts", TextArray, "tt0087182")],
        },
        QuerySpec {
            id: "index_meta_for_ids",
            title: "TMDB id-index metadata for ids",
            endpoint: "(local movie/tv search)",
            db: TargetDb::Tmdb,
            trigram: false,
            sql: INDEX_META_FOR_IDS,
            params: vec![p("kind", Text, "movie"), p("ids", BigIntArray, "438631")],
        },
        // ── health ──
        QuerySpec {
            id: "ping",
            title: "Health ping",
            endpoint: "GET /health",
            db: TargetDb::Musicbrainz,
            trigram: false,
            sql: PING,
            params: vec![],
        },
    ]
}

/// Find a spec by its stable id.
#[must_use]
pub fn find(id: &str) -> Option<QuerySpec> {
    catalog().into_iter().find(|q| q.id == id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_ids_are_unique() {
        let cat = catalog();
        let mut ids: Vec<&str> = cat.iter().map(|q| q.id).collect();
        ids.sort_unstable();
        let n = ids.len();
        ids.dedup();
        assert_eq!(ids.len(), n, "duplicate query id in catalog");
    }

    #[test]
    fn every_spec_declares_all_its_binds() {
        // The highest `$n` referenced in the SQL must have a matching param slot,
        // so the runner never binds too few args. (Params may be reused across
        // several `$n` positions — e.g. `$1`/`$2` appear many times — so we check
        // the max index, not raw occurrences.)
        for q in catalog() {
            let max_n = (1..=9).filter(|n| q.sql.contains(&format!("${n}"))).max().unwrap_or(0);
            assert_eq!(
                max_n as usize,
                q.params.len(),
                "query `{}` references ${max_n} but declares {} params",
                q.id,
                q.params.len()
            );
        }
    }

    #[test]
    fn find_resolves_known_and_rejects_unknown() {
        assert!(find("search_imdb_titles").is_some());
        assert!(find("nope").is_none());
    }
}
