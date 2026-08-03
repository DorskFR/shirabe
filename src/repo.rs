//! Read-only query layer against the MusicBrainz Postgres mirror (`musicbrainz`
//! schema). Uses sqlx runtime queries (no compile-time macros) so the build
//! never needs a live DB.

use std::collections::HashMap;

use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{PgConnection, PgPool, Postgres, Row};
use uuid::Uuid;

use crate::date::{DateEvent, select_release_date};
use crate::models::{
    Alias, Artist, ArtistCredit, ArtistLookup, ArtistRef, Medium, Recording, RecordingRef,
    Relation, Release, ReleaseGroup, ReleaseGroupDetail, ReleaseGroupRelease, ReleaseStub, Track,
    UrlRelation, UrlResource,
};
use crate::queries;
use crate::search::configure_search_session;

/// Scale a pg_trgm similarity (0.0-1.0) into a MusicBrainz-style score (0-100).
///
/// `similarity()` returns Postgres `real` (FLOAT4), so the score is decoded as
/// `f32`; we widen to `f64` only for the arithmetic here.
fn to_score(similarity: f32) -> i32 {
    (f64::from(similarity) * 100.0).round().clamp(0.0, 100.0) as i32
}

/// Run a trigram fuzzy-fallback search (used only when the FTS primary matched
/// nothing). Bounded by the session `statement_timeout` (SHIB-19); a pathological
/// common-token fallback that hits the cap yields no rows (SQLSTATE 57014) rather
/// than erroring the whole request — the FTS primary already returned none.
async fn fetch_fuzzy(
    query: sqlx::query::Query<'_, Postgres, PgArguments>,
    conn: &mut PgConnection,
) -> Result<Vec<PgRow>, sqlx::Error> {
    match query.fetch_all(conn).await {
        Ok(rows) => Ok(rows),
        Err(e)
            if e.as_database_error().and_then(sqlx::error::DatabaseError::code).as_deref()
                == Some("57014") =>
        {
            Ok(Vec::new())
        }
        Err(e) => Err(e),
    }
}

// ── Batched (set-based) hydration ─────────────────────────
//
// SHIB-17: search hydration previously fanned out into O(rows) sequential
// round-trips (per-release date/status/comment/track-count/group/credit, and a
// deeply nested per-recording→per-release repeat for recording search). These
// `batch_*` helpers replace that fan-out with one `= ANY($1)` query per data
// kind, assembling the results in-memory keyed by id. Callers re-emit rows in
// the original rank order, so the JSON contract (including artist-credit
// position ordering) is unchanged.

/// Aliases for many artists at once, keyed by artist id, preserving `id ASC`
/// order within each artist (matches the per-row loader).
async fn batch_artist_aliases(
    pool: &PgPool,
    artist_ids: &[i32],
) -> Result<HashMap<i32, Vec<Alias>>, sqlx::Error> {
    let mut map: HashMap<i32, Vec<Alias>> = HashMap::new();
    if artist_ids.is_empty() {
        return Ok(map);
    }
    let rows = sqlx::query(queries::BATCH_ARTIST_ALIASES).bind(artist_ids).fetch_all(pool).await?;
    for r in rows {
        let aid: i32 = r.try_get("artist")?;
        map.entry(aid)
            .or_default()
            .push(Alias { name: r.get("name"), sort_name: r.try_get("sort_name").ok() });
    }
    Ok(map)
}

/// Ordered artist-credits for many `artist_credit` ids at once, keyed by
/// `artist_credit` id and preserving `position ASC` order within each credit.
/// When `with_aliases` is set, each artist's aliases are batched in one extra
/// query and attached (recording credits carry aliases per the contract).
async fn batch_artist_credits(
    pool: &PgPool,
    ac_ids: &[i32],
    with_aliases: bool,
) -> Result<HashMap<i32, Vec<ArtistCredit>>, sqlx::Error> {
    let mut map: HashMap<i32, Vec<ArtistCredit>> = HashMap::new();
    if ac_ids.is_empty() {
        return Ok(map);
    }
    let rows = sqlx::query(queries::BATCH_ARTIST_CREDITS).bind(ac_ids).fetch_all(pool).await?;

    let aliases_map = if with_aliases {
        let mut aids: Vec<i32> =
            rows.iter().filter_map(|r| r.try_get::<i32, _>("artist_id").ok()).collect();
        aids.sort_unstable();
        aids.dedup();
        batch_artist_aliases(pool, &aids).await?
    } else {
        HashMap::new()
    };

    for r in rows {
        let ac_id: i32 = r.try_get("ac_id")?;
        let artist_id: i32 = r.try_get("artist_id")?;
        let gid: Uuid = r.try_get("artist_gid")?;
        let aliases = if with_aliases {
            aliases_map.get(&artist_id).cloned().unwrap_or_default()
        } else {
            Vec::new()
        };
        map.entry(ac_id).or_default().push(ArtistCredit {
            artist: ArtistRef { id: gid.to_string(), name: r.try_get("credit_name")?, aliases },
        });
    }
    Ok(map)
}

/// Release-groups for many rg ids at once, keyed by rg id.
async fn batch_release_groups(
    pool: &PgPool,
    rg_ids: &[i32],
) -> Result<HashMap<i32, ReleaseGroup>, sqlx::Error> {
    let mut map = HashMap::new();
    if rg_ids.is_empty() {
        return Ok(map);
    }
    let rows = sqlx::query(queries::BATCH_RELEASE_GROUPS).bind(rg_ids).fetch_all(pool).await?;
    for r in rows {
        let id: i32 = r.try_get("id")?;
        let gid: Uuid = r.get("gid");
        map.insert(
            id,
            ReleaseGroup { id: gid.to_string(), primary_type: r.try_get("primary_type").ok() },
        );
    }
    Ok(map)
}

/// Secondary-type names for many release groups at once, keyed by rg id.
async fn batch_release_group_secondary_types(
    pool: &PgPool,
    rg_ids: &[i32],
) -> Result<HashMap<i32, Vec<String>>, sqlx::Error> {
    let mut map: HashMap<i32, Vec<String>> = HashMap::new();
    if rg_ids.is_empty() {
        return Ok(map);
    }
    let rows = sqlx::query(queries::BATCH_RELEASE_GROUP_SECONDARY_TYPES)
        .bind(rg_ids)
        .fetch_all(pool)
        .await?;
    for r in rows {
        let rg: i32 = r.try_get("rg")?;
        map.entry(rg).or_default().push(r.get("name"));
    }
    Ok(map)
}

/// Collapsed release dates for many releases at once, keyed by release id.
/// Missing releases are simply absent (callers default to "").
async fn batch_release_dates(
    pool: &PgPool,
    release_ids: &[i32],
) -> Result<HashMap<i32, String>, sqlx::Error> {
    let mut map = HashMap::new();
    if release_ids.is_empty() {
        return Ok(map);
    }
    let rows = sqlx::query(queries::BATCH_RELEASE_DATES).bind(release_ids).fetch_all(pool).await?;

    let mut events: HashMap<i32, Vec<DateEvent>> = HashMap::new();
    for r in rows {
        let rel: i32 = r.try_get("rel")?;
        events.entry(rel).or_default().push(DateEvent {
            year: r.try_get("y").ok(),
            month: r.try_get("m").ok(),
            day: r.try_get("d").ok(),
            is_xw: r.try_get("is_xw").unwrap_or(false),
        });
    }
    for (rel, evs) in events {
        map.insert(rel, select_release_date(&evs));
    }
    Ok(map)
}

/// Release status names for many releases at once, keyed by release id (absent
/// when the release has no status → caller emits `None`).
async fn batch_release_statuses(
    pool: &PgPool,
    release_ids: &[i32],
) -> Result<HashMap<i32, String>, sqlx::Error> {
    let mut map = HashMap::new();
    if release_ids.is_empty() {
        return Ok(map);
    }
    let rows =
        sqlx::query(queries::BATCH_RELEASE_STATUSES).bind(release_ids).fetch_all(pool).await?;
    for r in rows {
        let id: i32 = r.try_get("id")?;
        map.insert(id, r.get("name"));
    }
    Ok(map)
}

/// Non-empty release disambiguation comments for many releases at once.
async fn batch_release_comments(
    pool: &PgPool,
    release_ids: &[i32],
) -> Result<HashMap<i32, String>, sqlx::Error> {
    let mut map = HashMap::new();
    if release_ids.is_empty() {
        return Ok(map);
    }
    let rows =
        sqlx::query(queries::BATCH_RELEASE_COMMENTS).bind(release_ids).fetch_all(pool).await?;
    for r in rows {
        let id: i32 = r.try_get("id")?;
        let c: String = r.get("comment");
        if !c.is_empty() {
            map.insert(id, c);
        }
    }
    Ok(map)
}

/// Summed medium track-counts for many releases at once (absent when 0).
async fn batch_release_track_counts(
    pool: &PgPool,
    release_ids: &[i32],
) -> Result<HashMap<i32, u32>, sqlx::Error> {
    let mut map = HashMap::new();
    if release_ids.is_empty() {
        return Ok(map);
    }
    let rows =
        sqlx::query(queries::BATCH_RELEASE_TRACK_COUNTS).bind(release_ids).fetch_all(pool).await?;
    for r in rows {
        let rel: i32 = r.try_get("rel")?;
        let total: i64 = r.try_get("total")?;
        if total > 0 {
            map.insert(rel, total as u32);
        }
    }
    Ok(map)
}

/// Intermediate (Clone-able) media/track carriers so a release that appears
/// under several recordings can be materialised into fresh `Medium`/`Track`
/// model instances per occurrence without cloning the (non-Clone) model types.
#[derive(Clone)]
struct MediaData {
    id: i32,
    position: i32,
    track_count: i32,
    title: Option<String>,
    format: Option<String>,
    tracks: Vec<TrackData>,
}

#[derive(Clone)]
struct TrackData {
    track_gid: Uuid,
    track_name: String,
    position: i32,
    number: String,
    rec_gid: Uuid,
    rec_name: String,
    rec_length: Option<i32>,
    artist_credit: Vec<ArtistCredit>,
}

fn media_to_model(data: Vec<MediaData>) -> Vec<Medium> {
    data.into_iter()
        .map(|m| Medium {
            id: m.id.to_string(),
            position: m.position as u32,
            track_count: m.track_count as u32,
            title: m.title,
            format: m.format,
            tracks: m
                .tracks
                .into_iter()
                .map(|t| Track {
                    id: t.track_gid.to_string(),
                    title: t.track_name,
                    position: t.position as u32,
                    number: t.number,
                    recording: RecordingRef {
                        id: t.rec_gid.to_string(),
                        title: t.rec_name,
                        length: t.rec_length,
                    },
                    artist_credit: t.artist_credit,
                })
                .collect(),
        })
        .collect()
}

/// Tracks for many media at once, keyed by medium id and preserving
/// `position ASC` order. Track-level artist credits are batched in one query.
async fn batch_tracks(
    pool: &PgPool,
    medium_ids: &[i32],
) -> Result<HashMap<i32, Vec<TrackData>>, sqlx::Error> {
    let mut map: HashMap<i32, Vec<TrackData>> = HashMap::new();
    if medium_ids.is_empty() {
        return Ok(map);
    }
    let rows = sqlx::query(queries::BATCH_TRACKS).bind(medium_ids).fetch_all(pool).await?;

    let mut ac_ids: Vec<i32> =
        rows.iter().filter_map(|r| r.try_get::<i32, _>("track_ac").ok()).collect();
    ac_ids.sort_unstable();
    ac_ids.dedup();
    let ac_map = batch_artist_credits(pool, &ac_ids, false).await?;

    for r in rows {
        let mid: i32 = r.try_get("mid")?;
        let track_gid: Uuid = r.try_get("track_gid")?;
        let rec_gid: Uuid = r.try_get("rec_gid")?;
        let position: i32 = r.try_get("position")?;
        let track_ac: Option<i32> = r.try_get("track_ac").ok();
        let artist_credit = track_ac.and_then(|ac| ac_map.get(&ac).cloned()).unwrap_or_default();
        map.entry(mid).or_default().push(TrackData {
            track_gid,
            track_name: r.try_get("track_name")?,
            position,
            number: r.try_get("number")?,
            rec_gid,
            rec_name: r.try_get("rec_name")?,
            rec_length: r.try_get("rec_length").ok(),
            artist_credit,
        });
    }
    Ok(map)
}

/// Media (with tracks) for many releases at once, keyed by release id and
/// preserving `position ASC` order within each release.
async fn batch_media(
    pool: &PgPool,
    release_ids: &[i32],
) -> Result<HashMap<i32, Vec<MediaData>>, sqlx::Error> {
    let mut map: HashMap<i32, Vec<MediaData>> = HashMap::new();
    if release_ids.is_empty() {
        return Ok(map);
    }
    let rows = sqlx::query(queries::BATCH_MEDIA).bind(release_ids).fetch_all(pool).await?;

    let medium_ids: Vec<i32> = rows.iter().filter_map(|r| r.try_get::<i32, _>("id").ok()).collect();
    let mut tracks_map = batch_tracks(pool, &medium_ids).await?;

    for r in rows {
        let rel: i32 = r.try_get("rel")?;
        let mid: i32 = r.try_get("id")?;
        let position: i32 = r.try_get("position")?;
        let track_count: i32 = r.try_get("track_count")?;
        let title: Option<String> = r.try_get::<String, _>("title").ok().filter(|s| !s.is_empty());
        let tracks = tracks_map.remove(&mid).unwrap_or_default();
        map.entry(rel).or_default().push(MediaData {
            id: mid,
            position,
            track_count,
            title,
            format: r.try_get("format").ok(),
            tracks,
        });
    }
    Ok(map)
}

// ── Artist search ─────────────────────────────────────────

/// `GET /ws/2/artist?query=<name>&inc=aliases`
///
/// Trigram-ranks artists by name / sort-name and attaches their aliases.
pub async fn search_artists(
    pool: &PgPool,
    name: &str,
    limit: i64,
    threshold: f64,
    work_mem: &str,
) -> Result<Vec<Artist>, sqlx::Error> {
    // SHIB-23: FTS + f_unaccent whole-word fast path across artist.name /
    // sort_name / artist_alias.name (3 UNION branches, de-duped by id with MAX
    // score so a name/sort_name/alias hit all compete). Accent-folded, so 'bjork'
    // matches 'Björk'. Trigram `%` fallback only when FTS matches nothing (typo'd /
    // partial query). SHIB-20: the alias branch keeps alias-only recall (localised
    // / alternate names), previously loaded per-row by FK but never searched.
    let mut conn = pool.acquire().await?;
    configure_search_session(&mut conn, threshold, work_mem).await?;
    let mut rows =
        sqlx::query(queries::SEARCH_ARTISTS).bind(name).bind(limit).fetch_all(&mut *conn).await?;
    if rows.is_empty() {
        rows = fetch_fuzzy(
            sqlx::query(queries::SEARCH_ARTISTS_FUZZY).bind(name).bind(limit),
            &mut conn,
        )
        .await?;
    }
    drop(conn);

    // SHIB-17: batch all artists' aliases in one query instead of one per row.
    let ids: Vec<i32> = rows.iter().filter_map(|r| r.try_get::<i32, _>("id").ok()).collect();
    let mut aliases_map = batch_artist_aliases(pool, &ids).await?;

    let mut artists = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row.try_get("id")?;
        let gid: Uuid = row.try_get("gid")?;
        let score: f32 = row.try_get("score")?;
        let aliases = aliases_map.remove(&id).unwrap_or_default();
        artists.push(Artist {
            id: gid.to_string(),
            name: row.try_get("name")?,
            score: Some(to_score(score)),
            aliases,
        });
    }
    Ok(artists)
}

// ── Artist lookup ─────────────────────────────────────────

/// `GET /ws/2/artist/{mbid}[?inc=url-rels]`
///
/// Loads the core artist row by MBID; when `with_url_rels` is set, also attaches
/// the artist's URL relationships (e.g. the `image` link).
pub async fn lookup_artist(
    pool: &PgPool,
    gid: Uuid,
    with_url_rels: bool,
) -> Result<Option<ArtistLookup>, sqlx::Error> {
    let Some(row) = sqlx::query(queries::LOOKUP_ARTIST).bind(gid).fetch_optional(pool).await?
    else {
        return Ok(None);
    };

    let id: i32 = row.try_get("id")?;
    let comment: String = row.try_get("comment").unwrap_or_default();
    let relations =
        if with_url_rels { load_artist_url_relations(pool, id).await? } else { Vec::new() };

    Ok(Some(ArtistLookup {
        id: gid.to_string(),
        name: row.try_get("name")?,
        sort_name: row.try_get("sort_name")?,
        artist_type: row.try_get("type_name").ok(),
        disambiguation: if comment.is_empty() { None } else { Some(comment) },
        relations,
    }))
}

/// artist-url relations (`l_artist_url`) for an artist lookup. Maps
/// `link_type.name` to the ws/2 relation `type` and `url.url` to
/// `relation.url.resource`. These are always `forward` (artist -> url).
async fn load_artist_url_relations(
    pool: &PgPool,
    artist_id: i32,
) -> Result<Vec<UrlRelation>, sqlx::Error> {
    let rows =
        sqlx::query(queries::LOAD_ARTIST_URL_RELATIONS).bind(artist_id).fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(|r| UrlRelation {
            rel_type: r.get("rel_type"),
            direction: "forward".to_string(),
            url: UrlResource { resource: r.get("resource") },
        })
        .collect())
}

async fn load_artist_aliases(pool: &PgPool, artist_id: i32) -> Result<Vec<Alias>, sqlx::Error> {
    let rows = sqlx::query(queries::LOAD_ARTIST_ALIASES).bind(artist_id).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|r| Alias { name: r.get("name"), sort_name: r.try_get("sort_name").ok() })
        .collect())
}

// ── Artist credits ────────────────────────────────────────

/// Load the ordered artist-credit for one `artist_credit` id, optionally with
/// each artist's aliases (recording credits include aliases per the contract).
async fn load_artist_credit(
    pool: &PgPool,
    artist_credit_id: i32,
    with_aliases: bool,
) -> Result<Vec<ArtistCredit>, sqlx::Error> {
    let rows =
        sqlx::query(queries::LOAD_ARTIST_CREDIT).bind(artist_credit_id).fetch_all(pool).await?;

    let mut credits = Vec::with_capacity(rows.len());
    for row in rows {
        let artist_id: i32 = row.try_get("artist_id")?;
        let gid: Uuid = row.try_get("artist_gid")?;
        let aliases =
            if with_aliases { load_artist_aliases(pool, artist_id).await? } else { Vec::new() };
        credits.push(ArtistCredit {
            artist: ArtistRef { id: gid.to_string(), name: row.try_get("credit_name")?, aliases },
        });
    }
    Ok(credits)
}

// ── Release dates ─────────────────────────────────────────

/// Gather all date events for a release across `release_country` and
/// `release_unknown_country`, then collapse to one MB partial date string.
async fn release_date(pool: &PgPool, release_id: i32) -> Result<String, sqlx::Error> {
    let rows = sqlx::query(queries::RELEASE_DATE).bind(release_id).fetch_all(pool).await?;

    let events: Vec<DateEvent> = rows
        .into_iter()
        .map(|r| DateEvent {
            year: r.try_get("y").ok(),
            month: r.try_get("m").ok(),
            day: r.try_get("d").ok(),
            is_xw: r.try_get("is_xw").unwrap_or(false),
        })
        .collect();
    Ok(select_release_date(&events))
}

// ── Release search ────────────────────────────────────────

#[derive(Debug, Default, Clone, Copy)]
pub struct ReleaseFilters<'a> {
    pub artist: Option<&'a str>,
    pub year: Option<&'a str>,
    pub primary_type: Option<&'a str>,
    pub status: Option<&'a str>,
}

/// `GET /ws/2/release?query=release:(..) AND artist:(..) [AND date:(YYYY*)]`
pub async fn search_releases(
    pool: &PgPool,
    title: &str,
    filters: ReleaseFilters<'_>,
    limit: i64,
    threshold: f64,
    work_mem: &str,
) -> Result<Vec<Release>, sqlx::Error> {
    // SHIB-22: FTS whole-word fast path (0.4ms warm vs ~350ms for the trigram `%`
    // scan), ranked by similarity() with an optional artist-credit similarity bonus
    // so title stays the dominant signal; trigram `%` fallback only when FTS matches
    // nothing (typo'd / partial query).
    let mut conn = pool.acquire().await?;
    configure_search_session(&mut conn, threshold, work_mem).await?;
    let year_i32 = filters.year.and_then(|y| y.parse::<i32>().ok());
    // FTS fast path (whole-word match); fall back to the trigram `%` scan only when
    // FTS finds nothing (typo'd / partial query), so recall never regresses.
    let mut rows = sqlx::query(queries::SEARCH_RELEASES)
        .bind(title)
        .bind(filters.artist)
        .bind(year_i32)
        .bind(limit)
        .bind(filters.primary_type)
        .bind(filters.status)
        .fetch_all(&mut *conn)
        .await?;
    if rows.is_empty() {
        rows = fetch_fuzzy(
            sqlx::query(queries::SEARCH_RELEASES_FUZZY)
                .bind(title)
                .bind(filters.artist)
                .bind(year_i32)
                .bind(limit)
                .bind(filters.primary_type)
                .bind(filters.status),
            &mut conn,
        )
        .await?;
    }
    drop(conn);
    hydrate_release_search_rows(pool, rows).await
}

/// `GET /ws/2/release?query=arid:{mbid} [AND primarytype:.. AND status:..]`
pub async fn browse_releases_by_artist(
    pool: &PgPool,
    artist_gid: Uuid,
    primary_type: Option<&str>,
    status: Option<&str>,
    limit: i64,
) -> Result<Vec<Release>, sqlx::Error> {
    let rows = sqlx::query(queries::BROWSE_RELEASES_BY_ARTIST)
        .bind(artist_gid)
        .bind(primary_type)
        .bind(status)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    hydrate_release_search_rows(pool, rows).await
}

async fn hydrate_release_search_rows(
    pool: &PgPool,
    rows: Vec<PgRow>,
) -> Result<Vec<Release>, sqlx::Error> {
    // SHIB-17: collect keys across all rows and batch every per-release load into
    // one set-based query each (credits, groups, dates, statuses, comments,
    // track-counts) rather than ~6 round-trips per row. Rows are then re-emitted
    // in rank order from the in-memory maps.
    let release_ids: Vec<i32> =
        rows.iter().filter_map(|r| r.try_get::<i32, _>("id").ok()).collect();
    let ac_ids: Vec<i32> =
        rows.iter().filter_map(|r| r.try_get::<i32, _>("artist_credit").ok()).collect();
    let rg_ids: Vec<i32> =
        rows.iter().filter_map(|r| r.try_get::<i32, _>("release_group").ok()).collect();

    let ac_map = batch_artist_credits(pool, &ac_ids, false).await?;
    let rg_map = batch_release_groups(pool, &rg_ids).await?;
    let date_map = batch_release_dates(pool, &release_ids).await?;
    let status_map = batch_release_statuses(pool, &release_ids).await?;
    let comment_map = batch_release_comments(pool, &release_ids).await?;
    let tc_map = batch_release_track_counts(pool, &release_ids).await?;

    let mut releases = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row.try_get("id")?;
        let gid: Uuid = row.try_get("gid")?;
        let title_score: f32 = row.try_get("title_score")?;
        let artist_credit_id: i32 = row.try_get("artist_credit")?;
        let rg_id: Option<i32> = row.try_get("release_group").ok();

        let artist_credit = ac_map.get(&artist_credit_id).cloned().unwrap_or_default();
        let release_group = rg_id.and_then(|r| rg_map.get(&r).cloned());
        let date = date_map.get(&id).cloned().unwrap_or_default();
        let status = status_map.get(&id).cloned();
        let disambiguation = comment_map.get(&id).cloned();
        let track_count = tc_map.get(&id).copied();

        releases.push(Release {
            id: gid.to_string(),
            title: row.try_get("name")?,
            date,
            score: Some(to_score(title_score)),
            status,
            disambiguation,
            artist_credit,
            track_count,
            release_group,
            media: Vec::new(),
            relations: Vec::new(),
        });
    }
    Ok(releases)
}

async fn load_release_group(
    pool: &PgPool,
    rg_id: Option<i32>,
) -> Result<Option<ReleaseGroup>, sqlx::Error> {
    let Some(rg_id) = rg_id else { return Ok(None) };
    let row = sqlx::query(queries::LOAD_RELEASE_GROUP).bind(rg_id).fetch_optional(pool).await?;
    Ok(row.map(|r| {
        let gid: Uuid = r.get("gid");
        ReleaseGroup { id: gid.to_string(), primary_type: r.try_get("primary_type").ok() }
    }))
}

async fn load_release_status(
    pool: &PgPool,
    release_id: i32,
) -> Result<Option<String>, sqlx::Error> {
    let row =
        sqlx::query(queries::LOAD_RELEASE_STATUS).bind(release_id).fetch_optional(pool).await?;
    Ok(row.map(|r| r.get("name")))
}

async fn load_release_comment(
    pool: &PgPool,
    release_id: i32,
) -> Result<Option<String>, sqlx::Error> {
    let row =
        sqlx::query(queries::LOAD_RELEASE_COMMENT).bind(release_id).fetch_optional(pool).await?;
    Ok(row.and_then(|r| {
        let c: String = r.get("comment");
        if c.is_empty() { None } else { Some(c) }
    }))
}

// ── Release lookup (full) ─────────────────────────────────

/// `GET /ws/2/release/{mbid}?inc=...media+recordings+...rels`
pub async fn lookup_release(pool: &PgPool, gid: Uuid) -> Result<Option<Release>, sqlx::Error> {
    let Some(row) = sqlx::query(queries::LOOKUP_RELEASE).bind(gid).fetch_optional(pool).await?
    else {
        return Ok(None);
    };

    let id: i32 = row.try_get("id")?;
    let artist_credit_id: i32 = row.try_get("artist_credit")?;
    let rg_id: Option<i32> = row.try_get("release_group").ok();

    let artist_credit = load_artist_credit(pool, artist_credit_id, false).await?;
    let release_group = load_release_group(pool, rg_id).await?;
    let date = release_date(pool, id).await?;
    let status = load_release_status(pool, id).await?;
    let disambiguation = load_release_comment(pool, id).await?;
    let media = load_media(pool, id).await?;
    let track_count = Some(media.iter().map(|m| m.track_count).sum()).filter(|c: &u32| *c > 0);
    let relations = load_release_relations(pool, id).await?;

    Ok(Some(Release {
        id: gid.to_string(),
        title: row.try_get("name")?,
        date,
        score: None,
        status,
        disambiguation,
        artist_credit,
        track_count,
        release_group,
        media,
        relations,
    }))
}

/// Load all media (discs) for a release, ordered by position, each with tracks.
async fn load_media(pool: &PgPool, release_id: i32) -> Result<Vec<Medium>, sqlx::Error> {
    let rows = sqlx::query(queries::LOAD_MEDIA).bind(release_id).fetch_all(pool).await?;

    let mut media = Vec::with_capacity(rows.len());
    for row in rows {
        let medium_id: i32 = row.try_get("id")?;
        let position: i32 = row.try_get("position")?;
        let track_count: i32 = row.try_get("track_count")?;
        let title: Option<String> =
            row.try_get::<String, _>("title").ok().filter(|s| !s.is_empty());
        let tracks = load_tracks(pool, medium_id).await?;
        media.push(Medium {
            id: medium_id.to_string(),
            position: position as u32,
            track_count: track_count as u32,
            title,
            format: row.try_get("format").ok(),
            tracks,
        });
    }
    Ok(media)
}

/// Load tracks for one medium, ordered by position, with their recordings and
/// any track-level artist credit (for compilations).
async fn load_tracks(pool: &PgPool, medium_id: i32) -> Result<Vec<Track>, sqlx::Error> {
    let rows = sqlx::query(queries::LOAD_TRACKS).bind(medium_id).fetch_all(pool).await?;

    let mut tracks = Vec::with_capacity(rows.len());
    for row in rows {
        let track_gid: Uuid = row.try_get("track_gid")?;
        let rec_gid: Uuid = row.try_get("rec_gid")?;
        let position: i32 = row.try_get("position")?;
        let track_ac: Option<i32> = row.try_get("track_ac").ok();

        // Track-level credit is only meaningful when present; the consumer
        // treats it as optional (compilations).
        let artist_credit = match track_ac {
            Some(ac) => load_artist_credit(pool, ac, false).await?,
            None => Vec::new(),
        };

        tracks.push(Track {
            id: track_gid.to_string(),
            title: row.try_get("track_name")?,
            position: position as u32,
            number: row.try_get("number")?,
            recording: RecordingRef {
                id: rec_gid.to_string(),
                title: row.try_get("rec_name")?,
                length: row.try_get("rec_length").ok(),
            },
            artist_credit,
        });
    }
    Ok(tracks)
}

/// release-release relations (`l_release_release`) for a release lookup.
async fn load_release_relations(
    pool: &PgPool,
    release_id: i32,
) -> Result<Vec<Relation>, sqlx::Error> {
    let rows =
        sqlx::query(queries::LOAD_RELEASE_RELATIONS).bind(release_id).fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let gid: Uuid = r.get("gid");
            Relation {
                direction: r.get("direction"),
                release: Some(ReleaseStub { id: gid.to_string(), title: r.get("name") }),
                recording: None,
            }
        })
        .collect())
}

// ── Release-group browse / lookup ─────────────────────────

fn first_release_date(row: &PgRow) -> String {
    DateEvent {
        year: row.try_get("y").ok(),
        month: row.try_get("m").ok(),
        day: row.try_get("d").ok(),
        is_xw: false,
    }
    .render()
}

async fn hydrate_release_groups(
    pool: &PgPool,
    rows: Vec<PgRow>,
) -> Result<Vec<ReleaseGroupDetail>, sqlx::Error> {
    let rg_ids: Vec<i32> = rows.iter().filter_map(|r| r.try_get::<i32, _>("id").ok()).collect();
    let ac_ids: Vec<i32> =
        rows.iter().filter_map(|r| r.try_get::<i32, _>("artist_credit").ok()).collect();
    let ac_map = batch_artist_credits(pool, &ac_ids, false).await?;
    let mut st_map = batch_release_group_secondary_types(pool, &rg_ids).await?;

    let mut groups = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row.try_get("id")?;
        let gid: Uuid = row.try_get("gid")?;
        let ac_id: i32 = row.try_get("artist_credit")?;
        groups.push(ReleaseGroupDetail {
            id: gid.to_string(),
            title: row.try_get("name")?,
            primary_type: row.try_get("primary_type").ok(),
            secondary_types: st_map.remove(&id).unwrap_or_default(),
            first_release_date: first_release_date(&row),
            disambiguation: row.try_get("comment").unwrap_or_default(),
            artist_credit: ac_map.get(&ac_id).cloned().unwrap_or_default(),
            releases: None,
        });
    }
    Ok(groups)
}

/// `GET /ws/2/release-group?artist={mbid}&limit=&offset=`
///
/// Returns `(total, page)`: the full count for the artist plus one stable page
/// ordered by first-release-date then gid.
pub async fn browse_release_groups(
    pool: &PgPool,
    artist_gid: Uuid,
    limit: i64,
    offset: i64,
) -> Result<(i64, Vec<ReleaseGroupDetail>), sqlx::Error> {
    let total: i64 = sqlx::query(queries::BROWSE_RELEASE_GROUPS_COUNT)
        .bind(artist_gid)
        .fetch_one(pool)
        .await?
        .try_get("total")?;
    let rows = sqlx::query(queries::BROWSE_RELEASE_GROUPS)
        .bind(artist_gid)
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;
    let groups = hydrate_release_groups(pool, rows).await?;
    Ok((total, groups))
}

/// `GET /ws/2/release-group/{mbid}?inc=artist-credits+releases`
pub async fn lookup_release_group(
    pool: &PgPool,
    gid: Uuid,
) -> Result<Option<ReleaseGroupDetail>, sqlx::Error> {
    let Some(row) =
        sqlx::query(queries::LOOKUP_RELEASE_GROUP).bind(gid).fetch_optional(pool).await?
    else {
        return Ok(None);
    };
    let rg_id: i32 = row.try_get("id")?;
    let mut group = hydrate_release_groups(pool, vec![row]).await?.remove(0);
    group.releases = Some(load_release_group_releases(pool, rg_id).await?);
    Ok(Some(group))
}

async fn load_release_group_releases(
    pool: &PgPool,
    rg_id: i32,
) -> Result<Vec<ReleaseGroupRelease>, sqlx::Error> {
    let rows =
        sqlx::query(queries::LOAD_RELEASE_GROUP_RELEASES).bind(rg_id).fetch_all(pool).await?;
    let release_ids: Vec<i32> =
        rows.iter().filter_map(|r| r.try_get::<i32, _>("id").ok()).collect();
    let date_map = batch_release_dates(pool, &release_ids).await?;

    let mut releases = Vec::with_capacity(rows.len());
    for r in rows {
        let id: i32 = r.try_get("id")?;
        let gid: Uuid = r.try_get("gid")?;
        releases.push(ReleaseGroupRelease {
            id: gid.to_string(),
            date: date_map.get(&id).cloned().unwrap_or_default(),
            status: r.try_get("status").ok(),
        });
    }
    Ok(releases)
}

// ── Recording search ──────────────────────────────────────

/// `GET /ws/2/recording?query=recording:".." AND artist:".."&inc=releases+...+media`
pub async fn search_recordings(
    pool: &PgPool,
    title: &str,
    artist: Option<&str>,
    limit: i64,
    threshold: f64,
    work_mem: &str,
) -> Result<Vec<Recording>, sqlx::Error> {
    // SHIB-22: FTS whole-word fast path, ranked by similarity(); trigram `%`
    // fallback only when FTS matches nothing. Recording is 36M rows, where the old
    // trigram-`%`-primary scan timed out — FTS reduces the candidate set to the
    // handful of rows containing every query word before ranking.
    let mut conn = pool.acquire().await?;
    configure_search_session(&mut conn, threshold, work_mem).await?;
    let mut rows = sqlx::query(queries::SEARCH_RECORDINGS)
        .bind(title)
        .bind(artist)
        .bind(limit)
        .fetch_all(&mut *conn)
        .await?;
    if rows.is_empty() {
        rows = fetch_fuzzy(
            sqlx::query(queries::SEARCH_RECORDINGS_FUZZY).bind(title).bind(artist).bind(limit),
            &mut conn,
        )
        .await?;
    }
    drop(conn);

    // SHIB-17: batch the artist-credits (with aliases) for every recording in one
    // query, and batch-hydrate the nested recording→releases fan-out (media +
    // tracks + release metadata) across ALL recordings at once instead of the
    // former per-recording-per-release N+1.
    let rec_ids: Vec<i32> = rows.iter().filter_map(|r| r.try_get::<i32, _>("id").ok()).collect();
    let ac_ids: Vec<i32> =
        rows.iter().filter_map(|r| r.try_get::<i32, _>("artist_credit").ok()).collect();
    let ac_map = batch_artist_credits(pool, &ac_ids, true).await?;
    // inc=releases+media: full release shapes incl. media/tracks.
    let mut releases_map = batch_recording_releases(pool, &rec_ids, true).await?;

    let mut recordings = Vec::with_capacity(rows.len());
    for row in rows {
        let rec_id: i32 = row.try_get("id")?;
        let gid: Uuid = row.try_get("gid")?;
        let title_score: f32 = row.try_get("title_score")?;
        let ac_id: i32 = row.try_get("artist_credit")?;

        let artist_credit = ac_map.get(&ac_id).cloned().unwrap_or_default();
        let releases = releases_map.remove(&rec_id).unwrap_or_default();

        recordings.push(Recording {
            id: gid.to_string(),
            title: row.try_get("name")?,
            length: row.try_get("length").ok(),
            score: Some(to_score(title_score)),
            artist_credit,
            releases,
        });
    }
    Ok(recordings)
}

// ── Recording lookup ──────────────────────────────────────

/// `GET /ws/2/recording/{mbid}?inc=releases+artist-credits+aliases`
pub async fn lookup_recording(pool: &PgPool, gid: Uuid) -> Result<Option<Recording>, sqlx::Error> {
    let Some(row) = sqlx::query(queries::LOOKUP_RECORDING).bind(gid).fetch_optional(pool).await?
    else {
        return Ok(None);
    };

    let rec_id: i32 = row.try_get("id")?;
    let ac_id: i32 = row.try_get("artist_credit")?;
    let artist_credit = load_artist_credit(pool, ac_id, true).await?;
    // Lookup does not request media; emit lightweight release shapes.
    let mut releases_map = batch_recording_releases(pool, &[rec_id], false).await?;
    let releases = releases_map.remove(&rec_id).unwrap_or_default();

    Ok(Some(Recording {
        id: gid.to_string(),
        title: row.try_get("name")?,
        length: row.try_get("length").ok(),
        score: None,
        artist_credit,
        releases,
    }))
}

/// All releases that contain each of the given recordings (via medium/track),
/// keyed by recording id and ordered `r.id ASC` within each recording. When
/// `with_media` is set, each release carries its media/tracks (recording search
/// needs this).
///
/// SHIB-17: this batches the whole nested fan-out — one join query maps every
/// recording to its releases, then all distinct release ids across every
/// recording are hydrated with a fixed handful of set-based queries (credits,
/// groups, dates, statuses, comments, track-counts, and media/tracks when
/// requested). A release shared by several recordings is materialised into a
/// fresh `Release` per occurrence from the shared in-memory maps.
async fn batch_recording_releases(
    pool: &PgPool,
    recording_ids: &[i32],
    with_media: bool,
) -> Result<HashMap<i32, Vec<Release>>, sqlx::Error> {
    let mut result: HashMap<i32, Vec<Release>> = HashMap::new();
    if recording_ids.is_empty() {
        return Ok(result);
    }
    let rows =
        sqlx::query(queries::BATCH_RECORDING_RELEASES).bind(recording_ids).fetch_all(pool).await?;

    // Distinct keys across every recording's releases, hydrated once.
    let mut release_ids: Vec<i32> =
        rows.iter().filter_map(|r| r.try_get::<i32, _>("id").ok()).collect();
    release_ids.sort_unstable();
    release_ids.dedup();
    let ac_ids: Vec<i32> =
        rows.iter().filter_map(|r| r.try_get::<i32, _>("artist_credit").ok()).collect();
    let rg_ids: Vec<i32> =
        rows.iter().filter_map(|r| r.try_get::<i32, _>("release_group").ok()).collect();

    let ac_map = batch_artist_credits(pool, &ac_ids, false).await?;
    let rg_map = batch_release_groups(pool, &rg_ids).await?;
    let date_map = batch_release_dates(pool, &release_ids).await?;
    let status_map = batch_release_statuses(pool, &release_ids).await?;
    let comment_map = batch_release_comments(pool, &release_ids).await?;
    let media_map =
        if with_media { batch_media(pool, &release_ids).await? } else { HashMap::new() };
    // track-count derives from media when present; media-less releases (and the
    // no-media lookup path) fall back to the summed medium track-counts.
    let tc_map = batch_release_track_counts(pool, &release_ids).await?;

    for row in rows {
        let rec: i32 = row.try_get("rec")?;
        let id: i32 = row.try_get("id")?;
        let gid: Uuid = row.try_get("gid")?;
        let artist_credit_id: i32 = row.try_get("artist_credit")?;
        let rg_id: Option<i32> = row.try_get("release_group").ok();

        let artist_credit = ac_map.get(&artist_credit_id).cloned().unwrap_or_default();
        let release_group = rg_id.and_then(|r| rg_map.get(&r).cloned());
        let date = date_map.get(&id).cloned().unwrap_or_default();
        let status = status_map.get(&id).cloned();
        let disambiguation = comment_map.get(&id).cloned();
        let media = if with_media {
            media_to_model(media_map.get(&id).cloned().unwrap_or_default())
        } else {
            Vec::new()
        };
        let track_count = if media.is_empty() {
            tc_map.get(&id).copied()
        } else {
            Some(media.iter().map(|m| m.track_count).sum())
        };

        result.entry(rec).or_default().push(Release {
            id: gid.to_string(),
            title: row.try_get("name")?,
            date,
            score: None,
            status,
            disambiguation,
            artist_credit,
            track_count,
            release_group,
            media,
            relations: Vec::new(),
        });
    }
    Ok(result)
}

/// Cheap connectivity probe used by the health endpoint.
pub async fn ping(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(queries::PING).execute(pool).await.map(|_| ())
}
