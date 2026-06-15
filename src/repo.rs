//! Read-only query layer against the MusicBrainz Postgres mirror (`musicbrainz`
//! schema). Uses sqlx runtime queries (no compile-time macros) so the build
//! never needs a live DB.

use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres, Row};
use uuid::Uuid;

use crate::date::{DateEvent, select_release_date};
use crate::models::{
    Alias, Artist, ArtistCredit, ArtistRef, Medium, Recording, RecordingRef, Relation, Release,
    ReleaseGroup, ReleaseStub, Track,
};

/// Scale a pg_trgm similarity (0.0-1.0) into a MusicBrainz-style score (0-100).
///
/// `similarity()` returns Postgres `real` (FLOAT4), so the score is decoded as
/// `f32`; we widen to `f64` only for the arithmetic here.
fn to_score(similarity: f32) -> i32 {
    (f64::from(similarity) * 100.0).round().clamp(0.0, 100.0) as i32
}

/// Set the `pg_trgm.similarity_threshold` GUC (the cutoff used by the `%`
/// operator) on a single connection. `set_limit()` clamps to [0,1] and applies
/// to the session, so it must run on the same connection as the search query.
async fn set_similarity_limit(
    conn: &mut PoolConnection<Postgres>,
    threshold: f64,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_limit($1)").bind(threshold as f32).execute(&mut **conn).await?;
    Ok(())
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
) -> Result<Vec<Artist>, sqlx::Error> {
    // Candidate filter uses the `%` trigram operator against the RAW columns so
    // the gin_trgm_ops indexes (shirabe_artist_name_trgm / _sortname_trgm) are
    // used; `set_limit` sets the operator's cutoff for this connection. The
    // score is the GREATEST over name + sort_name similarity so romanised /
    // native variants both rank.
    let mut conn = pool.acquire().await?;
    set_similarity_limit(&mut conn, threshold).await?;
    let rows = sqlx::query(
        r"
        SELECT a.id, a.gid, a.name,
               GREATEST(
                 similarity(a.name, $1),
                 similarity(a.sort_name, $1)
               ) AS score
        FROM musicbrainz.artist a
        WHERE a.name % $1 OR a.sort_name % $1
        ORDER BY score DESC, a.id ASC
        LIMIT $2
        ",
    )
    .bind(name)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    drop(conn);

    let mut artists = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row.try_get("id")?;
        let gid: Uuid = row.try_get("gid")?;
        let score: f32 = row.try_get("score")?;
        let aliases = load_artist_aliases(pool, id).await?;
        artists.push(Artist {
            id: gid.to_string(),
            name: row.try_get("name")?,
            score: Some(to_score(score)),
            aliases,
        });
    }
    Ok(artists)
}

async fn load_artist_aliases(pool: &PgPool, artist_id: i32) -> Result<Vec<Alias>, sqlx::Error> {
    let rows = sqlx::query(
        r"
        SELECT name, sort_name
        FROM musicbrainz.artist_alias
        WHERE artist = $1
        ORDER BY id ASC
        ",
    )
    .bind(artist_id)
    .fetch_all(pool)
    .await?;
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
    let rows = sqlx::query(
        r"
        SELECT a.id AS artist_id, a.gid AS artist_gid, acn.name AS credit_name
        FROM musicbrainz.artist_credit_name acn
        JOIN musicbrainz.artist a ON a.id = acn.artist
        WHERE acn.artist_credit = $1
        ORDER BY acn.position ASC
        ",
    )
    .bind(artist_credit_id)
    .fetch_all(pool)
    .await?;

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
    let rows = sqlx::query(
        r"
        SELECT rc.date_year::int AS y, rc.date_month::int AS m, rc.date_day::int AS d,
               (iso.code = 'XW') AS is_xw
        FROM musicbrainz.release_country rc
        LEFT JOIN musicbrainz.iso_3166_1 iso ON iso.area = rc.country
        WHERE rc.release = $1
        UNION ALL
        SELECT date_year::int, date_month::int, date_day::int, false
        FROM musicbrainz.release_unknown_country
        WHERE release = $1
        ",
    )
    .bind(release_id)
    .fetch_all(pool)
    .await?;

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

/// `GET /ws/2/release?query=release:(..) AND artist:(..) [AND date:(YYYY*)]`
pub async fn search_releases(
    pool: &PgPool,
    title: &str,
    artist: Option<&str>,
    year: Option<&str>,
    limit: i64,
    threshold: f64,
) -> Result<Vec<Release>, sqlx::Error> {
    // Combine release-title trigram score with an optional artist-credit-name
    // trigram score. The artist score, when requested, is a weighted bonus so
    // title remains the dominant signal. The candidate filter uses the `%`
    // operator on the RAW columns (release.name / artist_credit.name) so the
    // gin_trgm_ops indexes are used; the `%` cutoff is set via `set_limit`.
    let mut conn = pool.acquire().await?;
    set_similarity_limit(&mut conn, threshold).await?;
    let rows = sqlx::query(
        r"
        SELECT r.id, r.gid, r.name, r.artist_credit, r.release_group,
               ac.name AS credit_name,
               similarity(r.name, $1) AS title_score,
               CASE WHEN $2::text IS NULL THEN NULL
                    ELSE similarity(ac.name, $2) END AS artist_score
        FROM musicbrainz.release r
        JOIN musicbrainz.artist_credit ac ON ac.id = r.artist_credit
        WHERE r.name % $1
          AND ($2::text IS NULL OR ac.name % $2)
          AND ($3::int IS NULL OR EXISTS (
                SELECT 1 FROM musicbrainz.release_country rc
                WHERE rc.release = r.id AND rc.date_year = $3
                UNION ALL
                SELECT 1 FROM musicbrainz.release_unknown_country ruc
                WHERE ruc.release = r.id AND ruc.date_year = $3))
        ORDER BY (similarity(r.name, $1)
                  + COALESCE(CASE WHEN $2::text IS NULL THEN 0
                       ELSE similarity(ac.name, $2) END, 0) * 0.5) DESC,
                 r.id ASC
        LIMIT $4
        ",
    )
    .bind(title)
    .bind(artist)
    .bind(year.and_then(|y| y.parse::<i32>().ok()))
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    drop(conn);

    let mut releases = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row.try_get("id")?;
        let gid: Uuid = row.try_get("gid")?;
        let title_score: f32 = row.try_get("title_score")?;
        let artist_credit_id: i32 = row.try_get("artist_credit")?;
        let rg_id: Option<i32> = row.try_get("release_group").ok();

        let artist_credit = load_artist_credit(pool, artist_credit_id, false).await?;
        let release_group = load_release_group(pool, rg_id).await?;
        let date = release_date(pool, id).await?;
        let status = load_release_status(pool, id).await?;
        let disambiguation = load_release_comment(pool, id).await?;
        let track_count = release_track_count(pool, id).await?;

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
    let row = sqlx::query(
        r"
        SELECT rg.gid, rgpt.name AS primary_type
        FROM musicbrainz.release_group rg
        LEFT JOIN musicbrainz.release_group_primary_type rgpt ON rgpt.id = rg.type
        WHERE rg.id = $1
        ",
    )
    .bind(rg_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| {
        let gid: Uuid = r.get("gid");
        ReleaseGroup { id: gid.to_string(), primary_type: r.try_get("primary_type").ok() }
    }))
}

async fn load_release_status(
    pool: &PgPool,
    release_id: i32,
) -> Result<Option<String>, sqlx::Error> {
    let row = sqlx::query(
        r"
        SELECT rs.name
        FROM musicbrainz.release r
        JOIN musicbrainz.release_status rs ON rs.id = r.status
        WHERE r.id = $1
        ",
    )
    .bind(release_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.get("name")))
}

async fn load_release_comment(
    pool: &PgPool,
    release_id: i32,
) -> Result<Option<String>, sqlx::Error> {
    let row = sqlx::query("SELECT comment FROM musicbrainz.release WHERE id = $1")
        .bind(release_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.and_then(|r| {
        let c: String = r.get("comment");
        if c.is_empty() { None } else { Some(c) }
    }))
}

async fn release_track_count(pool: &PgPool, release_id: i32) -> Result<Option<u32>, sqlx::Error> {
    let row = sqlx::query(
        r"
        SELECT COALESCE(SUM(track_count), 0)::bigint AS total
        FROM musicbrainz.medium WHERE release = $1
        ",
    )
    .bind(release_id)
    .fetch_one(pool)
    .await?;
    let total: i64 = row.try_get("total")?;
    Ok(if total > 0 { Some(total as u32) } else { None })
}

// ── Release lookup (full) ─────────────────────────────────

/// `GET /ws/2/release/{mbid}?inc=...media+recordings+...rels`
pub async fn lookup_release(pool: &PgPool, gid: Uuid) -> Result<Option<Release>, sqlx::Error> {
    let Some(row) = sqlx::query(
        r"
        SELECT r.id, r.gid, r.name, r.artist_credit, r.release_group
        FROM musicbrainz.release r
        WHERE r.gid = $1
        ",
    )
    .bind(gid)
    .fetch_optional(pool)
    .await?
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
    let rows = sqlx::query(
        r"
        SELECT m.id, m.position, m.track_count, m.name AS title, mf.name AS format
        FROM musicbrainz.medium m
        LEFT JOIN musicbrainz.medium_format mf ON mf.id = m.format
        WHERE m.release = $1
        ORDER BY m.position ASC
        ",
    )
    .bind(release_id)
    .fetch_all(pool)
    .await?;

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
    let rows = sqlx::query(
        r"
        SELECT t.gid AS track_gid, t.name AS track_name, t.position, t.number,
               t.artist_credit AS track_ac,
               rec.gid AS rec_gid, rec.name AS rec_name, rec.length AS rec_length
        FROM musicbrainz.track t
        JOIN musicbrainz.recording rec ON rec.id = t.recording
        WHERE t.medium = $1
        ORDER BY t.position ASC
        ",
    )
    .bind(medium_id)
    .fetch_all(pool)
    .await?;

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
    let rows = sqlx::query(
        r"
        SELECT 'forward' AS direction, r1.gid AS gid, r1.name AS name
        FROM musicbrainz.l_release_release l
        JOIN musicbrainz.release r1 ON r1.id = l.entity1
        WHERE l.entity0 = $1
        UNION ALL
        SELECT 'backward' AS direction, r0.gid AS gid, r0.name AS name
        FROM musicbrainz.l_release_release l
        JOIN musicbrainz.release r0 ON r0.id = l.entity0
        WHERE l.entity1 = $1
        ",
    )
    .bind(release_id)
    .fetch_all(pool)
    .await?;

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

// ── Recording search ──────────────────────────────────────

/// `GET /ws/2/recording?query=recording:".." AND artist:".."&inc=releases+...+media`
pub async fn search_recordings(
    pool: &PgPool,
    title: &str,
    artist: Option<&str>,
    limit: i64,
    threshold: f64,
) -> Result<Vec<Recording>, sqlx::Error> {
    // Candidate filter uses the `%` operator on the RAW recording.name /
    // artist_credit.name columns so the gin_trgm_ops indexes are used; cutoff
    // set via `set_limit` on this connection.
    let mut conn = pool.acquire().await?;
    set_similarity_limit(&mut conn, threshold).await?;
    let rows = sqlx::query(
        r"
        SELECT rec.id, rec.gid, rec.name, rec.length, rec.artist_credit,
               similarity(rec.name, $1) AS title_score
        FROM musicbrainz.recording rec
        JOIN musicbrainz.artist_credit ac ON ac.id = rec.artist_credit
        WHERE rec.name % $1
          AND ($2::text IS NULL OR ac.name % $2)
        ORDER BY (similarity(rec.name, $1)
                  + COALESCE(CASE WHEN $2::text IS NULL THEN 0
                       ELSE similarity(ac.name, $2) END, 0) * 0.5) DESC,
                 rec.id ASC
        LIMIT $3
        ",
    )
    .bind(title)
    .bind(artist)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    drop(conn);

    let mut recordings = Vec::with_capacity(rows.len());
    for row in rows {
        let rec_id: i32 = row.try_get("id")?;
        let gid: Uuid = row.try_get("gid")?;
        let title_score: f32 = row.try_get("title_score")?;
        let ac_id: i32 = row.try_get("artist_credit")?;

        let artist_credit = load_artist_credit(pool, ac_id, true).await?;
        // inc=releases+media: full release shapes incl. media/tracks.
        let releases = load_recording_releases(pool, rec_id, true).await?;

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
    let Some(row) = sqlx::query(
        r"
        SELECT rec.id, rec.gid, rec.name, rec.length, rec.artist_credit
        FROM musicbrainz.recording rec
        WHERE rec.gid = $1
        ",
    )
    .bind(gid)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };

    let rec_id: i32 = row.try_get("id")?;
    let ac_id: i32 = row.try_get("artist_credit")?;
    let artist_credit = load_artist_credit(pool, ac_id, true).await?;
    // Lookup does not request media; emit lightweight release shapes.
    let releases = load_recording_releases(pool, rec_id, false).await?;

    Ok(Some(Recording {
        id: gid.to_string(),
        title: row.try_get("name")?,
        length: row.try_get("length").ok(),
        score: None,
        artist_credit,
        releases,
    }))
}

/// All releases that contain a recording (via medium/track). When `with_media`
/// is set, each release carries its media/tracks (recording search needs this).
async fn load_recording_releases(
    pool: &PgPool,
    recording_id: i32,
    with_media: bool,
) -> Result<Vec<Release>, sqlx::Error> {
    let rows = sqlx::query(
        r"
        SELECT DISTINCT r.id, r.gid, r.name, r.artist_credit, r.release_group
        FROM musicbrainz.release r
        JOIN musicbrainz.medium m ON m.release = r.id
        JOIN musicbrainz.track t ON t.medium = m.id
        WHERE t.recording = $1
        ORDER BY r.id ASC
        ",
    )
    .bind(recording_id)
    .fetch_all(pool)
    .await?;

    // DISTINCT on r.id already guards against duplicate releases.
    let mut releases = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i32 = row.try_get("id")?;
        let gid: Uuid = row.try_get("gid")?;
        let artist_credit_id: i32 = row.try_get("artist_credit")?;
        let rg_id: Option<i32> = row.try_get("release_group").ok();

        let artist_credit = load_artist_credit(pool, artist_credit_id, false).await?;
        let release_group = load_release_group(pool, rg_id).await?;
        let date = release_date(pool, id).await?;
        let status = load_release_status(pool, id).await?;
        let disambiguation = load_release_comment(pool, id).await?;
        let media = if with_media { load_media(pool, id).await? } else { Vec::new() };
        let track_count = if media.is_empty() {
            release_track_count(pool, id).await?
        } else {
            Some(media.iter().map(|m| m.track_count).sum())
        };

        releases.push(Release {
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
    Ok(releases)
}

/// Cheap connectivity probe used by the health endpoint.
pub async fn ping(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT 1").execute(pool).await.map(|_| ())
}
