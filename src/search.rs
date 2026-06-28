//! Local-first search + ranking across the writable index DBs (SHIB-10).
//!
//! The `/3` (TMDB) and `/v4` (TVDB) facades route a search query to the LOCAL
//! index FIRST, falling through to the live upstream API only on a thin or empty
//! local result, then MERGE both (dedupe by id). The local index is assembled,
//! per-pool, in Rust — the IMDb tables live in the `imdb` database and the
//! `shirabe.tmdb_id_index` / caches live in the `shirabe` database, which are two
//! SEPARATE Postgres databases (Option A), so they cannot be SQL-joined. Each
//! pool is queried independently and the hits are merged / ranked here.
//!
//! Non-latin resolution (e.g. 銀魂 → Gintama) rides the pg_trgm GIN index on
//! `imdb_title_akas.title` plus `imdb_title_basics.primary_title/original_title`
//! and the `shirabe.tmdb_id_index.name` — the same fields Kusaritoi re-scores
//! against. Scores are synthesised from pg_trgm similarity into the same 0-100
//! range MusicBrainz search emits (see [`crate::repo`]), so Kusaritoi's
//! confidence filter is unchanged; TMDB popularity breaks ties.
//!
//! Graceful degradation: when a writable pool is absent, that pool's local search
//! simply yields nothing, and the facade falls through to the live API (which may
//! itself be key-gated and yield nothing — never a panic).

use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres, Row};

/// Default pg_trgm `%` cutoff for local search candidate filtering. Matches the
/// permissive end of the MB search thresholds so romanised/native variants both
/// surface.
pub const LOCAL_SIMILARITY_THRESHOLD: f64 = 0.3;

/// A local result is considered "thin" (→ fall through to the live API and merge)
/// when it has fewer than this many hits…
pub const THIN_RESULT_MIN_HITS: usize = 3;

/// …or when its best score is below this 0-100 floor.
pub const THIN_RESULT_MIN_TOP_SCORE: i32 = 60;

/// Scale a pg_trgm similarity (0.0-1.0) into a MusicBrainz-style score (0-100).
///
/// Mirrors `repo::to_score` exactly so local hits rank on the same 0-100 scale as
/// the `/ws/2` search endpoints (Kusaritoi's confidence re-scoring stays
/// unchanged). `similarity()` returns Postgres `real` (FLOAT4), decoded as `f32`;
/// we widen to `f64` only for the arithmetic.
#[must_use]
pub fn similarity_to_score(similarity: f32) -> i32 {
    (f64::from(similarity) * 100.0).round().clamp(0.0, 100.0) as i32
}

/// One ranked local hit, provider-agnostic. `id` is the backing-store id rendered
/// as a string (a TMDB numeric id or an IMDb `tconst`); `name` is the matched
/// display name; `score` is the 0-100 synthesised similarity; `popularity` (when
/// known, from `tmdb_id_index`) breaks ranking ties.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoredHit {
    pub id: String,
    pub name: String,
    pub score: i32,
    pub popularity: Option<f64>,
    pub adult: Option<bool>,
}

/// Order two hits for ranking: higher score first, then higher popularity, then a
/// stable id tie-break so the ordering is deterministic.
fn rank_cmp(a: &ScoredHit, b: &ScoredHit) -> std::cmp::Ordering {
    b.score
        .cmp(&a.score)
        .then_with(|| {
            let pa = a.popularity.unwrap_or(0.0);
            let pb = b.popularity.unwrap_or(0.0);
            pb.partial_cmp(&pa).unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| a.id.cmp(&b.id))
}

/// Merge several already-scored hit lists into one ranked list, **deduped by id**.
/// When the same id appears in more than one source, the highest-scoring instance
/// wins (and carries the better popularity, preferring a present value). The
/// result is sorted by [`rank_cmp`] (score desc, popularity desc, id asc).
#[must_use]
pub fn merge_hits(sources: Vec<Vec<ScoredHit>>) -> Vec<ScoredHit> {
    use std::collections::HashMap;
    let mut best: HashMap<String, ScoredHit> = HashMap::new();
    for list in sources {
        for hit in list {
            best.entry(hit.id.clone())
                .and_modify(|existing| {
                    if hit.score > existing.score {
                        existing.score = hit.score;
                        existing.name.clone_from(&hit.name);
                    }
                    // Prefer a present popularity; if both present, keep the larger.
                    existing.popularity = match (existing.popularity, hit.popularity) {
                        (Some(a), Some(b)) => Some(a.max(b)),
                        (a, b) => a.or(b),
                    };
                    existing.adult = existing.adult.or(hit.adult);
                })
                .or_insert(hit);
        }
    }
    let mut merged: Vec<ScoredHit> = best.into_values().collect();
    merged.sort_by(rank_cmp);
    merged
}

/// Should the facade fall through to the live upstream API and merge? True when
/// the local result is empty, has fewer than [`THIN_RESULT_MIN_HITS`] hits, or its
/// top score is below [`THIN_RESULT_MIN_TOP_SCORE`].
#[must_use]
pub fn is_thin_result(hits: &[ScoredHit]) -> bool {
    if hits.len() < THIN_RESULT_MIN_HITS {
        return true;
    }
    let top = hits.iter().map(|h| h.score).max().unwrap_or(0);
    top < THIN_RESULT_MIN_TOP_SCORE
}

/// Set the session pg_trgm `%` cutoff on a single connection (the `%` operator
/// reads this GUC, so it must run on the same connection as the search).
async fn set_similarity_limit(
    conn: &mut PoolConnection<Postgres>,
    threshold: f64,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_limit($1)").bind(threshold as f32).execute(&mut **conn).await?;
    Ok(())
}

/// Trigram-search the `shirabe.tmdb_id_index` for one `kind` (`movie`/`tv`),
/// scoring on `name` and carrying `popularity`/`adult` for ranking + native shape.
/// Returns an empty vec (not an error) is reserved for genuine emptiness; DB
/// errors propagate so the caller can decide to fall through.
async fn search_tmdb_id_index(
    pool: &PgPool,
    query: &str,
    kind: &str,
    limit: i64,
    threshold: f64,
) -> Result<Vec<ScoredHit>, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    set_similarity_limit(&mut conn, threshold).await?;
    let rows = sqlx::query(
        r"
        SELECT id, name, popularity, adult,
               similarity(name, $1) AS score
        FROM shirabe.tmdb_id_index
        WHERE kind = $2 AND name % $1
        ORDER BY score DESC, popularity DESC NULLS LAST, id ASC
        LIMIT $3
        ",
    )
    .bind(query)
    .bind(kind)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    drop(conn);

    Ok(rows
        .into_iter()
        .map(|r| {
            let id: i64 = r.get("id");
            let score: f32 = r.get("score");
            let popularity: Option<f32> = r.try_get("popularity").ok();
            ScoredHit {
                id: id.to_string(),
                name: r.get("name"),
                score: similarity_to_score(score),
                popularity: popularity.map(f64::from),
                adult: r.try_get("adult").ok(),
            }
        })
        .collect())
}

/// Trigram-search the IMDb mirror for titles matching `query`, scoring over
/// `primary_title`, `original_title`, and any `title.akas.title` (the non-latin
/// path — 銀魂 resolves to its tconst here). One row per `tconst`, scored by the
/// GREATEST similarity across the three columns. `kind_filter`, when set, narrows
/// `imdb_title_basics.title_type` (e.g. `tvSeries` / `movie`); `None` searches all.
async fn search_imdb_titles(
    pool: &PgPool,
    query: &str,
    title_types: &[&str],
    limit: i64,
    threshold: f64,
) -> Result<Vec<ScoredHit>, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    set_similarity_limit(&mut conn, threshold).await?;
    // Candidate tconsts come from EITHER a basics-title match OR an akas-title
    // match (the akas GIN index carries the non-latin variants). We then take the
    // GREATEST similarity over primary/original/aka for the score. `$3` is a
    // (possibly empty) title_type allow-list applied only to basics rows.
    let rows = sqlx::query(
        r"
        WITH basics_hit AS (
            SELECT b.tconst,
                   GREATEST(
                     similarity(b.primary_title, $1),
                     similarity(coalesce(b.original_title, ''), $1)
                   ) AS s
            FROM imdb_title_basics b
            WHERE (b.primary_title % $1 OR b.original_title % $1)
              AND (cardinality($3::text[]) = 0 OR b.title_type = ANY($3))
        ),
        akas_hit AS (
            SELECT a.title_id AS tconst, max(similarity(a.title, $1)) AS s
            FROM imdb_title_akas a
            WHERE a.title % $1
            GROUP BY a.title_id
        ),
        unioned AS (
            SELECT tconst, s FROM basics_hit
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
        ",
    )
    .bind(query)
    .bind(limit)
    .bind(title_types)
    .fetch_all(&mut *conn)
    .await?;
    drop(conn);

    Ok(rows
        .into_iter()
        .map(|r| {
            let score: f32 = r.get("score");
            ScoredHit {
                id: r.get("tconst"),
                name: r.get("name"),
                score: similarity_to_score(score),
                popularity: None,
                adult: None,
            }
        })
        .collect())
}

/// IMDb `title_type` values that correspond to a TMDB `kind`.
fn imdb_title_types(kind: &str) -> &'static [&'static str] {
    match kind {
        "movie" => &["movie", "tvMovie", "short", "video"],
        // tv
        _ => &["tvSeries", "tvMiniSeries"],
    }
}

/// Run the LOCAL TMDB-kind search across both writable pools and merge.
///
/// Queries `shirabe.tmdb_id_index` (via `shirabe_pool`) and the IMDb mirror (via
/// `imdb_pool`) independently — they are separate databases — then merges /
/// dedupes / ranks in Rust. Absent pools simply contribute nothing. DB errors on
/// either pool are swallowed to an empty contribution so a half-provisioned
/// deployment degrades to the live API rather than 500-ing.
pub async fn local_tmdb_search(
    imdb_pool: Option<&PgPool>,
    shirabe_pool: Option<&PgPool>,
    query: &str,
    kind: &str,
    limit: i64,
) -> Vec<ScoredHit> {
    let mut sources: Vec<Vec<ScoredHit>> = Vec::new();

    if let Some(pool) = shirabe_pool {
        match search_tmdb_id_index(pool, query, kind, limit, LOCAL_SIMILARITY_THRESHOLD).await {
            Ok(hits) => sources.push(hits),
            Err(e) => tracing::warn!(error = %e, kind, "local tmdb_id_index search failed"),
        }
    }
    if let Some(pool) = imdb_pool {
        let types = imdb_title_types(kind);
        match search_imdb_titles(pool, query, types, limit, LOCAL_SIMILARITY_THRESHOLD).await {
            Ok(hits) => sources.push(hits),
            Err(e) => tracing::warn!(error = %e, kind, "local imdb title search failed"),
        }
    }

    let mut merged = merge_hits(sources);
    merged.truncate(limit.max(0) as usize);
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(id: &str, score: i32, pop: Option<f64>) -> ScoredHit {
        ScoredHit { id: id.to_string(), name: id.to_string(), score, popularity: pop, adult: None }
    }

    // ── score synthesis (similarity 0..1 → 0..100) ──────────────

    #[test]
    fn similarity_scales_to_0_100() {
        assert_eq!(similarity_to_score(0.0), 0);
        assert_eq!(similarity_to_score(1.0), 100);
        assert_eq!(similarity_to_score(0.5), 50);
        assert_eq!(similarity_to_score(0.333), 33);
        assert_eq!(similarity_to_score(0.666), 67); // rounds
    }

    #[test]
    fn similarity_score_is_clamped() {
        // Defensive: out-of-range inputs never escape the 0-100 band.
        assert_eq!(similarity_to_score(-0.5), 0);
        assert_eq!(similarity_to_score(1.5), 100);
    }

    #[test]
    fn matches_repo_score_scale() {
        // Same synthesis the MB search endpoints use, so Kusaritoi's confidence
        // filter behaves identically on local hits.
        for raw in [0.0_f32, 0.2, 0.41, 0.5, 0.75, 0.9, 1.0] {
            let expected = (f64::from(raw) * 100.0).round().clamp(0.0, 100.0) as i32;
            assert_eq!(similarity_to_score(raw), expected);
        }
    }

    // ── thin-result / fall-through decision ─────────────────────

    #[test]
    fn empty_local_result_is_thin() {
        assert!(is_thin_result(&[]));
    }

    #[test]
    fn too_few_hits_is_thin() {
        // Below THIN_RESULT_MIN_HITS triggers fall-through even with a great score.
        let hits = vec![hit("a", 100, None), hit("b", 95, None)];
        assert!(hits.len() < THIN_RESULT_MIN_HITS);
        assert!(is_thin_result(&hits));
    }

    #[test]
    fn enough_hits_but_low_top_score_is_thin() {
        let hits = vec![hit("a", 50, None), hit("b", 40, None), hit("c", 30, None)];
        assert!(is_thin_result(&hits)); // top 50 < 60 floor
    }

    #[test]
    fn enough_strong_hits_is_not_thin() {
        let hits = vec![hit("a", 90, None), hit("b", 70, None), hit("c", 65, None)];
        assert!(!is_thin_result(&hits)); // 3 hits, top 90 ≥ 60 → serve local, no live call
    }

    // ── merge / dedupe-by-id + popularity tie-break ─────────────

    #[test]
    fn merge_dedupes_by_id_keeping_best_score() {
        let a = vec![hit("1", 60, Some(1.0)), hit("2", 80, None)];
        let b = vec![hit("1", 90, Some(5.0))]; // same id, higher score
        let merged = merge_hits(vec![a, b]);
        assert_eq!(merged.len(), 2); // id "1" appears once
        let one = merged.iter().find(|h| h.id == "1").unwrap();
        assert_eq!(one.score, 90); // best score wins
        assert_eq!(one.popularity, Some(5.0)); // larger popularity carried
    }

    #[test]
    fn merge_orders_by_score_then_popularity_then_id() {
        let merged = merge_hits(vec![vec![
            hit("z", 80, Some(1.0)),
            hit("a", 80, Some(9.0)), // same score, higher popularity → ranks first
            hit("m", 90, None),      // highest score → overall first
            hit("b", 80, Some(9.0)), // ties a on score+pop → id breaks (a before b)
        ]]);
        let ids: Vec<&str> = merged.iter().map(|h| h.id.as_str()).collect();
        assert_eq!(ids, vec!["m", "a", "b", "z"]);
    }

    #[test]
    fn merge_breaks_pure_score_ties_by_popularity() {
        // Popularity is the tie-break when scores are equal (TMDB ranking rule).
        let merged = merge_hits(vec![vec![hit("low", 70, Some(2.0)), hit("high", 70, Some(50.0))]]);
        assert_eq!(merged[0].id, "high");
    }

    #[test]
    fn merge_handles_empty_sources() {
        assert!(merge_hits(vec![]).is_empty());
        assert!(merge_hits(vec![vec![], vec![]]).is_empty());
    }
}
