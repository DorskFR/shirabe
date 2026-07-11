//! Local-first search + ranking across the writable index DBs (SHIB-10).
//!
//! The `/3` (TMDB) and `/v4` (TVDB) facades route a search query to the LOCAL
//! index FIRST, falling through to the live upstream API only on a thin or empty
//! local result, then MERGE both (dedupe by id). The local index is assembled,
//! per-pool, in Rust — the IMDb tables live in the `imdb` database and the
//! `tmdb_id_index` / cache live in the dedicated `tmdb` database, which are
//! SEPARATE Postgres databases (five-DB layout), so they cannot be SQL-joined.
//! Each pool is queried independently and the hits are merged / ranked here.
//!
//! Non-latin resolution (e.g. 銀魂 → Gintama) rides the pg_trgm GIN index on
//! `imdb_title_akas.title` plus `imdb_title_basics.primary_title/original_title`
//! and the `tmdb_id_index.name` — the same fields Kusaritoi re-scores
//! against. Scores are synthesised from pg_trgm similarity into the same 0-100
//! range MusicBrainz search emits (see [`crate::repo`]), so Kusaritoi's
//! confidence filter is unchanged; TMDB popularity breaks ties.
//!
//! Graceful degradation: when a writable pool is absent, that pool's local search
//! simply yields nothing, and the facade falls through to the live API (which may
//! itself be key-gated and yield nothing — never a panic).

use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{PgPool, Postgres, Row};

use crate::queries;

/// Run a trigram fuzzy-fallback search (used only when the FTS primary matched
/// nothing), time-boxed to [`FUZZY_STATEMENT_TIMEOUT_MS`]. Hitting the cap
/// yields no rows (SQLSTATE 57014) rather than erroring the whole request.
async fn fetch_fuzzy(
    query: sqlx::query::Query<'_, Postgres, PgArguments>,
    conn: &mut PoolConnection<Postgres>,
) -> Result<Vec<PgRow>, sqlx::Error> {
    sqlx::query(&format!("SET statement_timeout = {FUZZY_STATEMENT_TIMEOUT_MS}"))
        .execute(&mut **conn)
        .await?;
    let result = match query.fetch_all(&mut **conn).await {
        Ok(rows) => Ok(rows),
        Err(e)
            if e.as_database_error().and_then(sqlx::error::DatabaseError::code).as_deref()
                == Some("57014") =>
        {
            Ok(Vec::new())
        }
        Err(e) => Err(e),
    };
    // The pooled connection must not carry the tight cap into later queries.
    let _ = sqlx::query(&format!("SET statement_timeout = {DEFAULT_STATEMENT_TIMEOUT_MS}"))
        .execute(&mut **conn)
        .await;
    result
}

/// Default pg_trgm `%` cutoff for local search candidate filtering. Matches the
/// permissive end of the MB search thresholds so romanised/native variants both
/// surface.
pub const LOCAL_SIMILARITY_THRESHOLD: f64 = 0.3;

/// Floor on the pg_trgm `%` cutoff for queries probing the IMDb tables (SHIB-16).
///
/// `imdb_title_akas` holds ~58M rows; at a 0.2 cutoff a short query like
/// "inception" pulls ~1.5M trigram candidates out of the GIN index, the bitmap
/// goes lossy at default `work_mem`, and the heap recheck touches tens of
/// millions of rows (~55s cold on prod — past Kusaritoi's 30s HTTP timeout).
/// Clamping the probe to 0.4 cuts the candidate set to a sane size (~6.5s with
/// the raised `work_mem`) while exact-name recall stays intact (an exact aka
/// match like "Gintama" still scores 1.0). Applies ONLY to the IMDb probes; the
/// MusicBrainz search paths keep the configured threshold unchanged.
pub const IMDB_PROBE_MIN_THRESHOLD: f64 = 0.4;

/// Fallback `work_mem` when the configured value sanitises to nothing.
const DEFAULT_SEARCH_WORK_MEM: &str = "256MB";

/// Per-connection `statement_timeout` (ms) for trigram search sessions (SHIB-19).
///
/// A runaway trigram query would otherwise run unbounded server-side and only die
/// at the client's request cap; this caps it server-side so it fails fast with a
/// clear Postgres error on the SAME connection that runs the search. Mirrors the
/// `SHIRABE_STATEMENT_TIMEOUT_MS` config default.
const DEFAULT_STATEMENT_TIMEOUT_MS: i64 = 10_000;

/// Tighter `statement_timeout` (ms) for the trigram fuzzy fallback only: a KNN
/// scan that hasn't answered in 3s will not answer well, and it must not hold
/// the request for the full session cap.
const FUZZY_STATEMENT_TIMEOUT_MS: i64 = 3_000;

/// Clamp a configured similarity threshold to the IMDb probe floor
/// ([`IMDB_PROBE_MIN_THRESHOLD`]). Pure so it is unit-testable.
#[must_use]
pub const fn imdb_probe_threshold(configured: f64) -> f64 {
    configured.max(IMDB_PROBE_MIN_THRESHOLD)
}

/// Sanitise a configured `work_mem` value for splicing into `SET work_mem = '…'`.
///
/// `SET` cannot take a bind parameter, so the value ends up in the SQL text —
/// and it arrives from an env var (`SHIRABE_SEARCH_WORK_MEM`), so it must not be
/// able to smuggle SQL. Only ASCII alphanumerics survive (every valid Postgres
/// memory setting — `64000`, `256MB`, `1GB` — is alphanumeric); anything that
/// sanitises to a value not starting with a digit falls back to
/// [`DEFAULT_SEARCH_WORK_MEM`].
#[must_use]
pub fn sanitize_work_mem(value: &str) -> String {
    let cleaned: String = value.chars().filter(char::is_ascii_alphanumeric).collect();
    if cleaned.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        cleaned
    } else {
        DEFAULT_SEARCH_WORK_MEM.to_string()
    }
}

/// Lucene-ish field prefixes users type into consumer search boxes (MusicBrainz
/// ws/2 habit). Whitelisted so titles containing a colon ("Mission: Impossible",
/// "RE:BORN", "8:46") are never mistaken for field syntax.
const LUCENE_FIELD_PREFIXES: &[&str] =
    &["title", "name", "originaltitle", "primarytitle", "artist", "release", "recording", "alias"];

/// Normalize a consumer-typed search query for the provider facades: strip a
/// whitelisted field prefix (`title:` / `title=`) and drop double quotes
/// (`title:"dancer in the dark"` → `dancer in the dark`). Neither the
/// FTS/trigram pipeline nor the upstream TMDB/TVDB APIs understand field
/// syntax. A NEGATED whitelisted field (`title!="x"`) returns `None`: a text
/// search cannot express exclusion, and answering it with matches FOR the
/// value would be the opposite of what was asked.
#[must_use]
pub fn normalize_query(raw: &str) -> Option<String> {
    let mut q = raw.trim();
    if let Some(sep) = q.find([':', '=']) {
        let (mut head, mut rest) = (&q[..sep], &q[sep + 1..]);
        if q.as_bytes()[sep] == b'=' && rest.starts_with('=') {
            rest = &rest[1..];
        }
        let negated = head.ends_with('!');
        if negated {
            head = &head[..head.len() - 1];
        }
        let field: String =
            head.trim().chars().filter(|c| *c != '_').collect::<String>().to_ascii_lowercase();
        // The whitelist is what keeps colon titles safe, so the colon-space
        // form (`title: dune`) can be stripped too.
        if LUCENE_FIELD_PREFIXES.contains(&field.as_str()) {
            if negated {
                return None;
            }
            q = rest.trim_start();
        }
    }
    let cleaned: String = q.chars().map(|c| if c == '"' { ' ' } else { c }).collect();
    Some(cleaned.split_whitespace().collect::<Vec<_>>().join(" "))
}

/// A local result is considered "thin" (→ fall through to the live API and merge)
/// when it has fewer than this many STRONG hits…
pub const THIN_RESULT_MIN_HITS: usize = 3;

/// …where a hit only counts as strong when its 0-100 score reaches this floor.
/// Weak pg_trgm hits (e.g. "Gina" for the query "gintama") never make a local
/// result confident, no matter how many of them there are (SHIB-15).
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

impl ScoredHit {
    /// The hit's TMDB numeric id, when it has one. IMDb-tconst hits (`tt…`) that
    /// were not cross-referenced to a TMDB id return `None` — they cannot be
    /// rendered as native TMDB results and must not count toward the thinness
    /// gate (SHIB-15: a strong-but-unrenderable hit would otherwise suppress the
    /// live merge while being dropped from the payload).
    #[must_use]
    pub fn tmdb_id(&self) -> Option<i64> {
        self.id.parse().ok()
    }
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

/// Should the facade fall through to the live upstream API and merge? True unless
/// the local result carries at least [`THIN_RESULT_MIN_HITS`] STRONG hits (score ≥
/// [`THIN_RESULT_MIN_TOP_SCORE`]). Weak-similarity-only results — however many —
/// are always thin, so a configured upstream key always gets a chance to correct
/// junk trigram matches (SHIB-15). Callers must pass only hits they can actually
/// render (see [`ScoredHit::tmdb_id`]).
#[must_use]
pub fn is_thin_result(hits: &[ScoredHit]) -> bool {
    let strong = hits.iter().filter(|h| h.score >= THIN_RESULT_MIN_TOP_SCORE).count();
    strong < THIN_RESULT_MIN_HITS
}

/// Configure a search session on a single connection: the pg_trgm `%` cutoff
/// (the operator reads this GUC, so it must run on the same connection as the
/// search), `work_mem` (SHIB-16 — the default 4MB makes big GIN bitmap scans
/// go lossy and recheck the heap), and `statement_timeout` (SHIB-19 — bound
/// runaway queries server-side so they fail fast). `work_mem` cannot be bound as
/// a parameter in `SET`, so the value is sanitised via [`sanitize_work_mem`]
/// before splicing; `statement_timeout` is a fixed integer count of milliseconds
/// ([`DEFAULT_STATEMENT_TIMEOUT_MS`]), so it is spliced directly (no injection
/// surface).
pub async fn configure_search_session(
    conn: &mut PoolConnection<Postgres>,
    threshold: f64,
    work_mem: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_limit($1)").bind(threshold as f32).execute(&mut **conn).await?;
    sqlx::query(&format!("SET work_mem = '{}'", sanitize_work_mem(work_mem)))
        .execute(&mut **conn)
        .await?;
    // `statement_timeout` as a bare integer is interpreted by Postgres as ms.
    // Set on THIS connection so the search query below inherits the cap.
    sqlx::query(&format!("SET statement_timeout = {DEFAULT_STATEMENT_TIMEOUT_MS}"))
        .execute(&mut **conn)
        .await?;
    Ok(())
}

/// Trigram-search the `tmdb_id_index` for one `kind` (`movie`/`tv`),
/// scoring on `name` and carrying `popularity`/`adult` for ranking + native shape.
/// Returns an empty vec (not an error) is reserved for genuine emptiness; DB
/// errors propagate so the caller can decide to fall through.
async fn search_tmdb_id_index(
    pool: &PgPool,
    query: &str,
    kind: &str,
    limit: i64,
    threshold: f64,
    work_mem: &str,
) -> Result<Vec<ScoredHit>, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    configure_search_session(&mut conn, threshold, work_mem).await?;
    // FTS primary (SHIB-24); fall back to the trigram fuzzy query only when FTS
    // matched nothing (typos / partials), bounded by the session statement_timeout.
    let mut rows = sqlx::query(queries::SEARCH_TMDB_ID_INDEX)
        .bind(query)
        .bind(kind)
        .bind(limit)
        .fetch_all(&mut *conn)
        .await?;
    if rows.is_empty() {
        rows = fetch_fuzzy(
            sqlx::query(queries::SEARCH_TMDB_ID_INDEX_FUZZY).bind(query).bind(kind).bind(limit),
            &mut conn,
        )
        .await?;
    }
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
    work_mem: &str,
) -> Result<Vec<ScoredHit>, sqlx::Error> {
    let mut conn = pool.acquire().await?;
    // The IMDb tables are probed with `%`, so the cutoff is clamped to the
    // probe floor — see [`IMDB_PROBE_MIN_THRESHOLD`] (SHIB-16).
    configure_search_session(&mut conn, imdb_probe_threshold(threshold), work_mem).await?;
    // Candidate tconsts come from EITHER a basics-title match OR an akas-title
    // match (the akas GIN index carries the non-latin variants). We then take the
    // GREATEST similarity over primary/original/aka for the score. `$3` is a
    // (possibly empty) title_type allow-list applied only to basics rows.
    // FTS primary (SHIB-24); fall back to the trigram/gist-KNN fuzzy query only
    // when FTS matched nothing, bounded by the session statement_timeout (a
    // pathological common-token fallback yields no rows rather than erroring).
    let mut rows = sqlx::query(queries::SEARCH_IMDB_TITLES)
        .bind(query)
        .bind(limit)
        .bind(title_types)
        .fetch_all(&mut *conn)
        .await?;
    if rows.is_empty() {
        rows = fetch_fuzzy(
            sqlx::query(queries::SEARCH_IMDB_TITLES_FUZZY)
                .bind(query)
                .bind(limit)
                .bind(title_types),
            &mut conn,
        )
        .await?;
    }
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

/// Ranking metadata for a TMDB id, read from `tmdb_id_index` (native display
/// name, popularity, adult flag) to enrich a cross-referenced IMDb hit.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IndexMeta {
    pub name: Option<String>,
    pub popularity: Option<f64>,
    pub adult: Option<bool>,
}

/// Map IMDb tconsts to TMDB ids using previously hydrated `tmdb_cache` detail
/// payloads: a tv/movie detail carries `external_ids.imdb_id` (movies also a
/// top-level `imdb_id`), so any title Shirabe has ever hydrated resolves locally
/// with no upstream call (SHIB-15). `kind` is the detail cache kind (`tv` /
/// `movie`). Backed by the `tmdb_cache_kind_imdb_id_idx` expression index.
async fn resolve_tconsts_via_cache(
    pool: &PgPool,
    tconsts: &[String],
    kind: &str,
) -> Result<std::collections::HashMap<String, i64>, sqlx::Error> {
    if tconsts.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let rows = sqlx::query(queries::RESOLVE_TCONSTS_VIA_CACHE)
        .bind(kind)
        .bind(tconsts)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let imdb_id: Option<String> = r.try_get("imdb_id").ok()?;
            let id: i64 = r.try_get("id").ok()?;
            imdb_id.map(|t| (t, id))
        })
        .collect())
}

/// Fetch [`IndexMeta`] for a set of TMDB ids from `tmdb_id_index`.
async fn index_meta_for_ids(
    pool: &PgPool,
    ids: &[i64],
    kind: &str,
) -> Result<std::collections::HashMap<i64, IndexMeta>, sqlx::Error> {
    if ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let rows =
        sqlx::query(queries::INDEX_META_FOR_IDS).bind(kind).bind(ids).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let id: i64 = r.get("id");
            let popularity: Option<f32> = r.try_get("popularity").ok();
            let meta = IndexMeta {
                name: r.try_get("name").ok(),
                popularity: popularity.map(f64::from),
                adult: r.try_get("adult").ok(),
            };
            (id, meta)
        })
        .collect())
}

/// Pure application of a tconst→TMDB-id resolution map (+ id-index enrichment) to
/// IMDb hits. A resolved hit keeps its similarity score (the akas match IS the
/// evidence) but takes the TMDB id — making it renderable as a native result and
/// dedupable against `tmdb_id_index` hits — plus the index's native display name
/// / popularity / adult flag when known. Unresolved tconst hits pass through
/// unchanged (they are skipped at render time and never count as strong local
/// evidence). Unit-testable without a DB.
#[must_use]
pub fn apply_tconst_resolution(
    hits: Vec<ScoredHit>,
    resolved: &std::collections::HashMap<String, i64>,
    meta: &std::collections::HashMap<i64, IndexMeta>,
) -> Vec<ScoredHit> {
    hits.into_iter()
        .map(|mut hit| {
            let Some(&tmdb_id) = resolved.get(&hit.id) else {
                return hit;
            };
            hit.id = tmdb_id.to_string();
            if let Some(m) = meta.get(&tmdb_id) {
                if let Some(name) = &m.name {
                    hit.name.clone_from(name);
                }
                hit.popularity = m.popularity.or(hit.popularity);
                hit.adult = m.adult.or(hit.adult);
            }
            hit
        })
        .collect()
}

/// Cross-reference IMDb-tconst hits to TMDB ids via the local `tmdb_cache`
/// (see [`resolve_tconsts_via_cache`]) and enrich from `tmdb_id_index`.
/// Best-effort: with no tmdb pool or on a DB error the hits pass through
/// unresolved (degraded, never an error).
async fn resolve_imdb_hits(
    tmdb_pool: Option<&PgPool>,
    hits: Vec<ScoredHit>,
    kind: &str,
) -> Vec<ScoredHit> {
    let Some(pool) = tmdb_pool else {
        return hits;
    };
    let tconsts: Vec<String> =
        hits.iter().filter(|h| h.tmdb_id().is_none()).map(|h| h.id.clone()).collect();
    if tconsts.is_empty() {
        return hits;
    }
    let resolved = match resolve_tconsts_via_cache(pool, &tconsts, kind).await {
        Ok(map) => map,
        Err(e) => {
            tracing::warn!(error = %e, kind, "tconst → tmdb id cache resolution failed");
            return hits;
        }
    };
    let ids: Vec<i64> = resolved.values().copied().collect();
    let meta = match index_meta_for_ids(pool, &ids, kind).await {
        Ok(map) => map,
        Err(e) => {
            tracing::warn!(error = %e, kind, "tmdb_id_index enrichment failed");
            std::collections::HashMap::new()
        }
    };
    apply_tconst_resolution(hits, &resolved, &meta)
}

/// Run the LOCAL TMDB-kind search across both writable pools and merge.
///
/// Queries `tmdb_id_index` (via `tmdb_pool`, the dedicated `tmdb` DB) and the
/// IMDb mirror (via `imdb_pool`) independently — they are separate databases —
/// then merges / dedupes / ranks in Rust. Absent pools simply contribute nothing.
/// DB errors on either pool are swallowed to an empty contribution so a
/// half-provisioned deployment degrades to the live API rather than 500-ing.
pub async fn local_tmdb_search(
    imdb_pool: Option<&PgPool>,
    tmdb_pool: Option<&PgPool>,
    query: &str,
    kind: &str,
    limit: i64,
    work_mem: &str,
) -> Vec<ScoredHit> {
    // The two pools are separate databases; probe them concurrently so their
    // latencies (worst case: one fuzzy-fallback cap each) overlap instead of
    // stacking. Source order (tmdb before imdb) is preserved for merge_hits.
    let tmdb_search = async {
        if let Some(pool) = tmdb_pool {
            match search_tmdb_id_index(
                pool,
                query,
                kind,
                limit,
                LOCAL_SIMILARITY_THRESHOLD,
                work_mem,
            )
            .await
            {
                Ok(hits) => return Some(hits),
                Err(e) => tracing::warn!(error = %e, kind, "local tmdb_id_index search failed"),
            }
        }
        None
    };
    let imdb_search = async {
        if let Some(pool) = imdb_pool {
            let types = imdb_title_types(kind);
            match search_imdb_titles(
                pool,
                query,
                types,
                limit,
                LOCAL_SIMILARITY_THRESHOLD,
                work_mem,
            )
            .await
            {
                // Cross-reference tconst hits to TMDB ids (via previously hydrated
                // cache payloads) before merging, so an exact IMDb-akas match (e.g.
                // "gintama" → tt0988818) surfaces under its TMDB id (57041) and
                // dedupes with any tmdb_id_index hit for the same title (SHIB-15).
                Ok(hits) => return Some(resolve_imdb_hits(tmdb_pool, hits, kind).await),
                Err(e) => tracing::warn!(error = %e, kind, "local imdb title search failed"),
            }
        }
        None
    };
    let (tmdb_hits, imdb_hits) = tokio::join!(tmdb_search, imdb_search);
    let mut sources: Vec<Vec<ScoredHit>> = Vec::new();
    sources.extend(tmdb_hits);
    sources.extend(imdb_hits);

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

    /// Lucene field syntax is stripped: prefix + quotes, quoted phrase intact,
    /// with or without a space after the colon.
    #[test]
    fn normalize_strips_field_prefix_and_quotes() {
        let n = |s: &str| normalize_query(s).expect("answerable");
        assert_eq!(n(r#"title:"dancer in the dark""#), "dancer in the dark");
        assert_eq!(n("title:inception"), "inception");
        assert_eq!(n("title: dune"), "dune");
        assert_eq!(n(r#"title="star wars""#), "star wars");
        assert_eq!(n(r#"name: "the wire""#), "the wire");
        assert_eq!(n(r#"original_title:"El laberinto del fauno""#), "El laberinto del fauno");
    }

    /// A negated whitelisted field cannot be answered by a text search — it must
    /// yield None (empty results), never matches FOR the excluded value.
    #[test]
    fn normalize_rejects_negated_fields() {
        assert_eq!(normalize_query(r#"title!="star wars""#), None);
        assert_eq!(normalize_query("name!=foo"), None);
        assert_eq!(normalize_query("title!:foo"), None);
    }

    /// Real titles containing a colon are NOT mistaken for field syntax: unknown
    /// prefixes, digit prefixes, and colon-space all pass through.
    #[test]
    fn normalize_keeps_titles_with_colons() {
        let n = |s: &str| normalize_query(s).expect("answerable");
        assert_eq!(n("Mission: Impossible"), "Mission: Impossible");
        assert_eq!(n("RE:BORN"), "RE:BORN");
        assert_eq!(n("8:46"), "8:46");
        assert_eq!(n("Frost:Nixon"), "Frost:Nixon");
        assert_eq!(n("a!=b movie"), "a!=b movie");
    }

    /// Plain queries are untouched beyond whitespace collapsing; bare quotes drop.
    #[test]
    fn normalize_plain_queries() {
        let n = |s: &str| normalize_query(s).expect("answerable");
        assert_eq!(n("dancer in the dark"), "dancer in the dark");
        assert_eq!(n(r#""dune part two""#), "dune part two");
        assert_eq!(n("  spirited   away  "), "spirited away");
        assert_eq!(n(""), "");
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

    // ── IMDb probe threshold clamp (SHIB-16) ────────────────────

    /// Bit-exact float equality for the clamp tests (avoids `float_cmp` while
    /// still asserting the exact value, not an approximation).
    fn assert_f64_eq(actual: f64, expected: f64) {
        assert!(actual.to_bits() == expected.to_bits(), "expected {expected}, got {actual}");
    }

    #[test]
    fn imdb_probe_clamps_low_thresholds_to_floor() {
        // The permissive MB default (0.2) and the local default (0.3) explode
        // the 58M-row akas trigram candidate set — both clamp up to 0.4.
        assert_f64_eq(imdb_probe_threshold(0.2), IMDB_PROBE_MIN_THRESHOLD);
        assert_f64_eq(imdb_probe_threshold(LOCAL_SIMILARITY_THRESHOLD), IMDB_PROBE_MIN_THRESHOLD);
        assert_f64_eq(imdb_probe_threshold(0.0), IMDB_PROBE_MIN_THRESHOLD);
    }

    #[test]
    fn imdb_probe_keeps_thresholds_at_or_above_floor() {
        // A stricter configured cutoff is respected, never lowered.
        assert_f64_eq(imdb_probe_threshold(0.4), 0.4);
        assert_f64_eq(imdb_probe_threshold(0.7), 0.7);
        assert_f64_eq(imdb_probe_threshold(1.0), 1.0);
    }

    // ── work_mem sanitisation (SET cannot bind a parameter) ─────

    #[test]
    fn work_mem_valid_values_pass_through() {
        assert_eq!(sanitize_work_mem("256MB"), "256MB");
        assert_eq!(sanitize_work_mem("1GB"), "1GB");
        assert_eq!(sanitize_work_mem("64000"), "64000"); // bare kB units
    }

    #[test]
    fn work_mem_strips_injection_attempts() {
        // A quote-breaking payload loses every non-alphanumeric character, so
        // nothing can escape the `SET work_mem = '…'` literal.
        assert_eq!(sanitize_work_mem("256MB'; DROP TABLE x; --"), "256MBDROPTABLEx");
        assert!(!sanitize_work_mem("1'||pg_sleep(10)||'").contains('\''));
    }

    #[test]
    fn work_mem_garbage_falls_back_to_default() {
        // Empty, whitespace, or values not starting with a digit (nothing
        // Postgres would accept for work_mem) fall back to the safe default.
        assert_eq!(sanitize_work_mem(""), "256MB");
        assert_eq!(sanitize_work_mem("   "), "256MB");
        assert_eq!(sanitize_work_mem("'; DROP TABLE x; --"), "256MB");
        assert_eq!(sanitize_work_mem("MB256"), "256MB");
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
        assert!(is_thin_result(&hits)); // no hit reaches the 60 floor
    }

    #[test]
    fn enough_strong_hits_is_not_thin() {
        let hits = vec![hit("a", 90, None), hit("b", 70, None), hit("c", 65, None)];
        assert!(!is_thin_result(&hits)); // 3 strong hits → serve local, no live call
    }

    #[test]
    fn weak_tail_behind_one_strong_hit_is_thin() {
        // SHIB-15 regression: "gintama" locally yields one strong hit plus weak
        // trigram junk ("Gina", "Ginirama"). The old gate (count ≥ 3 AND top ≥ 60)
        // called this confident and suppressed the live merge; weak hits must not
        // count toward the hit minimum.
        let hits = vec![hit("57041", 100, None), hit("gina", 50, None), hit("ginirama", 45, None)];
        assert!(is_thin_result(&hits));
    }

    #[test]
    fn strong_hits_below_min_count_is_thin() {
        // Even two perfect hits are thin — the live API may know a third.
        let hits = vec![hit("a", 100, None), hit("b", 100, None)];
        assert!(is_thin_result(&hits));
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

    // ── tconst → TMDB id cross-referencing (SHIB-15) ────────────

    #[test]
    fn tmdb_id_parses_numeric_ids_only() {
        assert_eq!(hit("57041", 100, None).tmdb_id(), Some(57041));
        assert_eq!(hit("tt0988818", 100, None).tmdb_id(), None);
        assert_eq!(hit("", 0, None).tmdb_id(), None);
    }

    #[test]
    fn tconst_resolution_rewrites_id_and_enriches() {
        use std::collections::HashMap;
        // The akas probe matched "Gintama" → tt0988818 (score 100). The cache
        // knows tt0988818 ↔ tmdb 57041 and the id index carries the native name
        // + popularity. The resolved hit keeps its similarity score.
        let hits = vec![ScoredHit {
            id: "tt0988818".into(),
            name: "Gintama".into(),
            score: 100,
            popularity: None,
            adult: None,
        }];
        let resolved: HashMap<String, i64> = [("tt0988818".to_string(), 57041)].into();
        let meta: HashMap<i64, IndexMeta> = [(
            57041,
            IndexMeta {
                name: Some("銀魂".to_string()),
                popularity: Some(42.0),
                adult: Some(false),
            },
        )]
        .into();
        let out = apply_tconst_resolution(hits, &resolved, &meta);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "57041"); // renderable as a native TMDB result now
        assert_eq!(out[0].tmdb_id(), Some(57041));
        assert_eq!(out[0].name, "銀魂"); // index display name preferred
        assert_eq!(out[0].score, 100); // the akas match remains the evidence
        assert_eq!(out[0].popularity, Some(42.0));
        assert_eq!(out[0].adult, Some(false));
    }

    #[test]
    fn tconst_resolution_without_meta_keeps_imdb_name() {
        use std::collections::HashMap;
        let hits = vec![ScoredHit {
            id: "tt0988818".into(),
            name: "Gintama".into(),
            score: 100,
            popularity: None,
            adult: None,
        }];
        let resolved: HashMap<String, i64> = [("tt0988818".to_string(), 57041)].into();
        let out = apply_tconst_resolution(hits, &resolved, &HashMap::new());
        assert_eq!(out[0].id, "57041");
        assert_eq!(out[0].name, "Gintama"); // IMDb primary title survives
        assert_eq!(out[0].popularity, None);
    }

    #[test]
    fn unresolved_tconst_passes_through_unchanged() {
        use std::collections::HashMap;
        let original = hit("tt0000001", 80, None);
        let out = apply_tconst_resolution(vec![original.clone()], &HashMap::new(), &HashMap::new());
        assert_eq!(out, vec![original]); // never hydrated → stays a tconst hit
    }

    #[test]
    fn resolved_tconst_dedupes_with_index_hit_via_merge() {
        // After resolution the akas hit shares the tmdb_id_index hit's id, so the
        // standard merge collapses them, keeping the better (akas) score and the
        // index popularity.
        let resolved_akas = hit("57041", 100, None);
        let index_hit = hit("57041", 55, Some(42.0));
        let merged = merge_hits(vec![vec![index_hit], vec![resolved_akas]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].score, 100);
        assert_eq!(merged[0].popularity, Some(42.0));
    }
}
