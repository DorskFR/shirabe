//! axum handlers implementing the MusicBrainz ws/2 subset the consumer uses.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::error::{ApiError, ApiResult};
use crate::models::{
    ArtistSearchResponse, RecordingSearchResponse, ReleaseGroupBrowseResponse,
    ReleaseSearchResponse,
};
use crate::{AppState, query, repo};

/// Common query string for the search endpoints.
#[derive(Debug, Deserialize)]
pub struct SearchParams {
    pub query: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    // `fmt` is accepted for compatibility but not acted on structurally — shapes
    // are fixed per endpoint. `inc` is honoured by the artist lookup
    // (url-rels, genres, tags, annotation).
    #[allow(dead_code)]
    pub fmt: Option<String>,
    pub inc: Option<String>,
}

/// Query string for the release-group browse endpoint (`?artist=<mbid>`).
#[derive(Debug, Deserialize)]
pub struct BrowseParams {
    pub artist: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    #[allow(dead_code)]
    pub fmt: Option<String>,
    #[allow(dead_code)]
    pub inc: Option<String>,
}

/// `GET /ws/2/artist`
pub async fn search_artist(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> ApiResult<Json<ArtistSearchResponse>> {
    let raw = params.query.unwrap_or_default();
    let parsed = query::parse(&raw);
    // Artist endpoint: the name is the bare query (or an explicit artist:field).
    let name = parsed
        .bare
        .or(parsed.artist)
        .ok_or_else(|| ApiError::BadRequest("missing query".into()))?;
    let limit = state.config.resolve_limit(params.limit);
    let offset = resolve_offset(params.offset);
    let (count, artists) = repo::search_artists(
        state.pool(),
        &name,
        limit,
        offset,
        state.config.similarity_threshold,
        &state.config.search_work_mem,
    )
    .await?;
    Ok(Json(ArtistSearchResponse { count, offset, artists }))
}

/// `GET /ws/2/release`
pub async fn search_release(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> ApiResult<Json<ReleaseSearchResponse>> {
    let raw = params.query.unwrap_or_default();
    let parsed = query::parse(&raw);
    let limit = state.config.resolve_limit(params.limit);
    let offset = resolve_offset(params.offset);
    if let Some(arid) = parsed.arid.as_deref() {
        let gid = parse_mbid(arid)?;
        let (count, releases) = repo::browse_releases_by_artist(
            state.pool(),
            gid,
            parsed.primary_type.as_deref(),
            parsed.status.as_deref(),
            limit,
            offset,
        )
        .await?;
        return Ok(Json(ReleaseSearchResponse { count, offset, releases }));
    }
    let title = parsed
        .release
        .or_else(|| parsed.bare.clone())
        .ok_or_else(|| ApiError::BadRequest("missing release title".into()))?;
    let (count, releases) = repo::search_releases(
        state.pool(),
        &title,
        repo::ReleaseFilters {
            artist: parsed.artist.as_deref(),
            year: parsed.date_year.as_deref(),
            primary_type: parsed.primary_type.as_deref(),
            status: parsed.status.as_deref(),
        },
        limit,
        offset,
        state.config.similarity_threshold,
        &state.config.search_work_mem,
    )
    .await?;
    Ok(Json(ReleaseSearchResponse { count, offset, releases }))
}

/// `GET /ws/2/recording`
pub async fn search_recording(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> ApiResult<Json<RecordingSearchResponse>> {
    let raw = params.query.unwrap_or_default();
    let parsed = query::parse(&raw);
    let title = parsed
        .recording
        .or_else(|| parsed.bare.clone())
        .ok_or_else(|| ApiError::BadRequest("missing recording title".into()))?;
    let limit = state.config.resolve_limit(params.limit);
    let offset = resolve_offset(params.offset);
    let (count, recordings) = repo::search_recordings(
        state.pool(),
        &title,
        parsed.artist.as_deref(),
        limit,
        offset,
        state.config.similarity_threshold,
        &state.config.search_work_mem,
    )
    .await?;
    Ok(Json(RecordingSearchResponse { count, offset, recordings }))
}

fn resolve_offset(offset: Option<i64>) -> i64 {
    offset.unwrap_or(0).max(0)
}

/// Parse a path MBID, mapping malformed UUIDs to 400.
fn parse_mbid(raw: &str) -> ApiResult<Uuid> {
    Uuid::parse_str(raw).map_err(|_| ApiError::BadRequest(format!("invalid mbid: {raw}")))
}

/// True when an `inc=` param (space/`+`-separated list) contains the given token.
fn inc_has(inc: Option<&str>, token: &str) -> bool {
    inc.is_some_and(|s| s.split([' ', '+']).any(|t| t == token))
}

fn artist_includes(inc: Option<&str>) -> repo::ArtistIncludes {
    repo::ArtistIncludes {
        url_rels: inc_has(inc, "url-rels"),
        genres: inc_has(inc, "genres"),
        tags: inc_has(inc, "tags"),
        annotation: inc_has(inc, "annotation"),
    }
}

/// `GET /ws/2/artist/{mbid}`
pub async fn lookup_artist(
    State(state): State<Arc<AppState>>,
    Path(mbid): Path<String>,
    Query(params): Query<SearchParams>,
) -> ApiResult<Json<Value>> {
    let gid = parse_mbid(&mbid)?;
    let inc = artist_includes(params.inc.as_deref());
    let artist = repo::lookup_artist(state.pool(), gid, inc).await?.ok_or(ApiError::NotFound)?;
    Ok(Json(serde_json::to_value(artist).expect("artist serializes")))
}

/// `GET /ws/2/release/{mbid}`
pub async fn lookup_release(
    State(state): State<Arc<AppState>>,
    Path(mbid): Path<String>,
) -> ApiResult<Json<Value>> {
    let gid = parse_mbid(&mbid)?;
    let release = repo::lookup_release(state.pool(), gid).await?.ok_or(ApiError::NotFound)?;
    Ok(Json(serde_json::to_value(release).expect("release serializes")))
}

/// `GET /ws/2/recording/{mbid}`
pub async fn lookup_recording(
    State(state): State<Arc<AppState>>,
    Path(mbid): Path<String>,
) -> ApiResult<Json<Value>> {
    let gid = parse_mbid(&mbid)?;
    let recording = repo::lookup_recording(state.pool(), gid).await?.ok_or(ApiError::NotFound)?;
    Ok(Json(serde_json::to_value(recording).expect("recording serializes")))
}

/// `GET /ws/2/release-group?artist={mbid}`
pub async fn browse_release_group(
    State(state): State<Arc<AppState>>,
    Query(params): Query<BrowseParams>,
) -> ApiResult<Json<ReleaseGroupBrowseResponse>> {
    let artist =
        params.artist.as_deref().ok_or_else(|| ApiError::BadRequest("missing artist".into()))?;
    let gid = parse_mbid(artist)?;
    let limit = state.config.resolve_limit(params.limit);
    let offset = resolve_offset(params.offset);
    let (total, release_groups) =
        repo::browse_release_groups(state.pool(), gid, limit, offset).await?;
    Ok(Json(ReleaseGroupBrowseResponse {
        release_group_count: total,
        release_group_offset: offset,
        release_groups,
    }))
}

/// `GET /ws/2/release-group/{mbid}`
pub async fn lookup_release_group(
    State(state): State<Arc<AppState>>,
    Path(mbid): Path<String>,
) -> ApiResult<Json<Value>> {
    let gid = parse_mbid(&mbid)?;
    let group = repo::lookup_release_group(state.pool(), gid).await?.ok_or(ApiError::NotFound)?;
    Ok(Json(serde_json::to_value(group).expect("release group serializes")))
}

/// `GET /health` and `GET /ws/2` ping.
pub async fn health(State(state): State<Arc<AppState>>) -> ApiResult<Json<Value>> {
    repo::ping(state.pool()).await?;
    Ok(Json(json!({ "status": "ok" })))
}

/// `GET /health/sources` — per-source observability.
///
/// For every registered source, merges the live `Source::health()` probe with the
/// persisted `shirabe.source` row (last_refresh_at/status/detail written by
/// `shirabe sync <source>` CronJob runs), so a stale or errored source is obvious
/// at a glance via the `healthy` rollup. Degrades gracefully when the writable
/// shirabe pool is absent (reports only what live `health()` can determine).
pub async fn health_sources(
    State(state): State<Arc<AppState>>,
) -> Json<crate::sources::SourcesHealthReport> {
    Json(state.registry.health_report().await)
}

#[cfg(test)]
mod tests {
    use super::{artist_includes, inc_has, resolve_offset};

    #[test]
    fn resolve_offset_defaults_and_clamps() {
        assert_eq!(resolve_offset(None), 0);
        assert_eq!(resolve_offset(Some(0)), 0);
        assert_eq!(resolve_offset(Some(50)), 50);
        assert_eq!(resolve_offset(Some(-1)), 0);
        assert_eq!(resolve_offset(Some(i64::MIN)), 0);
    }

    #[test]
    fn inc_has_matches_tokens() {
        assert!(inc_has(Some("url-rels"), "url-rels"));
        assert!(inc_has(Some("aliases+url-rels"), "url-rels"));
        assert!(inc_has(Some("aliases url-rels"), "url-rels"));
        assert!(!inc_has(Some("url-rels-extra"), "url-rels"));
        assert!(!inc_has(Some("aliases"), "url-rels"));
        assert!(!inc_has(None, "url-rels"));
    }

    #[test]
    fn artist_includes_requires_each_token() {
        let inc = artist_includes(Some("genres+tags+url-rels+annotation"));
        assert!(inc.url_rels && inc.genres && inc.tags && inc.annotation);

        let inc = artist_includes(Some("genres"));
        assert!(inc.genres);
        assert!(!inc.url_rels && !inc.tags && !inc.annotation);

        let inc = artist_includes(Some("aliases"));
        assert!(!inc.url_rels && !inc.genres && !inc.tags && !inc.annotation);

        let inc = artist_includes(None);
        assert!(!inc.url_rels && !inc.genres && !inc.tags && !inc.annotation);
    }
}
