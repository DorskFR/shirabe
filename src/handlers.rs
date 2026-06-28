//! axum handlers implementing the MusicBrainz ws/2 subset the consumer uses.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::error::{ApiError, ApiResult};
use crate::models::{ArtistSearchResponse, RecordingSearchResponse, ReleaseSearchResponse};
use crate::{AppState, query, repo};

/// Common query string for the search endpoints.
#[derive(Debug, Deserialize)]
pub struct SearchParams {
    pub query: Option<String>,
    pub limit: Option<i64>,
    // `fmt` is accepted for compatibility but not acted on structurally — shapes
    // are fixed per endpoint. `inc` is honoured by the artist lookup (url-rels).
    #[allow(dead_code)]
    pub fmt: Option<String>,
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
    let artists =
        repo::search_artists(state.pool(), &name, limit, state.config.similarity_threshold).await?;
    Ok(Json(ArtistSearchResponse { artists }))
}

/// `GET /ws/2/release`
pub async fn search_release(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> ApiResult<Json<ReleaseSearchResponse>> {
    let raw = params.query.unwrap_or_default();
    let parsed = query::parse(&raw);
    let title = parsed
        .release
        .or_else(|| parsed.bare.clone())
        .ok_or_else(|| ApiError::BadRequest("missing release title".into()))?;
    let limit = state.config.resolve_limit(params.limit);
    let releases = repo::search_releases(
        state.pool(),
        &title,
        parsed.artist.as_deref(),
        parsed.date_year.as_deref(),
        limit,
        state.config.similarity_threshold,
    )
    .await?;
    Ok(Json(ReleaseSearchResponse { releases }))
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
    let recordings = repo::search_recordings(
        state.pool(),
        &title,
        parsed.artist.as_deref(),
        limit,
        state.config.similarity_threshold,
    )
    .await?;
    Ok(Json(RecordingSearchResponse { recordings }))
}

/// Parse a path MBID, mapping malformed UUIDs to 400.
fn parse_mbid(raw: &str) -> ApiResult<Uuid> {
    Uuid::parse_str(raw).map_err(|_| ApiError::BadRequest(format!("invalid mbid: {raw}")))
}

/// True when an `inc=` param (space/`+`-separated list) contains the given token.
fn inc_has(inc: Option<&str>, token: &str) -> bool {
    inc.is_some_and(|s| s.split([' ', '+']).any(|t| t == token))
}

/// `GET /ws/2/artist/{mbid}`
pub async fn lookup_artist(
    State(state): State<Arc<AppState>>,
    Path(mbid): Path<String>,
    Query(params): Query<SearchParams>,
) -> ApiResult<Json<Value>> {
    let gid = parse_mbid(&mbid)?;
    let with_url_rels = inc_has(params.inc.as_deref(), "url-rels");
    let artist =
        repo::lookup_artist(state.pool(), gid, with_url_rels).await?.ok_or(ApiError::NotFound)?;
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
    use super::inc_has;

    #[test]
    fn inc_has_matches_tokens() {
        assert!(inc_has(Some("url-rels"), "url-rels"));
        assert!(inc_has(Some("aliases+url-rels"), "url-rels"));
        assert!(inc_has(Some("aliases url-rels"), "url-rels"));
        assert!(!inc_has(Some("url-rels-extra"), "url-rels"));
        assert!(!inc_has(Some("aliases"), "url-rels"));
        assert!(!inc_has(None, "url-rels"));
    }
}
