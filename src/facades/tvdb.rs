//! TheTVDB v4 facade — routing skeleton (SHIB-2).
//!
//! Mounts the exact `/v4/*` endpoints Kusaritoi's TVDB provider calls (per the
//! API contract, mirroring the upstream v4 shapes). No provider logic yet: every
//! handler returns 501. Kusaritoi reaches this by setting its `tvdb.base_url` to
//! `…/v4`.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::response::Response;
use axum::routing::{get, post};
use serde_json::Value;

use super::not_implemented;
use crate::AppState;

/// Build the `/v4` route group.
pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v4/login", post(login))
        .route("/v4/search", get(search))
        .route("/v4/series/{id}", get(series))
        .route("/v4/series/{id}/extended", get(series_extended))
        .route("/v4/series/{id}/episodes/{season_type}", get(series_episodes))
        .route("/v4/movies/{id}", get(movie))
}

/// `POST /v4/login` — `{apikey,pin}` → `{data:{token}}` (Shirabe-minted token).
async fn login(State(_state): State<Arc<AppState>>) -> Response {
    not_implemented("POST /v4/login")
}

/// `GET /v4/search?type=series&query=` → `{data:[{tvdb_id,name,year,aliases,translations}]}`.
async fn search(State(_state): State<Arc<AppState>>, Query(_params): Query<Value>) -> Response {
    not_implemented("GET /v4/search")
}

/// `GET /v4/series/{id}`.
async fn series(State(_state): State<Arc<AppState>>, Path(_id): Path<String>) -> Response {
    not_implemented("GET /v4/series/{id}")
}

/// `GET /v4/series/{id}/extended` → `{data:{name,firstAired,seasons:[{type:{type}}]}}`.
async fn series_extended(State(_state): State<Arc<AppState>>, Path(_id): Path<String>) -> Response {
    not_implemented("GET /v4/series/{id}/extended")
}

/// `GET /v4/series/{id}/episodes/{season_type}?season=&page=` → paginated episodes.
async fn series_episodes(
    State(_state): State<Arc<AppState>>,
    Path((_id, _season_type)): Path<(String, String)>,
    Query(_params): Query<Value>,
) -> Response {
    not_implemented("GET /v4/series/{id}/episodes/{season-type}")
}

/// `GET /v4/movies/{id}`.
async fn movie(State(_state): State<Arc<AppState>>, Path(_id): Path<String>) -> Response {
    not_implemented("GET /v4/movies/{id}")
}
