//! TMDB v3 facade — routing skeleton (SHIB-2).
//!
//! Mounts the exact `/3/*` endpoints Kusaritoi's TMDB provider calls, mirroring
//! the upstream v3 shapes. The `api_key` query param is accepted and ignored
//! (Shirabe holds the real key server-side). No provider logic yet: every
//! handler returns 501. Kusaritoi reaches this by setting its `tmdb.base_url` to
//! `…/3`.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::response::Response;
use axum::routing::get;
use serde_json::Value;

use super::not_implemented;
use crate::AppState;

/// Build the `/3` route group.
pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/3/search/tv", get(search_tv))
        .route("/3/search/movie", get(search_movie))
        .route("/3/tv/{id}", get(tv))
        .route("/3/tv/{id}/season/{n}", get(tv_season))
        .route("/3/movie/{id}", get(movie))
}

/// `GET /3/search/tv?query=` → `{results:[{id,name,first_air_date}]}`.
async fn search_tv(State(_state): State<Arc<AppState>>, Query(_params): Query<Value>) -> Response {
    not_implemented("GET /3/search/tv")
}

/// `GET /3/search/movie?query=` → `{results:[{id,title,release_date,overview}]}`.
async fn search_movie(
    State(_state): State<Arc<AppState>>,
    Query(_params): Query<Value>,
) -> Response {
    not_implemented("GET /3/search/movie")
}

/// `GET /3/tv/{id}` → `{name,first_air_date,seasons:[{season_number,name}]}`.
async fn tv(State(_state): State<Arc<AppState>>, Path(_id): Path<String>) -> Response {
    not_implemented("GET /3/tv/{id}")
}

/// `GET /3/tv/{id}/season/{n}` → `{episodes:[{episode_number,name,runtime}]}`.
async fn tv_season(
    State(_state): State<Arc<AppState>>,
    Path((_id, _n)): Path<(String, String)>,
) -> Response {
    not_implemented("GET /3/tv/{id}/season/{n}")
}

/// `GET /3/movie/{id}?append_to_response=external_ids` →
/// `{title,release_date,runtime,imdb_id,external_ids:{imdb_id},overview}`.
async fn movie(
    State(_state): State<Arc<AppState>>,
    Path(_id): Path<String>,
    Query(_params): Query<Value>,
) -> Response {
    not_implemented("GET /3/movie/{id}")
}
