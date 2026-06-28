//! Native-shape provider facades.
//!
//! Shirabe exposes each upstream provider's *native* API surface under a
//! version-native prefix on one host, so Kusaritoi consumes Shirabe by pointing
//! that provider's `base_url` at Shirabe with no client code change:
//!
//! - `/ws/2/*` — MusicBrainz ws/2 subset (implemented; see [`crate::handlers`]).
//! - `/v4/*`   — TheTVDB v4 facade (skeleton; see [`tvdb`]).
//! - `/3/*`    — TMDB v3 facade (skeleton; see [`tmdb`]).
//!
//! The TVDB/TMDB routers here are routing skeletons only: they mount the exact
//! endpoint paths Kusaritoi calls and return `501 Not Implemented` until the
//! provider logic lands (SHIB-4 / SHIB-5). The full contract is documented in
//! `docs/shirabe-api-contract.md`.

pub mod tmdb;
pub mod tvdb;

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// Placeholder response for a facade endpoint whose provider logic is not yet
/// implemented. Returns HTTP 501 with a small JSON body so callers (and the
/// access log) get a clear, machine-readable signal rather than an empty 404.
pub fn not_implemented(endpoint: &str) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({ "error": format!("{endpoint} not implemented yet") })),
    )
        .into_response()
}
