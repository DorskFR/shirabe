use axum::Json;
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde_json::json;

pub async fn no_such_route(method: Method, uri: Uri) -> Response {
    tracing::warn!(%method, path = %uri.path(), "no such route");
    let message = format!("shirabe: no such route: {method} {}", uri.path());
    (StatusCode::NOT_FOUND, Json(json!({ "error": message }))).into_response()
}

pub async fn method_not_allowed(method: Method, uri: Uri) -> Response {
    tracing::warn!(%method, path = %uri.path(), "method not allowed");
    let message = format!("shirabe: method not allowed: {method} {}", uri.path());
    (StatusCode::METHOD_NOT_ALLOWED, Json(json!({ "error": message }))).into_response()
}

/// Errors surfaced by request handlers.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("not found")]
    NotFound,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Database(ref e) => {
                tracing::error!(error = %e, "database error");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal database error".to_string())
            }
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            Self::NotFound => (StatusCode::NOT_FOUND, "not found".to_string()),
        };
        // MusicBrainz returns an `error` field on failures; mirror that shape.
        (status, Json(json!({ "error": message }))).into_response()
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
