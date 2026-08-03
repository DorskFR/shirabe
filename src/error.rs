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

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use serde_json::Value;

    use super::*;

    async fn parts(resp: Response) -> (StatusCode, Value) {
        let status = resp.status();
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        assert!(content_type.starts_with("application/json"), "got {content_type}");
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn database_error_is_opaque_500() {
        let (status, body) =
            parts(ApiError::Database(sqlx::Error::RowNotFound).into_response()).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"], "internal database error");
        assert!(!body["error"].as_str().unwrap().contains("no rows"), "must not leak DB detail");
    }

    #[tokio::test]
    async fn bad_request_carries_message() {
        let (status, body) =
            parts(ApiError::BadRequest("query is required".into()).into_response()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "query is required");
    }

    #[tokio::test]
    async fn not_found_shape() {
        let (status, body) = parts(ApiError::NotFound.into_response()).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "not found");
    }

    #[test]
    fn sqlx_error_converts_to_database_variant() {
        let err = ApiError::from(sqlx::Error::RowNotFound);
        assert!(matches!(err, ApiError::Database(_)));
    }

    #[tokio::test]
    async fn no_such_route_names_method_and_path() {
        let (status, body) =
            parts(no_such_route(Method::GET, Uri::from_static("/nope")).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "shirabe: no such route: GET /nope");
    }

    #[tokio::test]
    async fn method_not_allowed_names_method_and_path() {
        let (status, body) =
            parts(method_not_allowed(Method::POST, Uri::from_static("/ws/2/artist")).await).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(body["error"], "shirabe: method not allowed: POST /ws/2/artist");
    }
}
