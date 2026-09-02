//! shirabe — a small, fast MusicBrainz ws/2 subset served directly from a
//! MusicBrainz Postgres mirror via pg_trgm.

pub mod config;
pub mod date;
pub mod db;
pub mod debug_ui;
pub mod error;
pub mod facades;
pub mod handlers;
pub mod images;
pub mod migrate;
pub mod models;
pub mod queries;
pub mod query;
pub mod repo;
pub mod search;
pub mod sources;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use sqlx::PgPool;
use tower_http::LatencyUnit;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

use crate::config::Config;
use crate::db::Pools;
use crate::facades::coverart::CoverArtState;
use crate::sources::Registry;
use crate::sources::tvdb::TokenStore;

/// Shared application state handed to every handler.
pub struct AppState {
    /// All DB pools. `pools.musicbrainz` is the read-only mirror that the ws/2
    /// handlers query; the optional shirabe/imdb/tmdb/tvdb pools back
    /// coordination/ingest and the provider caches.
    pub pools: Pools,
    pub config: Config,
    pub registry: Registry,
    /// Shared in-memory TheTVDB bearer token, minted from the server-side key and
    /// reused by the `/v4` facade and the `tvdb` source.
    pub tvdb_tokens: TokenStore,
    /// Native Cover Art Archive proxy state (HTTP client + on-disk byte cache +
    /// single-flight locks) backing the `/release`, `/release-group`, and `/_ia`
    /// routes.
    pub coverart: CoverArtState,
}

impl AppState {
    /// The read-only MusicBrainz mirror pool the ws/2 handlers query.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pools.musicbrainz
    }
}

/// Run a single source's `refresh()` and exit non-zero on failure or unknown id.
pub async fn run_sync(registry: &Registry, source: &str) -> anyhow::Result<()> {
    tracing::info!(source, "running sync");
    match registry.run_refresh(source).await {
        Some(report) if report.ok => {
            tracing::info!(source, summary = %report.summary, "sync ok");
            Ok(())
        }
        Some(report) => {
            anyhow::bail!("sync of `{source}` failed: {}", report.summary)
        }
        None => {
            anyhow::bail!("unknown source `{source}`; known: {}", registry.ids().join(", "))
        }
    }
}

/// Start the axum HTTP server (unchanged default behaviour).
pub async fn serve(
    config: Config,
    pools: Pools,
    registry: Registry,
    tvdb_tokens: TokenStore,
) -> anyhow::Result<()> {
    let bind = config.bind.clone();
    tracing::info!(bind = %bind, "starting shirabe");
    let coverart = CoverArtState::new(&config);
    let state = Arc::new(AppState { pools, config, registry, tvdb_tokens, coverart });

    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(addr = %bind, "listening");
    axum::serve(listener, app).await?;
    Ok(())
}

/// MusicBrainz ws/2 routes, mounted both natively and under the `/musicbrainz`
/// alias. Excludes `/health` and `/health/sources`, which stay at the root only.
fn ws2_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/ws/2", get(handlers::health))
        .route("/ws/2/artist", get(handlers::search_artist))
        .route("/ws/2/artist/{mbid}", get(handlers::lookup_artist))
        .route("/ws/2/release", get(handlers::search_release))
        .route("/ws/2/release/{mbid}", get(handlers::lookup_release))
        .route("/ws/2/recording", get(handlers::search_recording))
        .route("/ws/2/recording/{mbid}", get(handlers::lookup_recording))
        .route("/ws/2/release-group", get(handlers::browse_release_group))
        .route("/ws/2/release-group/{mbid}", get(handlers::lookup_release_group))
}

/// `/music`: stripped `/artist|/release|/recording` shortcuts plus the full
/// `/ws/2` tree, so a ws/2 client can point its base at `/music`.
fn music_alias_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/artist", get(handlers::search_artist))
        .route("/artist/{mbid}", get(handlers::lookup_artist))
        .route("/release", get(handlers::search_release))
        .route("/release/{mbid}", get(handlers::lookup_release))
        .route("/recording", get(handlers::search_recording))
        .route("/recording/{mbid}", get(handlers::lookup_recording))
        .route("/release-group", get(handlers::browse_release_group))
        .route("/release-group/{mbid}", get(handlers::lookup_release_group))
        .merge(ws2_router())
}

/// Build the full application router over the shared state.
pub fn build_router(state: Arc<AppState>) -> Router {
    let app = Router::new()
        .route("/health", get(handlers::health))
        .route("/health/sources", get(handlers::health_sources))
        .merge(ws2_router())
        .nest("/musicbrainz", ws2_router())
        .nest("/music", music_alias_router())
        .merge(facades::tvdb::router())
        .nest("/tvdb", facades::tvdb::router())
        .merge(facades::tmdb::router())
        .nest("/tmdb", facades::tmdb::router())
        .merge(facades::fanart::router())
        .nest("/fanart", facades::fanart::router())
        .merge(facades::coverart::router())
        .nest("/coverart", facades::coverart::router());

    // Opt-in query explorer: needs SHIRABE_DEBUG_UI=1 and a non-empty
    // SHIRABE_DEBUG_UI_TOKEN; every `/debug/*` request must present that bearer.
    let app = match (state.config.debug_ui, state.config.debug_ui_token.as_deref()) {
        (true, Some(token)) if !token.is_empty() => app.merge(debug_ui::router(token.to_owned())),
        (true, _) => {
            tracing::warn!(
                "SHIRABE_DEBUG_UI set without SHIRABE_DEBUG_UI_TOKEN; debug UI not mounted"
            );
            app
        }
        (false, _) => app,
    };

    app.fallback(error::no_such_route)
        .method_not_allowed_fallback(error::method_not_allowed)
        // One INFO access-log line per request (method, uri, status, latency ms),
        // visible at the default `RUST_LOG=info`.
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_request(())
                .on_response(
                    DefaultOnResponse::new().level(Level::INFO).latency_unit(LatencyUnit::Millis),
                ),
        )
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use clap::Parser;
    use tower::ServiceExt;

    use super::{AppState, build_router};
    use crate::config::Cli;
    use crate::db::{Pools, connect_lazy};
    use crate::facades::coverart::CoverArtState;
    use crate::sources::Registry;
    use crate::sources::tvdb::TokenStore;

    fn test_state() -> std::sync::Arc<AppState> {
        // Lazy pool: never connects, so the router builds without a live DB.
        let cli = Cli::try_parse_from(["shirabe", "--database-url", "postgres://x/x"]).unwrap();
        let config = cli.config;
        let pools = Pools {
            musicbrainz: connect_lazy("postgres://x/x", 1).unwrap(),
            shirabe: None,
            imdb: None,
            tmdb: None,
            tvdb: None,
            fanart: None,
        };
        let tvdb_tokens = TokenStore::new();
        let registry = Registry::with_defaults(pools.clone(), config.clone(), tvdb_tokens.clone());
        let coverart = CoverArtState::new(&config);
        std::sync::Arc::new(AppState { pools, config, registry, tvdb_tokens, coverart })
    }

    async fn routes(path: &str) -> StatusCode {
        let app = build_router(test_state());
        let req = Request::builder().uri(path).body(Body::empty()).unwrap();
        app.oneshot(req).await.unwrap().status()
    }

    #[derive(Clone, Default)]
    struct Capture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = Self;
        fn make_writer(&'a self) -> Self {
            self.clone()
        }
    }

    #[tokio::test]
    async fn access_log_is_one_info_line_with_status_and_latency() {
        use tracing_subscriber::prelude::*;
        let capture = Capture::default();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::new("info"))
            .with(tracing_subscriber::fmt::layer().with_ansi(false).with_writer(capture.clone()));
        let _guard = subscriber.set_default();

        let app = build_router(test_state());
        let req = Request::builder().uri("/no-such-route").body(Body::empty()).unwrap();
        let status = app.oneshot(req).await.unwrap().status();

        let out = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<&str> = out.lines().filter(|l| l.contains("tower_http")).collect();
        assert_eq!(lines.len(), 1, "expected one access-log line, got:\n{out}");
        let line = lines[0];
        assert!(line.contains(" INFO "), "{line}");
        assert!(line.contains("method=GET"), "{line}");
        assert!(line.contains("uri=/no-such-route"), "{line}");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(line.contains("status=404"), "{line}");
        assert!(line.contains(" ms"), "{line}");
    }

    /// Every provider alias must reach a handler (non-404), proving the route is
    /// wired. Handlers may 5xx without a DB; only 404 would mean an unwired alias.
    #[tokio::test]
    async fn alias_routes_are_wired() {
        for path in [
            "/musicbrainz/ws/2/artist",
            "/tmdb/3/configuration",
            "/tmdb/3/movie/1",
            "/tvdb/v4/series/1",
            "/fanart/v3/movies/1",
            "/coverart/release/1",
        ] {
            assert_ne!(routes(path).await, StatusCode::NOT_FOUND, "alias not wired: {path}");
        }
    }

    #[tokio::test]
    async fn stripped_alias_roots_are_wired() {
        for path in [
            "/music/artist",
            "/music/artist/1",
            "/music/release",
            "/music/release/1",
            "/music/recording",
            "/music/recording/1",
            "/music/ws/2/artist",
            "/music/ws/2/artist/1",
            "/music/ws/2/release",
            "/music/ws/2/recording",
            "/music/release-group",
            "/music/release-group/1",
            "/music/ws/2/release-group",
        ] {
            assert_ne!(
                routes(path).await,
                StatusCode::NOT_FOUND,
                "stripped alias not wired: {path}"
            );
        }
    }

    #[tokio::test]
    async fn retired_media_type_aliases_are_gone() {
        for path in ["/tv/series/1", "/movie/configuration", "/movie/movie/1", "/movies/movie/1"] {
            assert_eq!(routes(path).await, StatusCode::NOT_FOUND, "alias should be gone: {path}");
        }
    }

    #[tokio::test]
    async fn release_group_routes_are_wired() {
        for path in [
            "/ws/2/release-group",
            "/ws/2/release-group/1",
            "/musicbrainz/ws/2/release-group",
            "/musicbrainz/ws/2/release-group/1",
        ] {
            assert_ne!(routes(path).await, StatusCode::NOT_FOUND, "not wired: {path}");
        }
    }

    #[tokio::test]
    async fn release_group_browse_requires_artist() {
        assert_eq!(routes("/ws/2/release-group").await, StatusCode::BAD_REQUEST);
        assert_eq!(routes("/ws/2/release-group?artist=not-a-uuid").await, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn native_prefixes_still_wired() {
        for path in ["/ws/2/artist", "/3/movie/1", "/v4/series/1", "/v3/movies/1", "/release/1"] {
            assert_ne!(routes(path).await, StatusCode::NOT_FOUND, "native not wired: {path}");
        }
    }

    async fn error_body(method: &str, path: &str) -> (StatusCode, String) {
        let app = build_router(test_state());
        let req = Request::builder().method(method).uri(path).body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn unknown_path_gets_json_404() {
        let (status, body) = error_body("GET", "/ws/2/nonexistent").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["error"], "shirabe: no such route: GET /ws/2/nonexistent");
    }

    #[tokio::test]
    async fn wrong_method_gets_json_405() {
        let (status, body) = error_body("POST", "/health").await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["error"], "shirabe: method not allowed: POST /health");
    }

    #[tokio::test]
    async fn nested_mount_misses_get_json_404() {
        for path in [
            "/tvdb/v4/nonexistent",
            "/tmdb/3/nonexistent",
            "/coverart/nonexistent",
            "/fanart/v3/nonexistent",
            "/musicbrainz/ws/2/nonexistent",
            "/music/nonexistent",
        ] {
            let (status, body) = error_body("GET", path).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "expected 404: {path}");
            let v: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(v["error"], format!("shirabe: no such route: GET {path}"), "body: {path}");
        }
    }
}
