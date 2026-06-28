//! shirabe — a small, fast MusicBrainz ws/2 subset served directly from a
//! MusicBrainz Postgres mirror via pg_trgm.

mod config;
mod date;
mod db;
mod error;
mod facades;
mod handlers;
mod images;
mod models;
mod query;
mod repo;
mod search;
mod sources;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use clap::Parser;
use sqlx::PgPool;
use tower_http::trace::TraceLayer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::{Cli, Command, Config};
use crate::db::Pools;
use crate::sources::Registry;
use crate::sources::tvdb::TokenStore;

/// Shared application state handed to every handler.
pub struct AppState {
    /// All DB pools. `pools.musicbrainz` is the read-only mirror that the ws/2
    /// handlers query; the optional shirabe/imdb pools back coordination/ingest.
    pub pools: Pools,
    pub config: Config,
    pub registry: Registry,
    /// Shared in-memory TheTVDB bearer token, minted from the server-side key and
    /// reused by the `/v4` facade and the `tvdb` source.
    pub tvdb_tokens: TokenStore,
}

impl AppState {
    /// The read-only MusicBrainz mirror pool the ws/2 handlers query.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pools.musicbrainz
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();

    let cli = Cli::parse();
    let config = cli.config;
    let pools = Pools::connect(
        &config.database_url,
        config.shirabe_database_url.as_deref(),
        config.imdb_database_url.as_deref(),
        config.db_pool_size,
    )
    .await?;
    let tvdb_tokens = TokenStore::new();
    let registry = Registry::with_defaults(pools.clone(), config.clone(), tvdb_tokens.clone());

    match cli.command {
        // CronJob entrypoint: refresh one source and exit.
        Some(Command::Sync { source }) => run_sync(&registry, &source).await,
        // Default: start the HTTP server exactly as before.
        None => serve(config, pools, registry, tvdb_tokens).await,
    }
}

/// Run a single source's `refresh()` and exit non-zero on failure or unknown id.
async fn run_sync(registry: &Registry, source: &str) -> anyhow::Result<()> {
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
async fn serve(
    config: Config,
    pools: Pools,
    registry: Registry,
    tvdb_tokens: TokenStore,
) -> anyhow::Result<()> {
    let bind = config.bind.clone();
    tracing::info!(bind = %bind, "starting shirabe");
    let state = Arc::new(AppState { pools, config, registry, tvdb_tokens });

    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(addr = %bind, "listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        .route("/health/sources", get(handlers::health_sources))
        .route("/ws/2", get(handlers::health))
        .route("/ws/2/artist", get(handlers::search_artist))
        .route("/ws/2/artist/{mbid}", get(handlers::lookup_artist))
        .route("/ws/2/release", get(handlers::search_release))
        .route("/ws/2/release/{mbid}", get(handlers::lookup_release))
        .route("/ws/2/recording", get(handlers::search_recording))
        .route("/ws/2/recording/{mbid}", get(handlers::lookup_recording))
        // Native-shape provider facades (routing skeletons; 501 until SHIB-4/5).
        // Kusaritoi points `tvdb.base_url` → …/v4 and `tmdb.base_url` → …/3.
        .merge(facades::tvdb::router())
        .merge(facades::tmdb::router())
        // Per-request access log (method, path, status, latency). Enable with
        // `tower_http=debug` in RUST_LOG to see every ws/2 call.
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
