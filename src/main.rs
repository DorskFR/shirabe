//! shirabe — a small, fast MusicBrainz ws/2 subset served directly from a
//! MusicBrainz Postgres mirror via pg_trgm.

mod config;
mod date;
mod db;
mod error;
mod handlers;
mod models;
mod query;
mod repo;

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use clap::Parser;
use sqlx::PgPool;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use crate::config::Config;

/// Shared application state handed to every handler.
pub struct AppState {
    pub pool: PgPool,
    pub config: Config,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();

    let config = Config::parse();
    tracing::info!(bind = %config.bind, "starting shirabe");

    let pool = db::connect(&config.database_url, config.db_pool_size).await?;
    let bind = config.bind.clone();
    let state = Arc::new(AppState { pool, config });

    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(addr = %bind, "listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health", get(handlers::health))
        .route("/ws/2", get(handlers::health))
        .route("/ws/2/artist", get(handlers::search_artist))
        .route("/ws/2/release", get(handlers::search_release))
        .route("/ws/2/release/{mbid}", get(handlers::lookup_release))
        .route("/ws/2/recording", get(handlers::search_recording))
        .route("/ws/2/recording/{mbid}", get(handlers::lookup_recording))
        .with_state(state)
}
