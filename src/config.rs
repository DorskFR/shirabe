use clap::{Parser, Subcommand};

/// Top-level CLI. With no subcommand, shirabe starts the axum API server
/// (unchanged behaviour). `shirabe sync <source>` instead runs that source's
/// `refresh()` once and exits — so bulk ingest runs as a CronJob on the same
/// image, independent of the API pod.
#[derive(Debug, Clone, Parser)]
#[command(name = "shirabe", about = "MusicBrainz ws/2 subset served from a Postgres mirror")]
pub struct Cli {
    #[command(flatten)]
    pub config: Config,

    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Subcommands. Absence => run the HTTP server.
#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Run a single source's ingest refresh, then exit (CronJob entrypoint).
    Sync {
        /// Source id to refresh (e.g. `musicbrainz`).
        source: String,
    },
}

/// Runtime configuration, sourced from environment variables (or CLI flags).
#[derive(Debug, Clone, clap::Args)]
pub struct Config {
    /// Postgres connection string for the MusicBrainz mirror (read-only role recommended).
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// Address:port to bind the HTTP server to.
    #[arg(long, env = "SHIRABE_BIND", default_value = "0.0.0.0:8800")]
    pub bind: String,

    /// Maximum size of the Postgres connection pool.
    #[arg(long, env = "SHIRABE_DB_POOL_SIZE", default_value_t = 8)]
    pub db_pool_size: u32,

    /// Default `limit` applied to search endpoints when the client omits one.
    #[arg(long, env = "SHIRABE_DEFAULT_LIMIT", default_value_t = 25)]
    pub default_limit: i64,

    /// Hard cap on the `limit` a client may request.
    #[arg(long, env = "SHIRABE_MAX_LIMIT", default_value_t = 100)]
    pub max_limit: i64,

    /// pg_trgm similarity threshold (0.0-1.0). Rows below this are discarded.
    #[arg(long, env = "SHIRABE_SIMILARITY_THRESHOLD", default_value_t = 0.2)]
    pub similarity_threshold: f64,
}

impl Config {
    /// Clamp a requested limit into `[1, max_limit]`, falling back to the default.
    #[must_use]
    pub fn resolve_limit(&self, requested: Option<i64>) -> i64 {
        requested.unwrap_or(self.default_limit).clamp(1, self.max_limit)
    }
}
