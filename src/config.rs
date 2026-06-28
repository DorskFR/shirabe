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

    /// Postgres connection string for the writable `shirabe` coordination database
    /// (source registry, xref, image_cache, TMDB/TVDB caches). Optional so the API
    /// pod still boots when unset; `shirabe sync <source>` errors if a source needs
    /// it and it is missing.
    #[arg(long, env = "SHIRABE_DATABASE_URL")]
    pub shirabe_database_url: Option<String>,

    /// Postgres connection string for the writable `imdb` bulk-mirror database
    /// (IMDb TSV tables, added in SHIB-5). Optional; only the IMDb source needs it.
    #[arg(long, env = "IMDB_DATABASE_URL")]
    pub imdb_database_url: Option<String>,

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

    /// Server-side TMDB v3 API key. Optional: when unset, the `/3` facade and the
    /// `tmdb` source degrade gracefully (503-style error / cache-only) rather than
    /// panicking, and the API server still boots and serves `/ws/2` + other
    /// facades. The inbound client `api_key` query param is always ignored;
    /// Shirabe holds the real key here.
    #[arg(long, env = "TMDB_API_KEY")]
    pub tmdb_api_key: Option<String>,

    /// TTL (in days) for cached TMDB v3 payloads in `shirabe.tmdb_cache`. A cache
    /// row older than this is treated as stale and re-fetched from upstream.
    #[arg(long, env = "TMDB_CACHE_TTL_DAYS", default_value_t = 7)]
    pub tmdb_cache_ttl_days: i64,

    /// Server-side TheTVDB v4 project API key. Optional: when unset, the `/v4`
    /// facade and the `tvdb` source degrade gracefully (failure-shaped error /
    /// cache-only) rather than panicking, and the API server still boots and
    /// serves `/ws/2` + other facades. Clients send their own apikey/pin to
    /// `/v4/login`; those are accepted and ignored — Shirabe holds the real key
    /// here and mints its own token.
    #[arg(long, env = "TVDB_API_KEY")]
    pub tvdb_api_key: Option<String>,

    /// Optional operator PIN paired with `TVDB_API_KEY` for TheTVDB's
    /// user-supported (licensed) keys. Held server-side; never re-exposed to
    /// clients.
    #[arg(long, env = "TVDB_PIN")]
    pub tvdb_pin: Option<String>,

    /// TTL (in days) for cached TheTVDB v4 payloads in `shirabe.tvdb_cache`. A
    /// cache row older than this is treated as stale and re-fetched from upstream.
    #[arg(long, env = "TVDB_CACHE_TTL_DAYS", default_value_t = 7)]
    pub tvdb_cache_ttl_days: i64,

    /// Externally-reachable base URL of the `caache` image proxy (SHIB-9). TMDB/TVDB
    /// poster/artwork URLs in the `/3` and `/v4` facade payloads are rewritten to
    /// route through caache's `/_ia/<host>/<path>` passthrough so the bytes are
    /// fetched + cached there (Shirabe stays stateless on image bytes). These URLs
    /// land in the browser/UI, so this is the public host, not the in-cluster svc.
    /// When unset/empty, rewriting is DISABLED — original upstream URLs are emitted
    /// unchanged (graceful no-op).
    #[arg(long, env = "SHIRABE_CAACHE_BASE_URL", default_value = "https://caache.dorsk.dev")]
    pub caache_base_url: Option<String>,
}

impl Config {
    /// Clamp a requested limit into `[1, max_limit]`, falling back to the default.
    #[must_use]
    pub fn resolve_limit(&self, requested: Option<i64>) -> i64 {
        requested.unwrap_or(self.default_limit).clamp(1, self.max_limit)
    }
}
