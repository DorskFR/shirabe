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
    /// Apply a writable database's embedded migration SQL, then exit. Used to
    /// schema-bootstrap the dedicated per-provider databases in-cluster (they come
    /// up empty and there is no external migration runner). Idempotent.
    ///
    /// `db` is one of `shirabe`, `imdb`, `tmdb`, `tvdb`, or `all` (every DB whose
    /// URL is configured). The read-only `musicbrainz` mirror is NOT migrated here.
    Migrate {
        /// Database to migrate: `shirabe` | `imdb` | `tmdb` | `tvdb` | `all`.
        db: String,
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

    /// Postgres connection string for the writable `tmdb` cache/index database
    /// (`tmdb_cache` + `tmdb_id_index`). Optional so the API pod still boots when
    /// unset; only the TMDB source/facade needs it.
    #[arg(long, env = "TMDB_DATABASE_URL")]
    pub tmdb_database_url: Option<String>,

    /// Postgres connection string for the writable `tvdb` cache database
    /// (`tvdb_cache`). Optional so the API pod still boots when unset; only the
    /// TVDB source/facade needs it.
    #[arg(long, env = "TVDB_DATABASE_URL")]
    pub tvdb_database_url: Option<String>,

    /// Postgres connection string for the writable `fanart` cache database
    /// (`fanart_cache`). Optional so the API pod still boots when unset; only the
    /// fanart.tv facade needs it.
    #[arg(long, env = "FANART_DATABASE_URL")]
    pub fanart_database_url: Option<String>,

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
    /// 0.3 (raised from 0.2 in SHIB-19) is a less permissive cutoff that keeps
    /// the trigram candidate set from over-inflating on short queries.
    #[arg(long, env = "SHIRABE_SIMILARITY_THRESHOLD", default_value_t = 0.3)]
    pub similarity_threshold: f64,

    /// Per-connection `statement_timeout` (milliseconds) applied to trigram search
    /// sessions (SHIB-19). Without it a runaway query runs unbounded server-side and
    /// only dies at the client's request cap; this makes it fail fast with a clear
    /// Postgres error. Set on the SAME connection that runs the search (see
    /// [`crate::search::configure_search_session`]).
    #[arg(long, env = "SHIRABE_STATEMENT_TIMEOUT_MS", default_value_t = 10_000)]
    pub statement_timeout_ms: i64,

    /// Per-connection `work_mem` applied to trigram search sessions (SHIB-16).
    /// The default 4MB makes the GIN bitmap scan over the 58M-row
    /// `imdb_title_akas` table go lossy, forcing a multi-million-row heap
    /// recheck (55s cold). 256MB keeps the bitmap exact. Any Postgres memory
    /// unit is accepted (e.g. `256MB`, `1GB`); the value is sanitised before
    /// being spliced into `SET work_mem` (see `search::sanitize_work_mem`).
    #[arg(long, env = "SHIRABE_SEARCH_WORK_MEM", default_value = "256MB")]
    pub search_work_mem: String,

    /// Server-side TMDB v3 API key. Optional: when unset, the `/3` facade and the
    /// `tmdb` source degrade gracefully (503-style error / cache-only) rather than
    /// panicking, and the API server still boots and serves `/ws/2` + other
    /// facades. The inbound client `api_key` query param is always ignored;
    /// Shirabe holds the real key here.
    #[arg(long, env = "TMDB_API_KEY")]
    pub tmdb_api_key: Option<String>,

    /// TTL (in days) for cached TMDB v3 payloads in the `tmdb_cache` table. A cache
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

    /// TTL (in days) for cached TheTVDB v4 payloads in the `tvdb_cache` table. A
    /// cache row older than this is treated as stale and re-fetched from upstream.
    #[arg(long, env = "TVDB_CACHE_TTL_DAYS", default_value_t = 7)]
    pub tvdb_cache_ttl_days: i64,

    /// Server-side fanart.tv v3 project API key. Optional: when unset, the `/v3`
    /// facade degrades gracefully (503-style error / cache-only) rather than
    /// panicking, and the API server still boots and serves `/ws/2` + other
    /// facades. Shirabe holds the real key here and never re-exposes it.
    #[arg(long, env = "FANART_API_KEY")]
    pub fanart_api_key: Option<String>,

    /// Optional personal fanart.tv API key, sent as the `client_key` query param
    /// alongside the project `api_key` (fanart.tv's supporter convention). Held
    /// server-side; never re-exposed to clients.
    #[arg(long, env = "FANART_PERSONAL_API_KEY")]
    pub fanart_personal_api_key: Option<String>,

    /// TTL (in days) for cached fanart.tv v3 payloads in the `fanart_cache` table. A
    /// cache row older than this is treated as stale and re-fetched from upstream.
    #[arg(long, env = "FANART_CACHE_TTL_DAYS", default_value_t = 7)]
    pub fanart_cache_ttl_days: i64,

    /// Enable the opt-in SQL query explorer at `/debug/queries` (SHIB-21). OFF by
    /// default. When set, shirabe serves a self-generated page listing every SQL
    /// statement it runs and a runner that executes each (and `EXPLAIN [ANALYZE]`)
    /// against the live pools with adjustable params / session knobs — for
    /// diagnosing slow trigram searches. Params are bound (never interpolated) and
    /// only catalog SQL is runnable; still, keep this off in any exposed
    /// deployment as it can run arbitrary EXPLAIN ANALYZE load against the DBs.
    #[arg(long, env = "SHIRABE_DEBUG_UI", default_value_t = false)]
    pub debug_ui: bool,

    /// Externally-reachable base URL of the `caache` image proxy (SHIB-9). TMDB/TVDB
    /// poster/artwork URLs in the `/3` and `/v4` facade payloads are rewritten to
    /// route through caache's `/_ia/<host>/<path>` passthrough so the bytes are
    /// fetched + cached there (Shirabe stays stateless on image bytes). These URLs
    /// land in the browser/UI, so this is the public host, not the in-cluster svc.
    /// When empty, URLs are rewritten to Shirabe's OWN relative `/_ia/<host>/<path>`
    /// route (the native Cover Art Archive proxy folded in from `caache`), so image
    /// bytes are fetched + cached by Shirabe itself with no separate proxy.
    #[arg(long, env = "SHIRABE_CAACHE_BASE_URL", default_value = "")]
    pub caache_base_url: Option<String>,

    /// Filesystem directory backing the native Cover Art Archive byte cache
    /// (`/_ia/<host>/<path>` passthrough). Should live on a PVC so cached image
    /// bytes survive restarts; nginx-equivalent to `caache`'s on-disk cache.
    #[arg(long, env = "SHIRABE_COVERART_CACHE_DIR", default_value = "/var/cache/shirabe/coverart")]
    pub coverart_cache_dir: String,

    /// Soft upper bound (bytes) on the on-disk Cover Art byte cache. When a write
    /// pushes the cache over this, the oldest entries (by mtime) are evicted until
    /// it is back under. Default ~9 GiB.
    #[arg(long, env = "SHIRABE_COVERART_CACHE_MAX_BYTES", default_value_t = 9_663_676_416)]
    pub coverart_cache_max_bytes: u64,

    /// Positive-cache TTL (seconds) for 200 image responses. Default 30 days.
    #[arg(long, env = "SHIRABE_COVERART_POSITIVE_TTL_SECS", default_value_t = 2_592_000)]
    pub coverart_positive_ttl_secs: u64,

    /// Negative-cache TTL (seconds) for 404 responses. Default 6 hours.
    #[arg(long, env = "SHIRABE_COVERART_NEGATIVE_TTL_SECS", default_value_t = 21_600)]
    pub coverart_negative_ttl_secs: u64,

    /// Upstream Cover Art Archive base for the `/release` and `/release-group`
    /// redirect layer.
    #[arg(
        long,
        env = "SHIRABE_COVERART_UPSTREAM_BASE",
        default_value = "https://coverartarchive.org"
    )]
    pub coverart_upstream_base: String,
}

impl Config {
    /// Clamp a requested limit into `[1, max_limit]`, falling back to the default.
    #[must_use]
    pub fn resolve_limit(&self, requested: Option<i64>) -> i64 {
        requested.unwrap_or(self.default_limit).clamp(1, self.max_limit)
    }
}
