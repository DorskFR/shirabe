use sqlx::postgres::{PgPool, PgPoolOptions};

/// Build a Postgres connection pool against the given URL (eager: opens a
/// connection now, so an unreachable DB fails fast at startup).
pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new().max_connections(max_connections).connect(database_url).await
}

/// Build a LAZILY-connected pool: returns immediately, opening connections on
/// first use. Used for the optional writable pools so the API still boots when a
/// writable DB is briefly unavailable or saturated (e.g. during a bulk sync) —
/// a busy imdb/tmdb DB must never crash-loop the API; queries just degrade.
pub fn connect_lazy(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new().max_connections(max_connections).connect_lazy(database_url)
}

/// The set of database pools shirabe may hold (five-database layout, one Postgres
/// per provider):
///
/// - `musicbrainz` — the required, READ-ONLY MusicBrainz mirror (`DATABASE_URL`).
/// - `shirabe` — the optional WRITABLE coordination DB (`SHIRABE_DATABASE_URL`):
///   the `shirabe.source` registry, `xref`, `image_cache`.
/// - `imdb` — the optional WRITABLE IMDb bulk-mirror DB (`IMDB_DATABASE_URL`).
/// - `tmdb` — the optional WRITABLE TMDB cache/index DB (`TMDB_DATABASE_URL`):
///   `tmdb_cache` + `tmdb_id_index`.
/// - `tvdb` — the optional WRITABLE TheTVDB cache DB (`TVDB_DATABASE_URL`):
///   `tvdb_cache`.
///
/// Only the `shirabe`, `imdb`, `tmdb`, and `tvdb` pools are ever written to;
/// `musicbrainz` stays strictly read-only.
#[derive(Clone)]
pub struct Pools {
    pub musicbrainz: PgPool,
    pub shirabe: Option<PgPool>,
    pub imdb: Option<PgPool>,
    pub tmdb: Option<PgPool>,
    pub tvdb: Option<PgPool>,
}

impl Pools {
    /// Connect the required MB pool and, when their URLs are set, the optional
    /// writable shirabe + imdb + tmdb + tvdb pools. The API pod boots with only
    /// the MB pool.
    // `tmdb`/`tvdb` differ by one char (fixed provider names) → similar_names noise.
    #[allow(clippy::similar_names)]
    pub async fn connect(
        database_url: &str,
        shirabe_database_url: Option<&str>,
        imdb_database_url: Option<&str>,
        tmdb_database_url: Option<&str>,
        tvdb_database_url: Option<&str>,
        max_connections: u32,
    ) -> Result<Self, sqlx::Error> {
        let musicbrainz = connect(database_url, max_connections).await?;
        // Writable pools are LAZY: a saturated/unavailable provider DB must not
        // crash the API at boot (queries degrade per-request instead).
        let connect_opt = |url: Option<&str>| match url {
            Some(url) => Ok::<_, sqlx::Error>(Some(connect_lazy(url, max_connections)?)),
            None => Ok(None),
        };
        let shirabe = connect_opt(shirabe_database_url)?;
        let imdb = connect_opt(imdb_database_url)?;
        let tmdb = connect_opt(tmdb_database_url)?;
        let tvdb = connect_opt(tvdb_database_url)?;
        Ok(Self { musicbrainz, shirabe, imdb, tmdb, tvdb })
    }
}
