use sqlx::postgres::{PgPool, PgPoolOptions};

/// Build a Postgres connection pool against the given URL.
pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new().max_connections(max_connections).connect(database_url).await
}

/// The set of database pools shirabe may hold (Option A multi-database layout):
///
/// - `musicbrainz` — the required, READ-ONLY MusicBrainz mirror (`DATABASE_URL`).
/// - `shirabe` — the optional WRITABLE coordination DB (`SHIRABE_DATABASE_URL`):
///   the `shirabe.source` registry, `xref`, `image_cache`, TMDB/TVDB caches.
/// - `imdb` — the optional WRITABLE IMDb bulk-mirror DB (`IMDB_DATABASE_URL`).
///
/// Only the `shirabe` and `imdb` pools are ever written to; `musicbrainz` stays
/// strictly read-only.
#[derive(Clone)]
pub struct Pools {
    pub musicbrainz: PgPool,
    pub shirabe: Option<PgPool>,
    pub imdb: Option<PgPool>,
}

impl Pools {
    /// Connect the required MB pool and, when their URLs are set, the optional
    /// writable shirabe + imdb pools. The API pod boots with only the MB pool.
    pub async fn connect(
        database_url: &str,
        shirabe_database_url: Option<&str>,
        imdb_database_url: Option<&str>,
        max_connections: u32,
    ) -> Result<Self, sqlx::Error> {
        let musicbrainz = connect(database_url, max_connections).await?;
        let shirabe = match shirabe_database_url {
            Some(url) => Some(connect(url, max_connections).await?),
            None => None,
        };
        let imdb = match imdb_database_url {
            Some(url) => Some(connect(url, max_connections).await?),
            None => None,
        };
        Ok(Self { musicbrainz, shirabe, imdb })
    }
}
