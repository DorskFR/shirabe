use sqlx::postgres::{PgPool, PgPoolOptions};

/// Build the Postgres connection pool against the MusicBrainz mirror.
pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new().max_connections(max_connections).connect(database_url).await
}
