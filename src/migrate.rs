//! In-binary schema bootstrap for the dedicated writable databases.
//!
//! The per-provider databases (`shirabe`, `imdb`, `tmdb`, `tvdb`) come up empty
//! in-cluster and there is no external migration runner. `shirabe migrate <db>`
//! connects the matching pool and applies that DB's migration SQL, which is
//! EMBEDDED into the binary via [`include_str!`] so it ships in the image with no
//! filesystem dependency. The migrations are idempotent DDL
//! (`CREATE … IF NOT EXISTS`, `CREATE EXTENSION IF NOT EXISTS`), so re-running is
//! safe. The read-only `musicbrainz` mirror is NOT migrated here — its migrations
//! are applied to the mirror out of band.

use sqlx::PgPool;

use crate::config::Config;
use crate::db::connect;

/// Embedded migration SQL for the `shirabe` coordination DB.
const SHIRABE_SQL: &str = include_str!("../migrations/shirabe/0001_init.sql");
/// Embedded migration SQL for the `imdb` bulk-mirror DB.
const IMDB_SQL: &str = include_str!("../migrations/imdb/0001_imdb_tables.sql");
/// Embedded migration SQL for the `tmdb` cache/index DB.
const TMDB_SQL: &str = include_str!("../migrations/tmdb/0001_tmdb_tables.sql");
/// Embedded migration SQL for the `tvdb` cache DB.
const TVDB_SQL: &str = include_str!("../migrations/tvdb/0001_tvdb_tables.sql");

/// The four writable databases that `shirabe migrate` can bootstrap, in apply
/// order for `migrate all`.
const MIGRATABLE: &[&str] = &["shirabe", "imdb", "tmdb", "tvdb"];

/// Resolve a db id to its embedded migration SQL. Returns `None` for unknown ids.
#[must_use]
fn embedded_sql(db: &str) -> Option<&'static str> {
    match db {
        "shirabe" => Some(SHIRABE_SQL),
        "imdb" => Some(IMDB_SQL),
        "tmdb" => Some(TMDB_SQL),
        "tvdb" => Some(TVDB_SQL),
        _ => None,
    }
}

/// Resolve a db id to its configured connection URL, if any.
fn db_url<'a>(config: &'a Config, db: &str) -> Option<&'a str> {
    match db {
        "shirabe" => config.shirabe_database_url.as_deref(),
        "imdb" => config.imdb_database_url.as_deref(),
        "tmdb" => config.tmdb_database_url.as_deref(),
        "tvdb" => config.tvdb_database_url.as_deref(),
        _ => None,
    }
}

/// Apply one database's embedded migration SQL against its configured pool.
/// Errors (and the caller exits non-zero) when the db id is unknown, its URL is
/// unset, or the connection / SQL fails.
async fn migrate_one(config: &Config, db: &str) -> anyhow::Result<()> {
    let sql = embedded_sql(db)
        .ok_or_else(|| anyhow::anyhow!("unknown db `{db}`; known: {}", MIGRATABLE.join(", ")))?;
    let url = db_url(config, db).ok_or_else(|| {
        anyhow::anyhow!(
            "{}_DATABASE_URL is not set; cannot migrate `{db}`",
            db.to_ascii_uppercase()
        )
    })?;
    tracing::info!(db, "applying embedded migration");
    let pool = connect(url, config.db_pool_size).await?;
    apply_sql(&pool, sql).await?;
    pool.close().await;
    tracing::info!(db, "migration applied");
    Ok(())
}

/// Execute a migration file's full SQL against the pool. The files are simple
/// idempotent DDL; Postgres' simple-query protocol runs the whole multi-statement
/// string in one round-trip via `execute`.
async fn apply_sql(pool: &PgPool, sql: &str) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(sql).execute(pool).await?;
    Ok(())
}

/// Entry point for `shirabe migrate <db>`. `all` migrates every writable DB whose
/// URL is configured (absent pools are skipped with a log line); a single db id
/// migrates exactly that one (erroring if its URL is unset).
pub async fn run(config: &Config, db: &str) -> anyhow::Result<()> {
    if db == "all" {
        let mut applied = 0u32;
        for &name in MIGRATABLE {
            if db_url(config, name).is_some() {
                migrate_one(config, name).await?;
                applied += 1;
            } else {
                tracing::info!(db = name, "URL not configured; skipping");
            }
        }
        tracing::info!(applied, "migrate all complete");
        Ok(())
    } else {
        migrate_one(config, db).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each known db id maps to a non-empty embedded SQL constant, and the right
    /// one (smoke-checked by a table name unique to that file). Unknown ids → None.
    #[test]
    fn maps_db_id_to_embedded_sql() {
        assert!(embedded_sql("shirabe").unwrap().contains("shirabe.source"));
        assert!(embedded_sql("imdb").unwrap().contains("imdb_title_basics"));
        assert!(embedded_sql("tmdb").unwrap().contains("tmdb_id_index"));
        assert!(embedded_sql("tvdb").unwrap().contains("tvdb_cache"));
        assert!(embedded_sql("musicbrainz").is_none());
        assert!(embedded_sql("nope").is_none());
    }

    /// The moved tables now live ONLY in their dedicated DBs' SQL, not the shirabe
    /// migration — guards the five-DB split against regressions.
    #[test]
    fn shirabe_sql_no_longer_defines_tmdb_or_tvdb_tables() {
        let shirabe = embedded_sql("shirabe").unwrap();
        assert!(!shirabe.contains("CREATE TABLE IF NOT EXISTS shirabe.tmdb_cache"));
        assert!(!shirabe.contains("CREATE TABLE IF NOT EXISTS shirabe.tvdb_cache"));
        assert!(!shirabe.contains("CREATE TABLE IF NOT EXISTS shirabe.tmdb_id_index"));
        assert!(embedded_sql("tmdb").unwrap().contains("CREATE TABLE IF NOT EXISTS tmdb_cache"));
        assert!(embedded_sql("tvdb").unwrap().contains("CREATE TABLE IF NOT EXISTS tvdb_cache"));
    }
}
