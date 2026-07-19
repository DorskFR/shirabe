//! In-binary schema bootstrap for the dedicated writable databases.
//!
//! The per-provider databases (`shirabe`, `imdb`, `tmdb`, `tvdb`) come up empty
//! in-cluster and there is no external migration runner. `shirabe migrate <db>`
//! connects the matching pool and applies that DB's migration SQL, which is
//! EMBEDDED into the binary via [`include_str!`] so it ships in the image with no
//! filesystem dependency. The migrations are idempotent DDL
//! (`CREATE … IF NOT EXISTS`, `CREATE EXTENSION IF NOT EXISTS`), so re-running is
//! safe. The `musicbrainz` mirror can also be migrated explicitly
//! (`shirabe migrate musicbrainz`, against `DATABASE_URL`) to apply shirabe's
//! pg_trgm gin/gist index layer — but it is excluded from `migrate all` so a heavy
//! (non-CONCURRENTLY) index build never runs implicitly on the live read path.

use sqlx::PgPool;

use crate::config::Config;
use crate::db::connect;

/// Embedded migration SQL for the `shirabe` coordination DB.
const SHIRABE_SQL: &[&str] = &[include_str!("../migrations/shirabe/0001_init.sql")];
/// Embedded migration SQL for the `imdb` bulk-mirror DB (applied in file order).
const IMDB_SQL: &[&str] = &[
    include_str!("../migrations/imdb/0001_imdb_tables.sql"),
    include_str!("../migrations/imdb/0002_title_knn_gist.sql"),
    include_str!("../migrations/imdb/0003_title_fts.sql"),
];
/// Embedded migration SQL for the `tmdb` cache/index DB (applied in file order).
const TMDB_SQL: &[&str] = &[
    include_str!("../migrations/tmdb/0001_tmdb_tables.sql"),
    include_str!("../migrations/tmdb/0002_tmdb_cache_imdb_id_idx.sql"),
    include_str!("../migrations/tmdb/0003_id_index_name_fts.sql"),
];
/// Embedded migration SQL for the `tvdb` cache DB.
const TVDB_SQL: &[&str] = &[include_str!("../migrations/tvdb/0001_tvdb_tables.sql")];
/// Embedded migration SQL for the `fanart` cache DB.
const FANART_SQL: &[&str] = &[include_str!("../migrations/fanart/0001_fanart_tables.sql")];
/// Embedded migration SQL for the `musicbrainz` mirror (the pg_trgm GIN + FTS
/// index layer shirabe adds on top of the replicated MB schema). Applied against
/// `DATABASE_URL`. Idempotent (`CREATE INDEX IF NOT EXISTS`), so on a mirror that
/// already carries the indexes each file no-ops.
const MUSICBRAINZ_SQL: &[&str] = &[
    include_str!("../migrations/0001_shirabe_search_indexes.sql"),
    include_str!("../migrations/0002_release_date_year_indexes.sql"),
    include_str!("../migrations/0003_search_fts.sql"),
];

/// The four writable databases that `shirabe migrate all` bootstraps, in apply
/// order. `musicbrainz` is deliberately NOT in this set: its migrations are plain
/// (non-CONCURRENTLY) `CREATE INDEX`, so it is migrated only when named explicitly
/// (`shirabe migrate musicbrainz`) — never implicitly on every deploy, so a heavy
/// index build can't lock the live mirror unexpectedly.
const MIGRATABLE: &[&str] = &["shirabe", "imdb", "tmdb", "tvdb", "fanart"];

/// Resolve a db id to its embedded migration SQL files (in apply order). Every
/// file is idempotent DDL, so all files are (re)applied on each run — a fresh DB
/// gets the full set, an existing one no-ops through the already-applied files.
/// Returns `None` for unknown ids.
#[must_use]
fn embedded_sql(db: &str) -> Option<&'static [&'static str]> {
    match db {
        "shirabe" => Some(SHIRABE_SQL),
        "imdb" => Some(IMDB_SQL),
        "tmdb" => Some(TMDB_SQL),
        "tvdb" => Some(TVDB_SQL),
        "fanart" => Some(FANART_SQL),
        "musicbrainz" => Some(MUSICBRAINZ_SQL),
        _ => None,
    }
}

/// Resolve a db id to its configured connection URL, if any. `musicbrainz` maps
/// to the (required) `DATABASE_URL` — the mirror shirabe reads and now owns the
/// index layer of.
fn db_url<'a>(config: &'a Config, db: &str) -> Option<&'a str> {
    match db {
        "shirabe" => config.shirabe_database_url.as_deref(),
        "imdb" => config.imdb_database_url.as_deref(),
        "tmdb" => config.tmdb_database_url.as_deref(),
        "tvdb" => config.tvdb_database_url.as_deref(),
        "fanart" => config.fanart_database_url.as_deref(),
        "musicbrainz" => Some(&config.database_url),
        _ => None,
    }
}

/// Apply one database's embedded migration SQL against its configured pool.
/// Errors (and the caller exits non-zero) when the db id is unknown, its URL is
/// unset, or the connection / SQL fails.
async fn migrate_one(config: &Config, db: &str) -> anyhow::Result<()> {
    let files = embedded_sql(db).ok_or_else(|| {
        anyhow::anyhow!("unknown db `{db}`; known: {}, musicbrainz", MIGRATABLE.join(", "))
    })?;
    let url = db_url(config, db).ok_or_else(|| {
        anyhow::anyhow!(
            "{}_DATABASE_URL is not set; cannot migrate `{db}`",
            db.to_ascii_uppercase()
        )
    })?;
    tracing::info!(db, files = files.len(), "applying embedded migrations");
    let pool = connect(url, config.db_pool_size).await?;
    for sql in files {
        apply_sql(&pool, sql).await?;
    }
    pool.close().await;
    tracing::info!(db, "migrations applied");
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

    /// Concatenate a db's embedded migration files for content smoke-checks.
    fn joined(db: &str) -> String {
        embedded_sql(db).unwrap().join("\n")
    }

    /// Each known db id maps to non-empty embedded SQL files, and the right ones
    /// (smoke-checked by a table name unique to that db). Unknown ids → None.
    #[test]
    fn maps_db_id_to_embedded_sql() {
        assert!(joined("shirabe").contains("shirabe.source"));
        assert!(joined("imdb").contains("imdb_title_basics"));
        assert!(joined("tmdb").contains("tmdb_id_index"));
        assert!(joined("tvdb").contains("tvdb_cache"));
        assert!(joined("fanart").contains("fanart_cache"));
        // musicbrainz maps to the mirror index layer (pg_trgm GIN + FTS), but is
        // excluded from `migrate all` (see `MIGRATABLE`).
        assert!(joined("musicbrainz").contains("to_tsvector"));
        assert!(!MIGRATABLE.contains(&"musicbrainz"));
        assert!(embedded_sql("nope").is_none());
    }

    /// The moved tables now live ONLY in their dedicated DBs' SQL, not the shirabe
    /// migration — guards the five-DB split against regressions.
    #[test]
    fn shirabe_sql_no_longer_defines_tmdb_or_tvdb_tables() {
        let shirabe = joined("shirabe");
        assert!(!shirabe.contains("CREATE TABLE IF NOT EXISTS shirabe.tmdb_cache"));
        assert!(!shirabe.contains("CREATE TABLE IF NOT EXISTS shirabe.tvdb_cache"));
        assert!(!shirabe.contains("CREATE TABLE IF NOT EXISTS shirabe.tmdb_id_index"));
        assert!(joined("tmdb").contains("CREATE TABLE IF NOT EXISTS tmdb_cache"));
        assert!(joined("tvdb").contains("CREATE TABLE IF NOT EXISTS tvdb_cache"));
    }

    /// The tmdb DB carries the SHIB-15 imdb_id cross-ref index migration, applied
    /// after the base tables (file order = apply order).
    #[test]
    fn tmdb_sql_includes_imdb_id_xref_index() {
        let files = embedded_sql("tmdb").unwrap();
        assert_eq!(files.len(), 3);
        assert!(files[0].contains("CREATE TABLE IF NOT EXISTS tmdb_cache"));
        assert!(files[1].contains("tmdb_cache_kind_imdb_id_idx"));
        assert!(files[2].contains("tmdb_id_index_name_fts_ua"));
    }
}
