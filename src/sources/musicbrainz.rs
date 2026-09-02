//! MusicBrainz source — the read-only ws/2 mirror, formalized as a [`Source`]
//! (SHIB-4).
//!
//! MusicBrainz is already synced into the read-only `musicbrainz` schema by
//! musicbrainz-docker replication, so there is nothing for shirabe to ingest:
//! `refresh()` is a true no-op that records success (we never write the
//! `musicbrainz` database), and `health()` pings the mirror, checks the pg_trgm
//! search indexes from migration 0001 are present, and reports row counts. The
//! ws/2 HTTP surface is served unchanged by [`crate::handlers`] / [`crate::repo`].

use async_trait::async_trait;
use serde_json::json;
use sqlx::{PgPool, Row};

use super::{IngestMode, RefreshCtx, RefreshReport, Source, SourceHealth};

/// The MusicBrainz read-only mirror, exposed as a [`Source`].
pub struct MusicBrainzSource {
    pool: PgPool,
}

impl MusicBrainzSource {
    /// Source id / `shirabe.source.name` primary key.
    pub const ID: &'static str = "musicbrainz";

    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Count the pg_trgm trigram search indexes created by migration 0001 that are
/// present on the mirror. A healthy mirror has these; their absence means the
/// `migrations/` layer was not applied and searches will be slow/failing.
async fn count_search_indexes(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(
        "SELECT count(*) AS n
           FROM pg_indexes
          WHERE schemaname = 'musicbrainz'
            AND indexname LIKE 'shirabe_%_trgm%'",
    )
    .fetch_one(pool)
    .await?;
    Ok(row.get::<i64, _>("n"))
}

/// Planner row estimate (`pg_class.reltuples`) for one mirror table; an exact
/// `count(*)` on the big mirror tables takes seconds and would stall `/health/sources`.
async fn estimate_rows(pool: &PgPool, table: &str) -> Result<i64, sqlx::Error> {
    // `table` is a fixed literal from our own call sites, never user input.
    sqlx::query_scalar::<_, i64>(&format!(
        "SELECT reltuples::bigint FROM pg_class WHERE oid = 'musicbrainz.{table}'::regclass"
    ))
    .fetch_one(pool)
    .await
}

#[async_trait]
impl Source for MusicBrainzSource {
    fn id(&self) -> &str {
        Self::ID
    }

    fn ingest_mode(&self) -> IngestMode {
        IngestMode::ReadOnlyMirror
    }

    async fn refresh(&self, ctx: &RefreshCtx) -> RefreshReport {
        // Read-only mirror: nothing to ingest, and we never write `musicbrainz`
        // (not even ANALYZE — keep it strictly read-only). Confirm reachability
        // so the persisted status reflects reality, then record success.
        match sqlx::query("SELECT 1").execute(&ctx.pools.musicbrainz).await {
            Ok(_) => RefreshReport::ok("read-only mirror, nothing to ingest")
                .with_detail(json!({ "mode": "read_only_mirror", "reachable": true })),
            Err(e) => RefreshReport::failed(format!("mirror unreachable: {e}")),
        }
    }

    async fn health(&self) -> SourceHealth {
        // Ping + index presence + a representative row estimate.
        let index_count = match count_search_indexes(&self.pool).await {
            Ok(n) => n,
            Err(e) => {
                return SourceHealth {
                    source: self.id().to_string(),
                    reachable: false,
                    detail: format!("musicbrainz mirror unreachable: {e}"),
                };
            }
        };
        let artist_rows = estimate_rows(&self.pool, "artist").await.unwrap_or(-1);
        SourceHealth {
            source: self.id().to_string(),
            reachable: true,
            detail: format!(
                "musicbrainz mirror reachable; {index_count} trgm search index(es); \
                 artist rows~{artist_rows}"
            ),
        }
    }
}
