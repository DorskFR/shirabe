//! MusicBrainz source — a minimal `ReadOnlyMirror` stub (SHIB-3).
//!
//! MusicBrainz is already synced into the read-only `musicbrainz` Postgres
//! schema, so there is nothing to ingest: `refresh()` runs `ANALYZE`-style
//! maintenance only, and `health()` pings the DB. Full ws/2 wrapping behind the
//! source model lands in SHIB-4; here it is just a registered source so the
//! trait/registry/sync wiring has a concrete member.

use async_trait::async_trait;
use serde_json::json;
use sqlx::PgPool;

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

#[async_trait]
impl Source for MusicBrainzSource {
    fn id(&self) -> &str {
        Self::ID
    }

    fn ingest_mode(&self) -> IngestMode {
        IngestMode::ReadOnlyMirror
    }

    async fn refresh(&self, ctx: &RefreshCtx) -> RefreshReport {
        // Read-only mirror: nothing to ingest. Confirm the mirror is reachable
        // so the persisted status reflects reality. (A future revision may run
        // ANALYZE on the shirabe-owned tables; we never write `musicbrainz`.)
        match sqlx::query("SELECT 1").execute(&ctx.pool).await {
            Ok(_) => RefreshReport::ok("read-only mirror; nothing to ingest")
                .with_detail(json!({ "mode": "read_only_mirror", "reachable": true })),
            Err(e) => RefreshReport::failed(format!("mirror unreachable: {e}")),
        }
    }

    async fn health(&self) -> SourceHealth {
        let reachable = sqlx::query("SELECT 1").execute(&self.pool).await.is_ok();
        SourceHealth {
            source: self.id().to_string(),
            reachable,
            detail: if reachable {
                "musicbrainz mirror reachable".to_string()
            } else {
                "musicbrainz mirror unreachable".to_string()
            },
        }
    }
}
