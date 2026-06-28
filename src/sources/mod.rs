//! Extensible source / plugin model (SHIB-3).
//!
//! Every ingest provider — MusicBrainz, IMDb, TMDB, TVDB, Wikidata — is a
//! uniform unit of work behind the [`Source`] trait. The [`Registry`] holds the
//! configured sources; facade routers stay thin and call into sources. Each
//! source owns one row in the writable `shirabe.source` table (name PK,
//! ingest_mode, last_refresh_at, status, detail jsonb); [`refresh`](Source::refresh)
//! and [`health`](Source::health) upsert that row so `/health/sources` (SHIB-12)
//! and the `shirabe sync <source>` CronJob can report freshness.
//!
//! Adding a provider later (IMDb `BulkDump`, TMDB `EnumerateLazyHydrate`, TVDB
//! `LazyScrape`, Wikidata `BulkDump`) is just: implement `Source`, register it.

pub mod imdb;
pub mod musicbrainz;
pub mod wikidata;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use sqlx::PgPool;

use crate::db::Pools;

/// How a source gets its data into Shirabe. Drives whether `shirabe sync`
/// pulls a full dump, enumerates + lazily hydrates, scrapes on demand, or just
/// mirrors a read-only upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestMode {
    /// Periodic full dump ingest (IMDb TSV, Wikidata).
    BulkDump,
    /// Enumerate the id space (e.g. TMDB daily id export), hydrate detail lazily.
    EnumerateLazyHydrate,
    /// No enumeration; fetch + cache on demand (TheTVDB).
    LazyScrape,
    /// An already-mirrored upstream we only read (the MusicBrainz Postgres mirror).
    ReadOnlyMirror,
}

impl IngestMode {
    /// Stable string stored in `shirabe.source.ingest_mode`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BulkDump => "bulk_dump",
            Self::EnumerateLazyHydrate => "enumerate_lazy_hydrate",
            Self::LazyScrape => "lazy_scrape",
            Self::ReadOnlyMirror => "read_only_mirror",
        }
    }
}

impl std::fmt::Display for IngestMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Outcome of a [`Source::refresh`] run, persisted as the source's status/detail.
#[derive(Debug, Clone)]
pub struct RefreshReport {
    /// `true` when the refresh completed without error.
    pub ok: bool,
    /// Human-readable one-line summary.
    pub summary: String,
    /// Structured detail (row counts, dump version, token expiry, …) stored as
    /// `shirabe.source.detail` jsonb.
    pub detail: Value,
}

impl RefreshReport {
    /// A successful refresh with a summary and no extra detail.
    #[must_use]
    pub fn ok(summary: impl Into<String>) -> Self {
        Self { ok: true, summary: summary.into(), detail: Value::Null }
    }

    /// A failed refresh carrying an error summary.
    #[must_use]
    pub fn failed(summary: impl Into<String>) -> Self {
        Self { ok: false, summary: summary.into(), detail: Value::Null }
    }

    /// Attach structured detail.
    #[must_use]
    pub fn with_detail(mut self, detail: Value) -> Self {
        self.detail = detail;
        self
    }

    /// `"ok"` / `"error"` string stored in `shirabe.source.status`.
    #[must_use]
    pub const fn status_str(&self) -> &'static str {
        if self.ok { "ok" } else { "error" }
    }
}

/// Reachability/freshness of a source, queried internally by the registry (and,
/// later, the `/health/sources` HTTP endpoint — SHIB-12).
#[derive(Debug, Clone)]
pub struct SourceHealth {
    /// Source id (matches [`Source::id`]).
    pub source: String,
    /// `true` when the source's backing store/upstream is reachable.
    pub reachable: bool,
    /// Human-readable one-line status.
    pub detail: String,
}

/// A registered ingest provider. Implementations are cheap handles (a pool
/// clone, a client) so the registry can hold `Arc<dyn Source>`.
#[async_trait]
pub trait Source: Send + Sync {
    /// Stable source id, also the `shirabe.source.name` primary key
    /// (e.g. `"musicbrainz"`, `"imdb"`, `"tmdb"`).
    fn id(&self) -> &str;

    /// How this source ingests data.
    fn ingest_mode(&self) -> IngestMode;

    /// Run the source's ingest (bulk dump, enumerate, analyze, …). Invoked by
    /// `shirabe sync <id>` as a CronJob, independent of the API pod. Persisting
    /// the report to `shirabe.source` is the registry's job
    /// ([`Registry::run_refresh`]).
    async fn refresh(&self, ctx: &RefreshCtx) -> RefreshReport;

    /// Probe reachability of the source's backing store / upstream.
    async fn health(&self) -> SourceHealth;
}

/// Context handed to [`Source::refresh`]: the full set of DB pools (MB read pool,
/// optional writable shirabe + imdb pools) plus anything a later source needs
/// (HTTP client, dump dir) can be threaded through here. Each source reaches for
/// the pool(s) it actually needs.
#[derive(Clone)]
pub struct RefreshCtx {
    pub pools: Pools,
}

/// Holds the configured sources, keyed by id. Facade routers and the `sync`
/// subcommand resolve a source through here.
#[derive(Clone)]
pub struct Registry {
    pools: Pools,
    sources: BTreeMap<String, Arc<dyn Source>>,
}

impl Registry {
    /// Build a registry over the configured pools with the default source set.
    /// Later waves register IMDb/TMDB/TVDB/Wikidata here.
    #[must_use]
    pub fn with_defaults(pools: Pools) -> Self {
        let mb_pool = pools.musicbrainz.clone();
        let imdb_pool = pools.imdb.clone();
        let shirabe_pool = pools.shirabe.clone();
        let mut registry = Self { pools, sources: BTreeMap::new() };
        registry.register(Arc::new(musicbrainz::MusicBrainzSource::new(mb_pool)));
        registry.register(Arc::new(imdb::ImdbSource::new(imdb_pool)));
        registry.register(Arc::new(wikidata::WikidataXrefSource::new(shirabe_pool)));
        registry
    }

    /// Register a source (last write wins on duplicate id).
    pub fn register(&mut self, source: Arc<dyn Source>) {
        self.sources.insert(source.id().to_string(), source);
    }

    /// Look up a source by id.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<&Arc<dyn Source>> {
        self.sources.get(id)
    }

    /// Ids of all registered sources, in stable order.
    #[must_use]
    pub fn ids(&self) -> Vec<&str> {
        self.sources.keys().map(String::as_str).collect()
    }

    /// Run a source's `refresh()` and upsert its `shirabe.source` row. Used by
    /// the `shirabe sync <id>` subcommand. Returns the report, or `None` if no
    /// such source is registered.
    pub async fn run_refresh(&self, id: &str) -> Option<RefreshReport> {
        let source = self.get(id)?.clone();
        let ctx = RefreshCtx { pools: self.pools.clone() };
        let report = source.refresh(&ctx).await;
        match self.pools.shirabe.as_ref() {
            Some(shirabe) => {
                if let Err(e) = upsert_refresh(shirabe, source.as_ref(), &report).await {
                    tracing::error!(source = id, error = %e, "failed to persist refresh status");
                }
            }
            None => {
                tracing::error!(
                    source = id,
                    "SHIRABE_DATABASE_URL is not set; cannot persist refresh status to \
                     the shirabe.source registry"
                );
            }
        }
        Some(report)
    }

    /// Health of every registered source (for the internal registry probe and,
    /// later, `/health/sources`).
    pub async fn health_all(&self) -> Vec<SourceHealth> {
        let mut out = Vec::with_capacity(self.sources.len());
        for source in self.sources.values() {
            out.push(source.health().await);
        }
        out
    }
}

/// Upsert a source's row in `shirabe.source` after a refresh. Runtime query
/// (no compile-time macro); writes only the `shirabe` schema.
async fn upsert_refresh(
    pool: &PgPool,
    source: &dyn Source,
    report: &RefreshReport,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO shirabe.source (name, ingest_mode, last_refresh_at, status, detail)
         VALUES ($1, $2, now(), $3, $4)
         ON CONFLICT (name) DO UPDATE SET
             ingest_mode     = EXCLUDED.ingest_mode,
             last_refresh_at = EXCLUDED.last_refresh_at,
             status          = EXCLUDED.status,
             detail          = EXCLUDED.detail",
    )
    .bind(source.id())
    .bind(source.ingest_mode().as_str())
    .bind(report.status_str())
    .bind(&report.detail)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_mode_round_trips_to_str() {
        for mode in [
            IngestMode::BulkDump,
            IngestMode::EnumerateLazyHydrate,
            IngestMode::LazyScrape,
            IngestMode::ReadOnlyMirror,
        ] {
            assert_eq!(mode.to_string(), mode.as_str());
            assert!(!mode.as_str().is_empty());
        }
    }

    #[test]
    fn refresh_report_status_strings() {
        assert_eq!(RefreshReport::ok("done").status_str(), "ok");
        assert_eq!(RefreshReport::failed("boom").status_str(), "error");
    }
}
