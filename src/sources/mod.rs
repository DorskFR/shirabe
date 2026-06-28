//! Extensible source / plugin model (SHIB-3).
//!
//! Every ingest provider — MusicBrainz, IMDb, TMDB, TVDB — is a
//! uniform unit of work behind the [`Source`] trait. The [`Registry`] holds the
//! configured sources; facade routers stay thin and call into sources. Each
//! source owns one row in the writable `shirabe.source` table (name PK,
//! ingest_mode, last_refresh_at, status, detail jsonb); [`refresh`](Source::refresh)
//! and [`health`](Source::health) upsert that row so `/health/sources` (SHIB-12)
//! and the `shirabe sync <source>` CronJob can report freshness.
//!
//! Adding a provider later (IMDb `BulkDump`, TMDB `EnumerateLazyHydrate`, TVDB
//! `LazyScrape`) is just: implement `Source`, register it.

pub mod imdb;
pub mod musicbrainz;
pub mod tmdb;
pub mod tvdb;
pub mod xref;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::config::Config;
use crate::db::Pools;
use crate::sources::tvdb::TokenStore;

/// How a source gets its data into Shirabe. Drives whether `shirabe sync`
/// pulls a full dump, enumerates + lazily hydrates, scrapes on demand, or just
/// mirrors a read-only upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestMode {
    /// Periodic full dump ingest (IMDb TSV).
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
    /// `config` + `tvdb_tokens` are threaded into the TheTVDB lazy-scrape source
    /// (server-side key/PIN + the shared in-memory bearer token, which the `/v4`
    /// facade reuses via `AppState`).
    // `tmdb_pool`/`tvdb_pool` differ by one char (fixed provider names).
    #[allow(clippy::similar_names)]
    #[must_use]
    pub fn with_defaults(pools: Pools, config: Config, tvdb_tokens: TokenStore) -> Self {
        let mb_pool = pools.musicbrainz.clone();
        let imdb_pool = pools.imdb.clone();
        let tmdb_pool = pools.tmdb.clone();
        let tvdb_pool = pools.tvdb.clone();
        let mut registry = Self { pools, sources: BTreeMap::new() };
        registry.register(Arc::new(musicbrainz::MusicBrainzSource::new(mb_pool)));
        registry.register(Arc::new(imdb::ImdbSource::new(imdb_pool)));
        registry.register(Arc::new(tmdb::TmdbSource::new(tmdb_pool)));
        registry.register(Arc::new(tvdb::TvdbSource::new(tvdb_pool, tvdb_tokens, config)));
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

    /// Gather the `/health/sources` report: for every registered source, merge the
    /// live [`Source::health`] probe with the persisted `shirabe.source` row
    /// (last_refresh_at/status/detail written by `shirabe sync` CronJob runs).
    ///
    /// Degrades gracefully when the writable `shirabe` pool is absent (the API pod
    /// may boot with only the read-only mirror): the persisted fields are left
    /// `None` and the report reflects only what live `health()` can determine.
    pub async fn health_report(&self) -> SourcesHealthReport {
        // Read the persisted registry rows once (best-effort); key by source name.
        let persisted = match self.pools.shirabe.as_ref() {
            Some(shirabe) => match fetch_source_rows(shirabe).await {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to read shirabe.source for /health/sources");
                    BTreeMap::new()
                }
            },
            None => BTreeMap::new(),
        };

        let mut sources = Vec::with_capacity(self.sources.len());
        for source in self.sources.values() {
            let live = source.health().await;
            let row = persisted.get(source.id());
            sources.push(merge_source_health(source.as_ref(), &live, row));
        }
        SourcesHealthReport { sources }
    }
}

/// A persisted `shirabe.source` row, as read back for `/health/sources`.
#[derive(Debug, Clone)]
struct SourceRow {
    ingest_mode: String,
    last_refresh_at: Option<String>,
    status: Option<String>,
    detail: Option<Value>,
}

/// One source's entry in the `/health/sources` response: the persisted registry
/// state merged with the live `health()` probe, plus a single `healthy` rollup
/// so a human/monitor can see at a glance which source is stale or errored.
// `PartialEq` is for the unit tests; `Value` precludes `Eq`, hence the allow.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SourceStatus {
    /// Source id / `shirabe.source.name`.
    pub id: String,
    /// How this source ingests data (`bulk_dump`, `lazy_scrape`, …).
    pub ingest_mode: String,
    /// Persisted status of the last `sync` run (`ok`/`error`), or `null` if the
    /// source has never run a CronJob `sync` (no `shirabe.source` row).
    pub status: Option<String>,
    /// Wall-clock time of the last persisted refresh, RFC3339, or `null`.
    pub last_refresh_at: Option<String>,
    /// Structured detail from the last persisted refresh (row counts, error
    /// summary, token expiry, …), or `null`.
    pub detail: Option<Value>,
    /// Whether the source's backing store/upstream is reachable right now.
    pub reachable: bool,
    /// One-line live `health()` detail (row/cache counts, token validity, …).
    pub live_detail: String,
    /// Single rollup: `true` only when the live probe is reachable AND the last
    /// persisted `sync` did not record an error. A stale or errored source is
    /// immediately visible as `healthy: false`.
    pub healthy: bool,
}

/// The `/health/sources` response: one [`SourceStatus`] per registered source.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SourcesHealthReport {
    pub sources: Vec<SourceStatus>,
}

/// Merge a source's live [`SourceHealth`] with its (optional) persisted
/// `shirabe.source` row into the response model. Pure — unit-tested without a DB.
fn merge_source_health(
    source: &dyn Source,
    live: &SourceHealth,
    row: Option<&SourceRow>,
) -> SourceStatus {
    let status = row.and_then(|r| r.status.clone());
    // Unhealthy if the live probe can't reach the backing store, or the last
    // persisted sync recorded an error status.
    let persisted_errored = status.as_deref() == Some("error");
    SourceStatus {
        id: source.id().to_string(),
        ingest_mode: row
            .map_or_else(|| source.ingest_mode().as_str().to_string(), |r| r.ingest_mode.clone()),
        status,
        last_refresh_at: row.and_then(|r| r.last_refresh_at.clone()),
        detail: row.and_then(|r| r.detail.clone()),
        reachable: live.reachable,
        live_detail: live.detail.clone(),
        healthy: live.reachable && !persisted_errored,
    }
}

/// Read all `shirabe.source` rows for the health report, keyed by source name.
/// Runtime query; reads only the `shirabe` schema. `last_refresh_at` is rendered
/// to RFC3339 text in SQL to avoid pulling in a timestamp type/feature.
async fn fetch_source_rows(pool: &PgPool) -> Result<BTreeMap<String, SourceRow>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT name,
                ingest_mode,
                to_char(last_refresh_at, 'YYYY-MM-DD\"T\"HH24:MI:SSOF') AS last_refresh_at,
                status,
                detail
           FROM shirabe.source",
    )
    .fetch_all(pool)
    .await?;
    let mut map = BTreeMap::new();
    for r in rows {
        let name: String = r.get("name");
        map.insert(
            name,
            SourceRow {
                ingest_mode: r.get("ingest_mode"),
                last_refresh_at: r.get("last_refresh_at"),
                status: r.get("status"),
                detail: r.get("detail"),
            },
        );
    }
    Ok(map)
}

/// Upsert a source's row in `shirabe.source` after a refresh. Runtime query
/// (no compile-time macro); writes only the `shirabe` schema.
async fn upsert_refresh(
    pool: &PgPool,
    source: &dyn Source,
    report: &RefreshReport,
) -> Result<(), sqlx::Error> {
    // Fold the one-line summary into the persisted detail so a failure's error
    // message is queryable in-app (via `/health/sources`), not only in the k8s
    // job log: a failed refresh often carries `detail: Null` and only the summary
    // describes what broke.
    let detail = persisted_detail(report);
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
    .bind(&detail)
    .execute(pool)
    .await?;
    Ok(())
}

/// Build the jsonb `detail` to persist for a refresh: always carry the one-line
/// `summary` (so a failure's cause is queryable), merging it into the report's
/// structured detail when present. Pure — unit-tested.
fn persisted_detail(report: &RefreshReport) -> Value {
    match &report.detail {
        Value::Object(map) => {
            let mut map = map.clone();
            map.entry("summary".to_string())
                .or_insert_with(|| Value::String(report.summary.clone()));
            Value::Object(map)
        }
        Value::Null => serde_json::json!({ "summary": report.summary }),
        // A non-object, non-null detail (rare): wrap it alongside the summary.
        other => serde_json::json!({ "summary": report.summary, "detail": other }),
    }
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

    /// A failed refresh with no structured detail still persists its summary, so
    /// the error is queryable via `/health/sources` rather than lost.
    #[test]
    fn persisted_detail_carries_summary_for_null_detail() {
        let report = RefreshReport::failed("mirror unreachable: timed out");
        assert_eq!(
            persisted_detail(&report),
            serde_json::json!({ "summary": "mirror unreachable: timed out" })
        );
    }

    /// A structured detail is preserved and the summary is merged in (without
    /// clobbering an explicit `summary` key if the source already set one).
    #[test]
    fn persisted_detail_merges_summary_into_object() {
        let report =
            RefreshReport::ok("ingested 5 rows").with_detail(serde_json::json!({ "rows": 5 }));
        assert_eq!(
            persisted_detail(&report),
            serde_json::json!({ "rows": 5, "summary": "ingested 5 rows" })
        );
    }

    /// A minimal `Source` used to exercise the pure merge without a live DB.
    struct FakeSource;

    #[async_trait]
    impl Source for FakeSource {
        #[allow(clippy::unnecessary_literal_bound)]
        fn id(&self) -> &str {
            "fake"
        }
        fn ingest_mode(&self) -> IngestMode {
            IngestMode::BulkDump
        }
        async fn refresh(&self, _ctx: &RefreshCtx) -> RefreshReport {
            RefreshReport::ok("noop")
        }
        async fn health(&self) -> SourceHealth {
            SourceHealth {
                source: "fake".to_string(),
                reachable: true,
                detail: "fake reachable; 3 rows".to_string(),
            }
        }
    }

    /// Merge with a persisted ok row + a reachable live probe → healthy, and the
    /// persisted fields (last_refresh_at/status/detail) flow through.
    #[test]
    fn merge_ok_row_with_reachable_probe_is_healthy() {
        let live = SourceHealth {
            source: "fake".to_string(),
            reachable: true,
            detail: "fake reachable; 3 rows".to_string(),
        };
        let row = SourceRow {
            ingest_mode: "bulk_dump".to_string(),
            last_refresh_at: Some("2026-06-29T07:00:00+00".to_string()),
            status: Some("ok".to_string()),
            detail: Some(serde_json::json!({ "rows": 3 })),
        };
        let status = merge_source_health(&FakeSource, &live, Some(&row));
        assert_eq!(
            status,
            SourceStatus {
                id: "fake".to_string(),
                ingest_mode: "bulk_dump".to_string(),
                status: Some("ok".to_string()),
                last_refresh_at: Some("2026-06-29T07:00:00+00".to_string()),
                detail: Some(serde_json::json!({ "rows": 3 })),
                reachable: true,
                live_detail: "fake reachable; 3 rows".to_string(),
                healthy: true,
            }
        );
    }

    /// A persisted `error` status makes the source unhealthy even if the live
    /// probe is reachable — a failed CronJob is visible in-app.
    #[test]
    fn merge_errored_row_is_unhealthy() {
        let live = SourceHealth {
            source: "fake".to_string(),
            reachable: true,
            detail: "fake reachable".to_string(),
        };
        let row = SourceRow {
            ingest_mode: "bulk_dump".to_string(),
            last_refresh_at: Some("2026-06-01T00:00:00+00".to_string()),
            status: Some("error".to_string()),
            detail: Some(serde_json::json!({ "summary": "ingest failed: 500" })),
        };
        let status = merge_source_health(&FakeSource, &live, Some(&row));
        assert!(!status.healthy);
        assert_eq!(status.status.as_deref(), Some("error"));
    }

    /// With no persisted row (source never synced, or shirabe pool absent), the
    /// persisted fields are null, ingest_mode falls back to the source's own, and
    /// `healthy` reflects only live reachability.
    #[test]
    fn merge_without_row_degrades_to_live_probe() {
        let live = SourceHealth {
            source: "fake".to_string(),
            reachable: false,
            detail: "unreachable".to_string(),
        };
        let status = merge_source_health(&FakeSource, &live, None);
        assert_eq!(status.ingest_mode, "bulk_dump");
        assert_eq!(status.status, None);
        assert_eq!(status.last_refresh_at, None);
        assert_eq!(status.detail, None);
        assert!(!status.healthy);
    }

    /// The response model serializes to the documented `{ "sources": [ … ] }`
    /// shape with the expected keys.
    #[test]
    fn report_serializes_to_sources_array() {
        let report = SourcesHealthReport {
            sources: vec![SourceStatus {
                id: "fake".to_string(),
                ingest_mode: "bulk_dump".to_string(),
                status: Some("ok".to_string()),
                last_refresh_at: Some("2026-06-29T07:00:00+00".to_string()),
                detail: None,
                reachable: true,
                live_detail: "ok".to_string(),
                healthy: true,
            }],
        };
        let v = serde_json::to_value(&report).expect("serializes");
        assert_eq!(v["sources"][0]["id"], "fake");
        assert_eq!(v["sources"][0]["ingest_mode"], "bulk_dump");
        assert_eq!(v["sources"][0]["status"], "ok");
        assert_eq!(v["sources"][0]["healthy"], true);
        assert_eq!(v["sources"][0]["detail"], serde_json::Value::Null);
    }
}
