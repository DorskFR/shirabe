//! fanart.tv source — health-only registry entry.
//!
//! fanart.tv is served by the `/v3` facade as a lazy, per-request fetch + cache
//! (`fanart_cache` in the dedicated `fanart` DB). There is no bulk ingest and no
//! sync DB of its own; this source exists so `/health/sources` reports whether
//! `FANART_API_KEY` is configured and the cache DB is reachable.

use async_trait::async_trait;
use serde_json::json;
use sqlx::PgPool;

use super::{IngestMode, RefreshCtx, RefreshReport, Source, SourceHealth};
use crate::config::Config;

pub struct FanartSource {
    pool: Option<PgPool>,
    config: Config,
}

impl FanartSource {
    /// Source id / `shirabe.source.name` primary key.
    pub const ID: &'static str = "fanart";

    #[must_use]
    pub const fn new(pool: Option<PgPool>, config: Config) -> Self {
        Self { pool, config }
    }

    const fn configured(&self) -> bool {
        self.config.fanart_api_key.is_some()
    }
}

#[async_trait]
impl Source for FanartSource {
    fn id(&self) -> &str {
        Self::ID
    }

    fn ingest_mode(&self) -> IngestMode {
        IngestMode::LazyScrape
    }

    async fn refresh(&self, _ctx: &RefreshCtx) -> RefreshReport {
        let configured = self.configured();
        let summary = if configured {
            "fanart is a lazy-scrape source; no bulk ingest (payloads fetched + cached on demand)"
        } else {
            "fanart is a lazy-scrape source; no bulk ingest. FANART_API_KEY not set — /v3 \
             serves only cached rows until a key is configured"
        };
        RefreshReport::ok(summary).with_detail(json!({
            "ingest": "lazy_scrape",
            "configured": configured,
            "attribution": "Artwork provided by fanart.tv (https://fanart.tv).",
        }))
    }

    async fn health(&self) -> SourceHealth {
        if !self.configured() {
            return SourceHealth {
                source: self.id().to_string(),
                reachable: false,
                detail: "FANART_API_KEY is not set; upstream fetches unavailable (cached rows \
                         still served)"
                    .to_string(),
            };
        }
        let Some(pool) = self.pool.as_ref() else {
            return SourceHealth {
                source: self.id().to_string(),
                reachable: true,
                detail: "fanart key configured; FANART_DATABASE_URL not set (responses uncached)"
                    .to_string(),
            };
        };
        match sqlx::query_scalar::<_, i64>("SELECT count(*) FROM fanart_cache")
            .fetch_one(pool)
            .await
        {
            Ok(n) => SourceHealth {
                source: self.id().to_string(),
                reachable: true,
                detail: format!("fanart_cache reachable; {n} cached rows; key configured"),
            },
            Err(e) => SourceHealth {
                source: self.id().to_string(),
                reachable: false,
                detail: format!("fanart_cache unreachable: {e}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::config::Cli;

    fn config(fanart_api_key: Option<&str>) -> Config {
        let cli = Cli::try_parse_from(["shirabe", "--database-url", "postgres://x/x"]).unwrap();
        let mut config = cli.config;
        config.fanart_api_key = fanart_api_key.map(str::to_string);
        config
    }

    /// A missing FANART_API_KEY is visible in /health/sources: the source reports
    /// unreachable with the key named in the detail.
    #[tokio::test]
    async fn missing_key_is_unreachable() {
        let health = FanartSource::new(None, config(None)).health().await;
        assert_eq!(health.source, "fanart");
        assert!(!health.reachable);
        assert!(health.detail.contains("FANART_API_KEY"));
    }

    /// With a key configured the source is reachable even without a cache pool
    /// (the facade then serves uncached upstream fetches).
    #[tokio::test]
    async fn configured_key_without_cache_pool_is_reachable() {
        let health = FanartSource::new(None, config(Some("k"))).health().await;
        assert!(health.reachable);
        assert!(health.detail.contains("FANART_DATABASE_URL"));
    }

    /// `refresh()` never ingests; it records the configuration state (ok either
    /// way — an operator without a key is not an error).
    #[tokio::test]
    async fn refresh_reports_configuration() {
        let ctx = RefreshCtx {
            pools: crate::db::Pools {
                musicbrainz: crate::db::connect_lazy("postgres://x/x", 1).unwrap(),
                shirabe: None,
                imdb: None,
                tmdb: None,
                tvdb: None,
                fanart: None,
            },
        };
        let report = FanartSource::new(None, config(None)).refresh(&ctx).await;
        assert!(report.ok);
        assert_eq!(report.detail["configured"], false);

        let report = FanartSource::new(None, config(Some("k"))).refresh(&ctx).await;
        assert!(report.ok);
        assert_eq!(report.detail["configured"], true);
    }
}
