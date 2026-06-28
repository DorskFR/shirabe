//! TMDB source — daily ID-export enumeration (SHIB-6).
//!
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │ ATTRIBUTION — TMDB                                                          │
//! │                                                                            │
//! │ This product uses the TMDB API but is not endorsed or certified by TMDB.   │
//! │ Data and images are courtesy of The Movie Database (https://themoviedb.org)│
//! └──────────────────────────────────────────────────────────────────────────┘
//!
//! TMDB publishes no full data dump. Instead it exports, once per day (~07:00
//! UTC), newline-delimited JSON gzip files listing every valid id plus a name and
//! a popularity score, under <https://files.tmdb.org/p/exports/>:
//!
//! - `movie_ids_MM_DD_YYYY.json.gz`
//! - `tv_series_ids_MM_DD_YYYY.json.gz`
//!
//! (Files are deleted after ~3 months, so we always fetch *today's* UTC date.)
//!
//! This is an [`IngestMode::EnumerateLazyHydrate`] source: `refresh()` streams
//! those gz exports (same streaming gunzip pattern as the IMDb source) into
//! `shirabe.tmdb_id_index` (id, kind movie/tv, name, popularity, adult). The
//! id index tells us *what exists* and carries popularity for ranking ties; the
//! hydrated per-title detail is fetched on demand by the `/3` facade and cached in
//! `shirabe.tmdb_cache`.
//!
//! Writes only the `shirabe` coordination DB (`ctx.pools.shirabe`); errors clearly
//! when `SHIRABE_DATABASE_URL` is unset.

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use std::io::BufRead;
use tokio_util::io::StreamReader;

use super::{IngestMode, RefreshCtx, RefreshReport, Source, SourceHealth};

/// Base URL for the daily TMDB id exports.
const EXPORTS_BASE: &str = "https://files.tmdb.org/p/exports";

/// One TMDB id-export file → the `kind` it populates in `shirabe.tmdb_id_index`.
struct Export {
    /// Filename prefix, e.g. `movie_ids` or `tv_series_ids`.
    prefix: &'static str,
    /// `kind` stored in the id index (`movie` / `tv`).
    kind: &'static str,
}

/// The two exports we enumerate.
const EXPORTS: &[Export] = &[
    Export { prefix: "movie_ids", kind: "movie" },
    Export { prefix: "tv_series_ids", kind: "tv" },
];

/// One row of a TMDB id export line. `original_title` (movies) / `original_name`
/// (tv) carries the display name; `popularity` ranks ties; `adult` is the flag.
#[derive(Debug, Clone, Deserialize, PartialEq)]
struct ExportRow {
    id: i64,
    #[serde(default, alias = "original_name")]
    original_title: Option<String>,
    #[serde(default)]
    popularity: Option<f64>,
    #[serde(default)]
    adult: Option<bool>,
}

/// Format the export filename for a given UTC date: `<prefix>_MM_DD_YYYY.json.gz`.
fn export_filename(prefix: &str, year: i64, month: u32, day: u32) -> String {
    format!("{prefix}_{month:02}_{day:02}_{year:04}.json.gz")
}

/// (year, month, day) in UTC for a Unix timestamp (seconds), via the standard
/// civil-from-days algorithm (Howard Hinnant). Avoids pulling in a date crate.
const fn civil_from_unix(secs: i64) -> (i64, u32, u32) {
    let days = secs.div_euclid(86_400);
    // Shift epoch to 0000-03-01 so leap days fall at the end of the era.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

/// Today's UTC date as (year, month, day).
fn utc_today() -> (i64, u32, u32) {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    #[allow(clippy::cast_possible_wrap)] // seconds since 1970 fit i64 for ~292By
    civil_from_unix(secs as i64)
}

/// Parse one newline-delimited JSON export line into an [`ExportRow`]. Blank lines
/// and malformed JSON yield `None` (skip rather than abort the stream).
fn parse_export_line(line: &str) -> Option<ExportRow> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    serde_json::from_str::<ExportRow>(trimmed).ok()
}

/// The TMDB enumeration source. Holds the optional writable `shirabe` pool so
/// `health()` can report the id-index row count; `None` when `SHIRABE_DATABASE_URL`
/// is unset (the API pod still boots and the source registers, but ingest and the
/// count are unavailable).
pub struct TmdbSource {
    pool: Option<PgPool>,
}

impl TmdbSource {
    /// Source id / `shirabe.source.name` primary key.
    pub const ID: &'static str = "tmdb";

    #[must_use]
    pub const fn new(pool: Option<PgPool>) -> Self {
        Self { pool }
    }
}

/// Flush the id-index upsert buffer once this many rows accumulate.
const BATCH_ROWS: usize = 1024;

/// Upsert a batch of id-index rows into `shirabe.tmdb_id_index`. Idempotent via
/// `ON CONFLICT (id, kind)`. Runtime query; writes only the `shirabe` schema.
async fn upsert_id_index(
    pool: &PgPool,
    kind: &str,
    rows: &[ExportRow],
) -> Result<u64, sqlx::Error> {
    let mut affected = 0u64;
    for row in rows {
        let res = sqlx::query(
            "INSERT INTO shirabe.tmdb_id_index (id, kind, name, popularity, adult)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (id, kind) DO UPDATE SET
                 name       = EXCLUDED.name,
                 popularity = EXCLUDED.popularity,
                 adult      = EXCLUDED.adult",
        )
        .bind(row.id)
        .bind(kind)
        .bind(row.original_title.as_deref())
        .bind(row.popularity.map(|p| p as f32))
        .bind(row.adult)
        .execute(pool)
        .await?;
        affected += res.rows_affected();
    }
    Ok(affected)
}

/// Stream one export gz → gunzip → line-by-line parse → batched upsert. Never
/// loads the whole export in memory. Returns the rows upserted.
async fn ingest_export(
    pool: &PgPool,
    client: &reqwest::Client,
    export: &Export,
    filename: &str,
) -> anyhow::Result<u64> {
    let url = format!("{EXPORTS_BASE}/{filename}");
    let resp = client.get(&url).send().await?.error_for_status()?;
    let stream = resp.bytes_stream().map(|r| r.map_err(std::io::Error::other));
    let async_reader = StreamReader::new(stream);
    let async_buf = tokio::io::BufReader::new(async_reader);
    let bridge = tokio_util::io::SyncIoBridge::new(async_buf);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<ExportRow>>(16);
    let parse_handle = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let gz = flate2::read::MultiGzDecoder::new(bridge);
        let reader = std::io::BufReader::new(gz);
        let mut batch: Vec<ExportRow> = Vec::with_capacity(BATCH_ROWS);
        for line in reader.lines() {
            let line = line?;
            if let Some(row) = parse_export_line(&line) {
                batch.push(row);
            }
            if batch.len() >= BATCH_ROWS && tx.blocking_send(std::mem::take(&mut batch)).is_err() {
                return Ok(());
            }
        }
        if !batch.is_empty() {
            let _ = tx.blocking_send(batch);
        }
        Ok(())
    });

    let mut total: u64 = 0;
    while let Some(batch) = rx.recv().await {
        total += upsert_id_index(pool, export.kind, &batch).await?;
    }
    parse_handle.await??;
    Ok(total)
}

#[async_trait]
impl Source for TmdbSource {
    fn id(&self) -> &str {
        Self::ID
    }

    fn ingest_mode(&self) -> IngestMode {
        IngestMode::EnumerateLazyHydrate
    }

    async fn refresh(&self, ctx: &RefreshCtx) -> RefreshReport {
        let Some(pool) = ctx.pools.shirabe.as_ref() else {
            return RefreshReport::failed(
                "SHIRABE_DATABASE_URL is not set; the tmdb source requires the writable \
                 shirabe coordination database",
            );
        };
        let client = match reqwest::Client::builder()
            .user_agent(concat!("shirabe/", env!("CARGO_PKG_VERSION")))
            .build()
        {
            Ok(c) => c,
            Err(e) => return RefreshReport::failed(format!("failed to build HTTP client: {e}")),
        };

        let (year, month, day) = utc_today();
        let mut counts = serde_json::Map::new();
        for export in EXPORTS {
            let filename = export_filename(export.prefix, year, month, day);
            match ingest_export(pool, &client, export, &filename).await {
                Ok(rows) => {
                    counts.insert(export.kind.to_string(), json!(rows));
                }
                Err(e) => {
                    return RefreshReport::failed(format!(
                        "tmdb id-export ingest of {filename} failed: {e}"
                    ))
                    .with_detail(json!({ "loaded": counts, "failed": filename }));
                }
            }
        }

        let total: u64 = counts.values().filter_map(serde_json::Value::as_u64).sum();
        RefreshReport::ok(format!("enumerated {total} tmdb ids across movie + tv exports"))
            .with_detail(json!({
                "attribution": "This product uses the TMDB API but is not endorsed or \
                                certified by TMDB.",
                "source": EXPORTS_BASE,
                "date": format!("{year:04}-{month:02}-{day:02}"),
                "rows": counts,
            }))
    }

    async fn health(&self) -> SourceHealth {
        let Some(pool) = self.pool.as_ref() else {
            return SourceHealth {
                source: self.id().to_string(),
                reachable: false,
                detail: "SHIRABE_DATABASE_URL is not set; tmdb id index unavailable".to_string(),
            };
        };
        match sqlx::query_scalar::<_, i64>("SELECT count(*) FROM shirabe.tmdb_id_index")
            .fetch_one(pool)
            .await
        {
            Ok(n) => SourceHealth {
                source: self.id().to_string(),
                reachable: true,
                detail: format!("shirabe.tmdb_id_index reachable; {n} enumerated ids"),
            },
            Err(e) => SourceHealth {
                source: self.id().to_string(),
                reachable: false,
                detail: format!("shirabe.tmdb_id_index unreachable: {e}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A movie id-export line parses into the right id/name/popularity/adult.
    #[test]
    fn parses_movie_export_line() {
        let line = r#"{"adult":false,"id":603,"original_title":"The Matrix","popularity":52.4,"video":false}"#;
        let row = parse_export_line(line).expect("parses");
        assert_eq!(
            row,
            ExportRow {
                id: 603,
                original_title: Some("The Matrix".to_string()),
                popularity: Some(52.4),
                adult: Some(false),
            }
        );
    }

    /// A tv id-export line uses `original_name`; the alias maps it to the same field.
    #[test]
    fn parses_tv_export_line_via_original_name_alias() {
        let line = r#"{"id":1396,"original_name":"Breaking Bad","popularity":201.6}"#;
        let row = parse_export_line(line).expect("parses");
        assert_eq!(row.id, 1396);
        assert_eq!(row.original_title.as_deref(), Some("Breaking Bad"));
        assert_eq!(row.popularity, Some(201.6));
        assert_eq!(row.adult, None);
    }

    /// Blank lines and malformed JSON are skipped, not fatal.
    #[test]
    fn skips_blank_and_malformed_lines() {
        assert!(parse_export_line("").is_none());
        assert!(parse_export_line("   ").is_none());
        assert!(parse_export_line("{not json").is_none());
    }

    /// The export filename is `<prefix>_MM_DD_YYYY.json.gz` with zero-padded MM/DD.
    #[test]
    fn formats_export_filename() {
        assert_eq!(export_filename("movie_ids", 2026, 6, 29), "movie_ids_06_29_2026.json.gz");
        assert_eq!(
            export_filename("tv_series_ids", 2026, 12, 1),
            "tv_series_ids_12_01_2026.json.gz"
        );
    }

    /// The civil-from-Unix conversion matches known UTC dates.
    #[test]
    fn civil_from_unix_known_dates() {
        assert_eq!(civil_from_unix(0), (1970, 1, 1));
        // 2026-06-29T00:00:00Z = 1782691200
        assert_eq!(civil_from_unix(1_782_691_200), (2026, 6, 29));
        // A leap day: 2024-02-29T12:00:00Z = 1709208000
        assert_eq!(civil_from_unix(1_709_208_000), (2024, 2, 29));
    }
}
