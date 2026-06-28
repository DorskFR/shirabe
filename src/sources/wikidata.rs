//! Wikidata cross-ID source — bulk ingest of the Wikidata entity dump to bridge
//! a title's id across providers (SHIB-8).
//!
//! This is a [`IngestMode::BulkDump`] source, intended to run monthly. `refresh()`
//! streams the gzipped Wikidata JSON dump
//! (`https://dumps.wikimedia.org/wikidatawiki/entity/latest-all.json.gz` — one
//! JSON entity per line, wrapped in a `[`…`]` array), gzip-decodes and parses it
//! **incrementally** line-by-line (the full dump is ~100GB+ gzipped; it must never
//! be loaded whole), reads each entity's `claims` for a fixed set of external-id
//! media properties, and upserts the resulting (wikidata_qid, source, external_id)
//! rows into `shirabe.xref`. Upsert (`ON CONFLICT (source, external_id)`) keeps the
//! refresh idempotent.
//!
//! Writes only the `shirabe` coordination DB (`ctx.pools.shirabe`); errors clearly
//! when `SHIRABE_DATABASE_URL` is unset.

use std::io::BufRead;

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio_util::io::StreamReader;

use super::{IngestMode, RefreshCtx, RefreshReport, Source, SourceHealth};

/// The Wikidata entity dump (one JSON entity per line, framed as a JSON array).
const DUMP_URL: &str = "https://dumps.wikimedia.org/wikidatawiki/entity/latest-all.json.gz";

/// One target Wikidata external-id property → the short `shirabe.xref.source` tag
/// it maps to. The property is an `external-id` datatype claim whose value is the
/// provider's id string.
struct XrefProperty {
    /// Wikidata property id, e.g. `P345`.
    pid: &'static str,
    /// Short source tag stored in `shirabe.xref.source`.
    source: &'static str,
}

/// The media external-id properties we bridge. Order is irrelevant (keyed by pid).
const XREF_PROPERTIES: &[XrefProperty] = &[
    XrefProperty { pid: "P345", source: "imdb" },
    XrefProperty { pid: "P4947", source: "tmdb_movie" },
    XrefProperty { pid: "P4983", source: "tmdb_tv" },
    XrefProperty { pid: "P12196", source: "tvdb" },
    // Legacy TheTVDB series property, predates P12196; same source tag.
    XrefProperty { pid: "P4835", source: "tvdb" },
    XrefProperty { pid: "P434", source: "musicbrainz_artist" },
    XrefProperty { pid: "P435", source: "musicbrainz_work" },
    XrefProperty { pid: "P436", source: "musicbrainz_release_group" },
];

/// Look up the source tag for a Wikidata property id, if it's one we bridge.
fn source_for_pid(pid: &str) -> Option<&'static str> {
    XREF_PROPERTIES.iter().find(|p| p.pid == pid).map(|p| p.source)
}

/// One extracted cross-id row: `(wikidata_qid, source, external_id)`.
type XrefRow = (Option<String>, String, String);

/// Strip the JSON-array framing the dump wraps each line in. The dump is a single
/// JSON array: the first line is `[`, the last is `]`, and every entity line ends
/// with a trailing comma (except the last entity). We return the bare entity JSON,
/// or `None` for framing-only lines (`[`, `]`, blank).
fn strip_array_framing(line: &str) -> Option<&str> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed == "[" || trimmed == "]" {
        return None;
    }
    // Drop a single trailing comma separating array elements.
    Some(trimmed.strip_suffix(',').unwrap_or(trimmed))
}

/// Extract every target cross-id row from one parsed Wikidata entity value.
///
/// Reads `entity.id` as the QID and, for each bridged property in `entity.claims`,
/// pulls the external-id string value from the main snak
/// (`mainsnak.datavalue.value`). Entities with no target claims yield no rows.
fn extract_xrefs(entity: &Value) -> Vec<XrefRow> {
    let mut rows = Vec::new();
    let qid = entity.get("id").and_then(Value::as_str);
    let Some(claims) = entity.get("claims").and_then(Value::as_object) else {
        return rows;
    };
    for (pid, statements) in claims {
        let Some(source) = source_for_pid(pid) else {
            continue;
        };
        let Some(statements) = statements.as_array() else {
            continue;
        };
        for stmt in statements {
            // external-id values are plain strings under mainsnak.datavalue.value.
            if let Some(external_id) = stmt
                .get("mainsnak")
                .and_then(|s| s.get("datavalue"))
                .and_then(|d| d.get("value"))
                .and_then(Value::as_str)
            {
                rows.push((
                    qid.map(ToString::to_string),
                    source.to_string(),
                    external_id.to_string(),
                ));
            }
        }
    }
    rows
}

/// Parse one (array-framing-stripped) dump line into its cross-id rows. Lines that
/// are pure array framing, or fail to parse as a JSON object, yield nothing.
fn xrefs_from_line(line: &str) -> Vec<XrefRow> {
    let Some(entity_json) = strip_array_framing(line) else {
        return Vec::new();
    };
    serde_json::from_str::<Value>(entity_json)
        .map_or_else(|_| Vec::new(), |entity| extract_xrefs(&entity))
}

/// The Wikidata cross-ID bulk-dump source. Holds the optional writable `shirabe`
/// pool so `health()` can report the current xref row count; `None` when
/// `SHIRABE_DATABASE_URL` is unset.
pub struct WikidataXrefSource {
    pool: Option<PgPool>,
}

impl WikidataXrefSource {
    /// Source id / `shirabe.source.name` primary key.
    pub const ID: &'static str = "wikidata";

    #[must_use]
    pub const fn new(pool: Option<PgPool>) -> Self {
        Self { pool }
    }
}

/// Upsert a batch of cross-id rows into `shirabe.xref`. Reusable by the bulk
/// refresh and by per-record hydration (SHIB-6/7 self-links). `wikidata_qid` may be
/// `None` (e.g. a provider self-link not bridged through Wikidata); the column is
/// nullable. Idempotent via `ON CONFLICT (source, external_id)`.
///
/// Runtime query (no compile-time macro); writes only the `shirabe` schema.
pub async fn upsert_xref(pool: &PgPool, rows: &[XrefRow]) -> Result<u64, sqlx::Error> {
    let mut affected = 0u64;
    for (qid, source, external_id) in rows {
        let res = sqlx::query(
            "INSERT INTO shirabe.xref (wikidata_qid, source, external_id)
             VALUES ($1, $2, $3)
             ON CONFLICT (source, external_id) DO UPDATE SET
                 wikidata_qid = COALESCE(EXCLUDED.wikidata_qid, shirabe.xref.wikidata_qid)",
        )
        .bind(qid.as_deref())
        .bind(source)
        .bind(external_id)
        .execute(pool)
        .await?;
        affected += res.rows_affected();
    }
    Ok(affected)
}

/// Stream the dump → gunzip → line-by-line parse → batched upsert into
/// `shirabe.xref`. Never loads the whole dump in memory. Returns the number of
/// xref rows upserted.
async fn ingest_dump(pool: &PgPool, client: &reqwest::Client) -> anyhow::Result<u64> {
    let resp = client.get(DUMP_URL).send().await?.error_for_status()?;
    let stream = resp.bytes_stream().map(|r| r.map_err(std::io::Error::other));
    let async_reader = StreamReader::new(stream);
    let async_buf = tokio::io::BufReader::new(async_reader);
    // Bridge the async byte stream into a blocking gzip + line reader; gzip decode
    // and JSON parsing run on a blocking thread, sending extracted xref batches
    // back over a channel to the async upsert side. Memory stays bounded — one
    // dump line at a time, never the whole ~100GB+ dump.
    let bridge = tokio_util::io::SyncIoBridge::new(async_buf);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<XrefRow>>(16);
    let parse_handle = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let gz = flate2::read::MultiGzDecoder::new(bridge);
        let reader = std::io::BufReader::new(gz);
        let mut batch: Vec<XrefRow> = Vec::with_capacity(BATCH_ROWS);
        for line in reader.lines() {
            let line = line?;
            batch.extend(xrefs_from_line(&line));
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
        total += upsert_xref(pool, &batch).await?;
    }
    parse_handle.await??;
    Ok(total)
}

/// Flush the upsert buffer once this many rows accumulate.
const BATCH_ROWS: usize = 1024;

#[async_trait]
impl Source for WikidataXrefSource {
    fn id(&self) -> &str {
        Self::ID
    }

    fn ingest_mode(&self) -> IngestMode {
        IngestMode::BulkDump
    }

    async fn refresh(&self, ctx: &RefreshCtx) -> RefreshReport {
        let Some(pool) = ctx.pools.shirabe.as_ref() else {
            return RefreshReport::failed(
                "SHIRABE_DATABASE_URL is not set; the wikidata source requires the writable \
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

        match ingest_dump(pool, &client).await {
            Ok(rows) => RefreshReport::ok(format!("ingested {rows} wikidata xref rows"))
                .with_detail(json!({
                    "source": DUMP_URL,
                    "rows": rows,
                    "properties": XREF_PROPERTIES.iter().map(|p| p.pid).collect::<Vec<_>>(),
                })),
            Err(e) => RefreshReport::failed(format!("wikidata xref ingest failed: {e}")),
        }
    }

    async fn health(&self) -> SourceHealth {
        let Some(pool) = self.pool.as_ref() else {
            return SourceHealth {
                source: self.id().to_string(),
                reachable: false,
                detail: "SHIRABE_DATABASE_URL is not set; wikidata xref store unavailable"
                    .to_string(),
            };
        };
        match sqlx::query_scalar::<_, i64>("SELECT count(*) FROM shirabe.xref")
            .fetch_one(pool)
            .await
        {
            Ok(n) => SourceHealth {
                source: self.id().to_string(),
                reachable: true,
                detail: format!("shirabe.xref reachable; {n} cross-id rows"),
            },
            Err(e) => SourceHealth {
                source: self.id().to_string(),
                reachable: false,
                detail: format!("shirabe.xref unreachable: {e}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A Wikidata entity carrying P345 (IMDb) and P4947 (TMDB movie) external-id
    /// claims extracts one row per claim with the right source tag and value, all
    /// stamped with the entity's QID.
    #[test]
    fn extracts_imdb_and_tmdb_claims() {
        let line = r#"{"type":"item","id":"Q42","claims":{
            "P345":[{"mainsnak":{"snaktype":"value","property":"P345",
                "datavalue":{"value":"tt0042876","type":"string"},
                "datatype":"external-id"}}],
            "P4947":[{"mainsnak":{"snaktype":"value","property":"P4947",
                "datavalue":{"value":"603","type":"string"},
                "datatype":"external-id"}}],
            "P31":[{"mainsnak":{"snaktype":"value","property":"P31",
                "datavalue":{"value":{"id":"Q5"},"type":"wikibase-entityid"}}}]
        }},"#;
        let mut rows = xrefs_from_line(line);
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (Some("Q42".to_string()), "imdb".to_string(), "tt0042876".to_string()),
                (Some("Q42".to_string()), "tmdb_movie".to_string(), "603".to_string()),
            ]
        );
    }

    /// Multiple statements for one property (a title with two MusicBrainz artist
    /// ids) each become a row.
    #[test]
    fn extracts_multiple_statements_for_one_property() {
        let line = r#"{"id":"Q1","claims":{"P434":[
            {"mainsnak":{"datavalue":{"value":"abc","type":"string"},"datatype":"external-id"}},
            {"mainsnak":{"datavalue":{"value":"def","type":"string"},"datatype":"external-id"}}
        ]}}"#;
        let mut rows = xrefs_from_line(line);
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (Some("Q1".to_string()), "musicbrainz_artist".to_string(), "abc".to_string()),
                (Some("Q1".to_string()), "musicbrainz_artist".to_string(), "def".to_string()),
            ]
        );
    }

    /// An entity with no target claims (only an untracked property) yields nothing.
    #[test]
    fn entity_without_target_claims_yields_nothing() {
        let line = r#"{"id":"Q2","claims":{"P31":[{"mainsnak":{
            "datavalue":{"value":{"id":"Q5"},"type":"wikibase-entityid"}}}]}},"#;
        assert!(xrefs_from_line(line).is_empty());
    }

    /// Array-framing lines (`[`, `]`) and blanks parse to nothing.
    #[test]
    fn array_framing_lines_yield_nothing() {
        assert!(xrefs_from_line("[").is_empty());
        assert!(xrefs_from_line("]").is_empty());
        assert!(xrefs_from_line("").is_empty());
        assert!(strip_array_framing("  [  ").is_none());
    }

    /// The legacy TheTVDB property P4835 maps to the same `tvdb` tag as P12196.
    #[test]
    fn legacy_tvdb_property_maps_to_tvdb() {
        assert_eq!(source_for_pid("P4835"), Some("tvdb"));
        assert_eq!(source_for_pid("P12196"), Some("tvdb"));
        assert_eq!(source_for_pid("P9999"), None);
    }
}
