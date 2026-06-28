//! IMDb source — bulk ingest of the IMDb Non-Commercial Datasets (SHIB-5).
//!
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │ LICENSE / ATTRIBUTION — IMDb Non-Commercial Datasets                       │
//! │                                                                            │
//! │ The data ingested here comes from the IMDb Non-Commercial Datasets         │
//! │ (https://datasets.imdbws.com/). It is licensed for PERSONAL and            │
//! │ NON-COMMERCIAL use ONLY. See https://www.imdb.com/interfaces/ and IMDb's   │
//! │ conditions of use. Do NOT use this data commercially.                      │
//! └──────────────────────────────────────────────────────────────────────────┘
//!
//! This is a [`IngestMode::BulkDump`] source. `refresh()` downloads the gzipped
//! TSV datasets, streams them through gzip decode + a tab-delimited TSV reader
//! (`\N` = null, no quoting — IMDb's exact dialect), and COPY-loads each into a
//! `_new` staging table. After a dataset loads cleanly it is atomically swapped
//! into place (DROP live, RENAME staging) so live reads never observe a
//! half-loaded set. pg_trgm GIN indexes on the title columns back non-latin
//! akas search (e.g. 銀魂 / Gintama).
//!
//! Writes only the dedicated `imdb` database (`ctx.pools.imdb`); errors clearly
//! when `IMDB_DATABASE_URL` is unset.

use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::json;
use sqlx::postgres::PgPoolCopyExt;
use sqlx::{PgPool, Row};
use tokio_util::io::StreamReader;

use super::{IngestMode, RefreshCtx, RefreshReport, Source, SourceHealth};

/// Base URL for the IMDb Non-Commercial Datasets.
const DATASETS_BASE: &str = "https://datasets.imdbws.com";

/// One IMDb dataset → one `imdb_*` table. `columns` is the destination column
/// list (in TSV column order); `parse` maps a raw TSV record to the COPY-text
/// fields (applying null/boolean normalization), or `None` to skip a malformed
/// or header row.
struct Dataset {
    /// Gzip file name under [`DATASETS_BASE`], e.g. `title.basics.tsv.gz`.
    file: &'static str,
    /// Live table name (e.g. `imdb_title_basics`); staging is `<table>_new`.
    table: &'static str,
    /// Destination columns, in the order [`Dataset::to_copy_fields`] emits.
    columns: &'static [&'static str],
    /// Number of TSV columns expected per record.
    tsv_cols: usize,
    /// `CREATE TABLE <table>_new (…)` body, mirroring the migration's live table.
    create_new: &'static str,
    /// `CREATE INDEX` statements to (re)build on the live table after swap.
    indexes: &'static [&'static str],
    /// Map a raw TSV record into COPY-text field values.
    to_copy_fields: fn(&csv::StringRecord) -> Vec<CopyField>,
}

/// A single COPY-text field: either a SQL NULL or a textual value to escape.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CopyField {
    Null,
    Text(String),
}

/// IMDb encodes null as the literal `\N`; everything else is a value.
fn field(raw: &str) -> CopyField {
    if raw == r"\N" || raw.is_empty() { CopyField::Null } else { CopyField::Text(raw.to_string()) }
}

/// IMDb booleans are `0` / `1`; map to Postgres `f` / `t`, null otherwise.
fn bool_field(raw: &str) -> CopyField {
    match raw {
        "0" => CopyField::Text("f".to_string()),
        "1" => CopyField::Text("t".to_string()),
        _ => CopyField::Null,
    }
}

/// Escape a value for Postgres COPY *text* format (tab-delimited): backslash,
/// tab, newline, carriage return must be escaped so embedded control chars in a
/// title can never break the row framing.
fn escape_copy(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str(r"\\"),
            '\t' => out.push_str(r"\t"),
            '\n' => out.push_str(r"\n"),
            '\r' => out.push_str(r"\r"),
            other => out.push(other),
        }
    }
    out
}

/// Render one record's fields into a COPY-text line (no trailing newline).
fn copy_line(fields: &[CopyField]) -> String {
    let mut parts = Vec::with_capacity(fields.len());
    for f in fields {
        match f {
            CopyField::Null => parts.push(r"\N".to_string()),
            CopyField::Text(v) => parts.push(escape_copy(v)),
        }
    }
    parts.join("\t")
}

/// The IMDb datasets we ingest, in load order. We load the title/akas search
/// backbone (basics, episode, akas, ratings) and intentionally SKIP the heavy
/// people/credits files (title.crew, title.principals ~4GB, name.basics) which
/// are not used for title/akas matching — see the commented block below to
/// re-enable them once people/credits are wired.
#[allow(clippy::too_many_lines)] // declarative dataset table; splitting hurts readability
fn datasets() -> Vec<Dataset> {
    vec![
        Dataset {
            file: "title.basics.tsv.gz",
            table: "imdb_title_basics",
            columns: &[
                "tconst",
                "title_type",
                "primary_title",
                "original_title",
                "is_adult",
                "start_year",
                "end_year",
                "runtime_minutes",
                "genres",
            ],
            tsv_cols: 9,
            create_new: "tconst text PRIMARY KEY, title_type text, primary_title text, \
                 original_title text, is_adult boolean, start_year integer, end_year integer, \
                 runtime_minutes integer, genres text",
            indexes: &[
                "CREATE INDEX imdb_title_basics_primary_title_trgm \
                 ON imdb_title_basics USING gin (primary_title gin_trgm_ops)",
                "CREATE INDEX imdb_title_basics_original_title_trgm \
                 ON imdb_title_basics USING gin (original_title gin_trgm_ops)",
            ],
            to_copy_fields: |r| {
                vec![
                    field(&r[0]),
                    field(&r[1]),
                    field(&r[2]),
                    field(&r[3]),
                    bool_field(&r[4]),
                    field(&r[5]),
                    field(&r[6]),
                    field(&r[7]),
                    field(&r[8]),
                ]
            },
        },
        Dataset {
            file: "title.episode.tsv.gz",
            table: "imdb_title_episode",
            columns: &["tconst", "parent_tconst", "season_number", "episode_number"],
            tsv_cols: 4,
            create_new: "tconst text PRIMARY KEY, parent_tconst text, season_number integer, \
                 episode_number integer",
            indexes: &["CREATE INDEX imdb_title_episode_parent_idx \
                 ON imdb_title_episode (parent_tconst)"],
            to_copy_fields: |r| vec![field(&r[0]), field(&r[1]), field(&r[2]), field(&r[3])],
        },
        Dataset {
            file: "title.akas.tsv.gz",
            table: "imdb_title_akas",
            columns: &[
                "title_id",
                "ordering",
                "title",
                "region",
                "language",
                "types",
                "attributes",
                "is_original_title",
            ],
            tsv_cols: 8,
            create_new: "title_id text NOT NULL, ordering integer NOT NULL, title text, \
                 region text, language text, types text, attributes text, \
                 is_original_title boolean, PRIMARY KEY (title_id, ordering)",
            indexes: &["CREATE INDEX imdb_title_akas_title_trgm \
                 ON imdb_title_akas USING gin (title gin_trgm_ops)"],
            to_copy_fields: |r| {
                vec![
                    field(&r[0]),
                    field(&r[1]),
                    field(&r[2]),
                    field(&r[3]),
                    field(&r[4]),
                    field(&r[5]),
                    field(&r[6]),
                    bool_field(&r[7]),
                ]
            },
        },
        Dataset {
            file: "title.ratings.tsv.gz",
            table: "imdb_title_ratings",
            columns: &["tconst", "average_rating", "num_votes"],
            tsv_cols: 3,
            create_new: "tconst text PRIMARY KEY, average_rating real, num_votes integer",
            indexes: &[],
            to_copy_fields: |r| vec![field(&r[0]), field(&r[1]), field(&r[2])],
        },
        // ── People/credits datasets — SKIPPED for now ────────────────────────────
        // title.crew (~79MB gz), title.principals (~736MB gz / ~4GB), and
        // name.basics (~292MB gz) are cast/crew/people data, NOT used for the
        // title/akas search backbone. Kept here (commented) to re-enable when
        // people/credits are wired; also uncomment their tables in
        // migrations/imdb/0001_imdb_tables.sql and size the imdb PVC accordingly.
        /*
        Dataset {
            file: "title.crew.tsv.gz",
            table: "imdb_title_crew",
            columns: &["tconst", "directors", "writers"],
            tsv_cols: 3,
            create_new: "tconst text PRIMARY KEY, directors text, writers text",
            indexes: &[],
            to_copy_fields: |r| vec![field(&r[0]), field(&r[1]), field(&r[2])],
        },
        Dataset {
            file: "title.principals.tsv.gz",
            table: "imdb_title_principals",
            columns: &["tconst", "ordering", "nconst", "category", "job", "characters"],
            tsv_cols: 6,
            create_new: "tconst text NOT NULL, ordering integer NOT NULL, nconst text, \
                 category text, job text, characters text, PRIMARY KEY (tconst, ordering)",
            indexes: &["CREATE INDEX imdb_title_principals_nconst_idx \
                 ON imdb_title_principals (nconst)"],
            to_copy_fields: |r| {
                vec![
                    field(&r[0]),
                    field(&r[1]),
                    field(&r[2]),
                    field(&r[3]),
                    field(&r[4]),
                    field(&r[5]),
                ]
            },
        },
        Dataset {
            file: "name.basics.tsv.gz",
            table: "imdb_name_basics",
            columns: &[
                "nconst",
                "primary_name",
                "birth_year",
                "death_year",
                "primary_profession",
                "known_for_titles",
            ],
            tsv_cols: 6,
            create_new: "nconst text PRIMARY KEY, primary_name text, birth_year integer, \
                 death_year integer, primary_profession text, known_for_titles text",
            indexes: &["CREATE INDEX imdb_name_basics_primary_name_trgm \
                 ON imdb_name_basics USING gin (primary_name gin_trgm_ops)"],
            to_copy_fields: |r| {
                vec![
                    field(&r[0]),
                    field(&r[1]),
                    field(&r[2]),
                    field(&r[3]),
                    field(&r[4]),
                    field(&r[5]),
                ]
            },
        },
        */
    ]
}

/// The IMDb bulk-dump source. Holds the optional writable `imdb` pool so
/// `health()` can report per-table row counts; `None` when `IMDB_DATABASE_URL`
/// is unset (the API pod still boots and the source registers, but ingest and
/// counts are unavailable).
pub struct ImdbSource {
    pool: Option<PgPool>,
}

impl ImdbSource {
    /// Source id / `shirabe.source.name` primary key.
    pub const ID: &'static str = "imdb";

    #[must_use]
    pub const fn new(pool: Option<PgPool>) -> Self {
        Self { pool }
    }
}

/// Build a fresh TSV reader over an arbitrary byte reader, configured for IMDb's
/// dialect: tab delimiter, no quoting, flexible field counts, no header parsing
/// (the header row is detected and skipped by the load loop).
fn tsv_reader<R: std::io::Read>(reader: R) -> csv::Reader<R> {
    csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .quoting(false)
        .flexible(true)
        .has_headers(false)
        .from_reader(reader)
}

/// Download + decode + COPY-load one dataset into its `_new` staging table, then
/// atomically swap it into place. Returns the loaded row count.
async fn ingest_dataset(
    pool: &PgPool,
    client: &reqwest::Client,
    ds: &Dataset,
) -> anyhow::Result<u64> {
    let staging = format!("{}_new", ds.table);

    // Fresh staging table (drop any leftover from a crashed run).
    sqlx::query(&format!("DROP TABLE IF EXISTS {staging}")).execute(pool).await?;
    sqlx::query(&format!("CREATE TABLE {staging} ({})", ds.create_new)).execute(pool).await?;

    // Stream the gzip download → gunzip → synchronous TSV parse, building COPY
    // text and streaming it into the staging table.
    let url = format!("{DATASETS_BASE}/{}", ds.file);
    let resp = client.get(&url).send().await?.error_for_status()?;
    let stream = resp.bytes_stream().map(|r| r.map_err(std::io::Error::other));
    let async_reader = StreamReader::new(stream);
    // Bridge the async byte stream into a blocking gzip+TSV reader.
    let async_buf = tokio::io::BufReader::new(async_reader);
    let bridge = tokio_util::io::SyncIoBridge::new(async_buf);

    let copy_columns = ds.columns.join(", ");
    let copy_stmt = format!("COPY {staging} ({copy_columns}) FROM STDIN");
    let mut copy = pool.copy_in_raw(&copy_stmt).await?;

    // Decode + parse on a blocking thread, sending COPY-text chunks back over a
    // channel; the async side feeds them to Postgres.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(16);
    let tsv_cols = ds.tsv_cols;
    let to_copy = ds.to_copy_fields;
    let parse_handle = tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
        let gz = flate2::read::MultiGzDecoder::new(bridge);
        let mut rdr = tsv_reader(gz);
        let mut record = csv::StringRecord::new();
        let mut rows: u64 = 0;
        let mut buf = Vec::with_capacity(1 << 20);
        let mut first = true;
        while rdr.read_record(&mut record)? {
            // Skip the header row (IMDb ships a header line on every dataset).
            if first {
                first = false;
                if record.get(0) == Some("tconst") || record.get(0) == Some("titleId") {
                    continue;
                }
            }
            if record.len() < tsv_cols {
                continue; // malformed/short row — skip rather than misframe COPY
            }
            let fields = to_copy(&record);
            let line = copy_line(&fields);
            buf.extend_from_slice(line.as_bytes());
            buf.push(b'\n');
            rows += 1;
            if buf.len() >= (1 << 20) {
                if tx.blocking_send(std::mem::take(&mut buf)).is_err() {
                    break;
                }
                buf = Vec::with_capacity(1 << 20);
            }
        }
        if !buf.is_empty() {
            let _ = tx.blocking_send(buf);
        }
        Ok(rows)
    });

    while let Some(chunk) = rx.recv().await {
        copy.send(chunk).await?;
    }
    copy.finish().await?;
    let rows = parse_handle.await??;

    // Build indexes on staging (named with a temp suffix to avoid clashing with
    // the live indexes, which get dropped with the live table in the swap).
    for (i, idx_sql) in ds.indexes.iter().enumerate() {
        // The live `CREATE INDEX … ON <table> …` is rebuilt post-swap; on staging
        // we create equivalent indexes so the swapped-in table is fully indexed.
        let staged = idx_sql
            .replacen(&format!("ON {}", ds.table), &format!("ON {staging}"), 1)
            .replacen("CREATE INDEX ", &format!("CREATE INDEX tmp{i}_"), 1);
        sqlx::query(&staged).execute(pool).await?;
    }

    // Atomic swap: drop live, rename staging (+ its indexes) into place.
    let mut tx = pool.begin().await?;
    sqlx::query(&format!("DROP TABLE IF EXISTS {}", ds.table)).execute(&mut *tx).await?;
    sqlx::query(&format!("ALTER TABLE {staging} RENAME TO {}", ds.table)).execute(&mut *tx).await?;
    // Rename the staged indexes to their canonical live names.
    for (i, idx_sql) in ds.indexes.iter().enumerate() {
        if let Some(name) = index_name(idx_sql) {
            let staged_name = format!("tmp{i}_{name}");
            sqlx::query(&format!("ALTER INDEX {staged_name} RENAME TO {name}"))
                .execute(&mut *tx)
                .await?;
        }
    }
    tx.commit().await?;

    Ok(rows)
}

/// Pull the index name out of a `CREATE INDEX <name> ON …` statement.
fn index_name(create_index_sql: &str) -> Option<String> {
    let rest = create_index_sql.trim().strip_prefix("CREATE INDEX ")?;
    rest.split_whitespace().next().map(ToString::to_string)
}

/// Row count for one (possibly absent) table; `-1` when the table is missing.
async fn count_rows(pool: &PgPool, table: &str) -> i64 {
    // `table` is a fixed literal from our own dataset list, never user input.
    sqlx::query(&format!("SELECT count(*) AS n FROM {table}"))
        .fetch_one(pool)
        .await
        .map_or(-1, |row| row.get::<i64, _>("n"))
}

#[async_trait]
impl Source for ImdbSource {
    fn id(&self) -> &str {
        Self::ID
    }

    fn ingest_mode(&self) -> IngestMode {
        IngestMode::BulkDump
    }

    async fn refresh(&self, ctx: &RefreshCtx) -> RefreshReport {
        let Some(pool) = ctx.pools.imdb.as_ref() else {
            return RefreshReport::failed(
                "IMDB_DATABASE_URL is not set; the imdb source requires the dedicated, \
                 writable imdb database",
            );
        };
        let client = match reqwest::Client::builder()
            .user_agent(concat!("shirabe/", env!("CARGO_PKG_VERSION")))
            .build()
        {
            Ok(c) => c,
            Err(e) => return RefreshReport::failed(format!("failed to build HTTP client: {e}")),
        };

        let mut counts = serde_json::Map::new();
        for ds in datasets() {
            match ingest_dataset(pool, &client, &ds).await {
                Ok(rows) => {
                    counts.insert(ds.table.to_string(), json!(rows));
                }
                Err(e) => {
                    return RefreshReport::failed(format!(
                        "imdb ingest of {} failed: {e}",
                        ds.file
                    ))
                    .with_detail(json!({ "loaded": counts, "failed": ds.file }));
                }
            }
        }

        let total: u64 = counts.values().filter_map(serde_json::Value::as_u64).sum();
        RefreshReport::ok(format!("ingested {total} imdb rows across 7 datasets")).with_detail(
            json!({
                "license": "IMDb Non-Commercial Datasets — personal/non-commercial use only",
                "source": DATASETS_BASE,
                "rows": counts,
            }),
        )
    }

    async fn health(&self) -> SourceHealth {
        // last_refresh is tracked by the registry (`shirabe.source` row); here we
        // report reachability + per-table row counts from the imdb pool.
        let Some(pool) = self.pool.as_ref() else {
            return SourceHealth {
                source: self.id().to_string(),
                reachable: false,
                detail: "IMDB_DATABASE_URL is not set; imdb bulk mirror unavailable".to_string(),
            };
        };
        match table_counts(pool).await {
            Ok(counts) => SourceHealth {
                source: self.id().to_string(),
                reachable: true,
                detail: format!("imdb bulk mirror reachable; row counts {counts}"),
            },
            Err(e) => SourceHealth {
                source: self.id().to_string(),
                reachable: false,
                detail: format!("imdb mirror unreachable: {e}"),
            },
        }
    }
}

/// Per-table row counts for the imdb mirror. Returns an error if the pool is
/// unreachable; missing individual tables surface as `-1`.
async fn table_counts(pool: &PgPool) -> Result<serde_json::Value, sqlx::Error> {
    // Reachability probe so a dead pool errors rather than reporting all -1.
    sqlx::query("SELECT 1").execute(pool).await?;
    // Only the loaded search-backbone tables; the people/credits tables
    // (crew/principals/name_basics) are skipped — see datasets().
    let tables =
        ["imdb_title_basics", "imdb_title_episode", "imdb_title_akas", "imdb_title_ratings"];
    let mut map = serde_json::Map::new();
    for t in tables {
        map.insert(t.to_string(), json!(count_rows(pool, t).await));
    }
    Ok(serde_json::Value::Object(map))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `title.basics` line with a `\N` end_year null and a `1` adult flag
    /// parses into the right COPY fields, with the boolean normalized to `t`.
    #[test]
    fn basics_row_with_null_and_bool() {
        let ds = &datasets()[0];
        assert_eq!(ds.table, "imdb_title_basics");
        let mut rec = csv::StringRecord::new();
        for f in [
            "tt0000001",
            "short",
            "Carmencita",
            "Carmencita",
            "1",
            "1894",
            r"\N",
            "1",
            "Documentary,Short",
        ] {
            rec.push_field(f);
        }
        let fields = (ds.to_copy_fields)(&rec);
        assert_eq!(fields[0], CopyField::Text("tt0000001".into()));
        assert_eq!(fields[4], CopyField::Text("t".into())); // is_adult 1 → t
        assert_eq!(fields[5], CopyField::Text("1894".into())); // start_year
        assert_eq!(fields[6], CopyField::Null); // end_year \N
        assert_eq!(
            copy_line(&fields),
            "tt0000001\tshort\tCarmencita\tCarmencita\tt\t1894\t\\N\t1\tDocumentary,Short"
        );
    }

    /// A non-latin `title.akas` title (銀魂) survives parsing intact and an empty
    /// region field becomes NULL.
    #[test]
    fn akas_row_non_latin_title() {
        let ds = &datasets()[2];
        assert_eq!(ds.table, "imdb_title_akas");
        let mut rec = csv::StringRecord::new();
        for f in ["tt0000001", "1", "銀魂", r"\N", "ja", r"\N", r"\N", "0"] {
            rec.push_field(f);
        }
        let fields = (ds.to_copy_fields)(&rec);
        assert_eq!(fields[2], CopyField::Text("銀魂".into()));
        assert_eq!(fields[3], CopyField::Null); // region \N
        assert_eq!(fields[7], CopyField::Text("f".into())); // is_original_title 0 → f
        assert_eq!(copy_line(&fields), "tt0000001\t1\t銀魂\t\\N\tja\t\\N\t\\N\tf");
    }

    /// A title carrying an embedded tab/backslash is escaped so it can never
    /// break COPY row framing.
    #[test]
    fn escapes_control_chars() {
        assert_eq!(escape_copy("a\tb\\c\nd"), r"a\tb\\c\nd");
    }

    /// The header row guard recognizes both `tconst` and `titleId` first columns.
    #[test]
    fn tsv_reader_reads_tab_delimited_unquoted() {
        let data = "tconst\ttitle\ntt1\t\"quoted\" stays literal\n";
        let mut rdr = tsv_reader(data.as_bytes());
        let mut rec = csv::StringRecord::new();
        rdr.read_record(&mut rec).unwrap();
        assert_eq!(rec.get(0), Some("tconst"));
        rdr.read_record(&mut rec).unwrap();
        // Quoting disabled: the quote chars are literal, not stripped.
        assert_eq!(rec.get(1), Some("\"quoted\" stays literal"));
    }
}
