//! IMDb search-index maintenance sources (SHIB-25): `imdb-fts` and `imdb-trgm`
//! run as separate `shirabe sync` steps after `sync imdb`, because the sync's
//! staging-and-swap DROPs the live tables and their indexes, and migrations
//! (recorded as applied) never re-run to restore them.

use async_trait::async_trait;
use serde_json::json;
use sqlx::{PgPool, Row};

use super::{IngestMode, RefreshCtx, RefreshReport, Source, SourceHealth};

/// Canonical index name (checked against `pg_indexes`) + idempotent DDL.
/// The index expression must stay byte-identical to what the queries in
/// `src/queries.rs` use, and mirrors `migrations/imdb/0002` / `0003`.
struct IndexDef {
    name: &'static str,
    ddl: &'static str,
}

/// Idempotent prerequisites run before the index builds.
const FTS_PREREQS: &[&str] = &[
    "CREATE EXTENSION IF NOT EXISTS unaccent",
    // unaccent()'s 2-arg form is IMMUTABLE (1-arg is only STABLE), so this
    // wrapper is what makes the expression legal in an index.
    "CREATE OR REPLACE FUNCTION public.f_unaccent(text) RETURNS text \
     LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS \
     $$ SELECT public.unaccent('public.unaccent', $1) $$",
];

const FTS_INDEXES: &[IndexDef] = &[
    IndexDef {
        name: "imdb_title_basics_primary_title_fts_ua",
        ddl: "CREATE INDEX IF NOT EXISTS imdb_title_basics_primary_title_fts_ua \
              ON imdb_title_basics USING gin \
              (to_tsvector('simple', public.f_unaccent(primary_title)))",
    },
    IndexDef {
        name: "imdb_title_basics_original_title_fts_ua",
        ddl: "CREATE INDEX IF NOT EXISTS imdb_title_basics_original_title_fts_ua \
              ON imdb_title_basics USING gin \
              (to_tsvector('simple', public.f_unaccent(original_title)))",
    },
    IndexDef {
        name: "imdb_title_akas_title_fts_ua",
        ddl: "CREATE INDEX IF NOT EXISTS imdb_title_akas_title_fts_ua \
              ON imdb_title_akas USING gin \
              (to_tsvector('simple', public.f_unaccent(title)))",
    },
];

const TRGM_PREREQS: &[&str] = &["CREATE EXTENSION IF NOT EXISTS pg_trgm"];

const TRGM_INDEXES: &[IndexDef] = &[
    IndexDef {
        name: "imdb_title_basics_primary_title_trgm_gist",
        ddl: "CREATE INDEX IF NOT EXISTS imdb_title_basics_primary_title_trgm_gist \
              ON imdb_title_basics USING gist (primary_title gist_trgm_ops)",
    },
    IndexDef {
        name: "imdb_title_basics_original_title_trgm_gist",
        ddl: "CREATE INDEX IF NOT EXISTS imdb_title_basics_original_title_trgm_gist \
              ON imdb_title_basics USING gist (original_title gist_trgm_ops)",
    },
    IndexDef {
        name: "imdb_title_akas_title_trgm_gist",
        ddl: "CREATE INDEX IF NOT EXISTS imdb_title_akas_title_trgm_gist \
              ON imdb_title_akas USING gist (title gist_trgm_ops)",
    },
];

/// Which index set a source instance maintains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexSet {
    Fts,
    Trgm,
}

impl IndexSet {
    const fn id(self) -> &'static str {
        match self {
            Self::Fts => "imdb-fts",
            Self::Trgm => "imdb-trgm",
        }
    }

    const fn prereqs(self) -> &'static [&'static str] {
        match self {
            Self::Fts => FTS_PREREQS,
            Self::Trgm => TRGM_PREREQS,
        }
    }

    const fn indexes(self) -> &'static [IndexDef] {
        match self {
            Self::Fts => FTS_INDEXES,
            Self::Trgm => TRGM_INDEXES,
        }
    }
}

/// An index-maintenance step over the writable `imdb` database. `pool` is
/// `None` when `IMDB_DATABASE_URL` is unset (registers but can't run).
pub struct ImdbIndexSource {
    pool: Option<PgPool>,
    set: IndexSet,
}

impl ImdbIndexSource {
    #[must_use]
    pub const fn new(pool: Option<PgPool>, set: IndexSet) -> Self {
        Self { pool, set }
    }
}

/// `(name, indisvalid)` for the expected indexes that exist in `public`.
/// Validity matters: a failed `CREATE INDEX CONCURRENTLY` leaves an INVALID
/// index behind, which the planner ignores but `IF NOT EXISTS` still skips.
async fn existing_indexes(
    pool: &PgPool,
    expected: &[IndexDef],
) -> sqlx::Result<Vec<(String, bool)>> {
    let names: Vec<String> = expected.iter().map(|d| d.name.to_string()).collect();
    let rows = sqlx::query(
        "SELECT c.relname AS indexname, i.indisvalid
           FROM pg_index i
           JOIN pg_class c ON c.oid = i.indexrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = 'public' AND c.relname = ANY($1)",
    )
    .bind(&names)
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(|r| (r.get("indexname"), r.get("indisvalid"))).collect())
}

#[async_trait]
impl Source for ImdbIndexSource {
    fn id(&self) -> &str {
        self.set.id()
    }

    fn ingest_mode(&self) -> IngestMode {
        IngestMode::IndexMaintenance
    }

    async fn refresh(&self, ctx: &RefreshCtx) -> RefreshReport {
        let Some(pool) = ctx.pools.imdb.as_ref() else {
            return RefreshReport::failed(
                "IMDB_DATABASE_URL is not set; index maintenance requires the dedicated, \
                 writable imdb database",
            );
        };

        for stmt in self.set.prereqs() {
            if let Err(e) = sqlx::query(stmt).execute(pool).await {
                return RefreshReport::failed(format!("prerequisite failed ({stmt}): {e}"));
            }
        }

        let pre_existing = match existing_indexes(pool, self.set.indexes()).await {
            Ok(names) => names,
            Err(e) => return RefreshReport::failed(format!("failed to read pg_indexes: {e}")),
        };

        for (name, valid) in &pre_existing {
            if !valid {
                tracing::warn!(source = self.id(), index = %name, "dropping INVALID index");
                if let Err(e) = sqlx::query(&format!("DROP INDEX {name}")).execute(pool).await {
                    return RefreshReport::failed(format!("dropping invalid {name} failed: {e}"));
                }
            }
        }

        let mut detail = serde_json::Map::new();
        let mut built = 0usize;
        for def in self.set.indexes() {
            let existed = pre_existing.iter().any(|(n, valid)| n == def.name && *valid);
            let started = std::time::Instant::now();
            if let Err(e) = sqlx::query(def.ddl).execute(pool).await {
                return RefreshReport::failed(format!("building {} failed: {e}", def.name))
                    .with_detail(json!({ "indexes": detail }));
            }
            let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            built += usize::from(!existed);
            detail.insert(def.name.to_string(), json!({ "existed": existed, "build_ms": ms }));
            tracing::info!(source = self.id(), index = def.name, existed, ms, "index ensured");
        }

        let total = self.set.indexes().len();
        RefreshReport::ok(format!(
            "{total} indexes ensured: built {built}, {} already present",
            total - built
        ))
        .with_detail(json!({ "indexes": detail }))
    }

    async fn health(&self) -> SourceHealth {
        let Some(pool) = self.pool.as_ref() else {
            return SourceHealth {
                source: self.id().to_string(),
                reachable: false,
                detail: "IMDB_DATABASE_URL is not set; imdb database unavailable".to_string(),
            };
        };
        match existing_indexes(pool, self.set.indexes()).await {
            Ok(present) => {
                let missing: Vec<&str> = self
                    .set
                    .indexes()
                    .iter()
                    .map(|d| d.name)
                    .filter(|n| !present.iter().any(|(p, valid)| p == n && *valid))
                    .collect();
                let detail = if missing.is_empty() {
                    format!("all {} indexes present and valid", self.set.indexes().len())
                } else {
                    format!("MISSING or INVALID indexes: {}", missing.join(", "))
                };
                // A missing index is the exact failure this job exists to
                // prevent, so it must flip `healthy`, not just the detail text.
                SourceHealth {
                    source: self.id().to_string(),
                    reachable: missing.is_empty(),
                    detail,
                }
            }
            Err(e) => SourceHealth {
                source: self.id().to_string(),
                reachable: false,
                detail: format!("imdb database unreachable: {e}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every index DDL is idempotent, names its canonical index, and targets
    /// one of the two searched tables.
    #[test]
    fn ddl_is_idempotent_and_names_match() {
        for def in FTS_INDEXES.iter().chain(TRGM_INDEXES) {
            assert!(def.ddl.starts_with("CREATE INDEX IF NOT EXISTS "), "{}", def.name);
            assert!(def.ddl.contains(def.name), "DDL must name {}", def.name);
            assert!(
                def.ddl.contains("ON imdb_title_basics ")
                    || def.ddl.contains("ON imdb_title_akas "),
                "{} targets an unexpected table",
                def.name
            );
        }
    }

    /// The two sets cover exactly the six search indexes the queries rely on,
    /// with no overlap between FTS and trigram.
    #[test]
    fn sets_cover_expected_indexes() {
        let fts: Vec<&str> = FTS_INDEXES.iter().map(|d| d.name).collect();
        let trgm: Vec<&str> = TRGM_INDEXES.iter().map(|d| d.name).collect();
        assert_eq!(
            fts,
            vec![
                "imdb_title_basics_primary_title_fts_ua",
                "imdb_title_basics_original_title_fts_ua",
                "imdb_title_akas_title_fts_ua",
            ]
        );
        assert_eq!(
            trgm,
            vec![
                "imdb_title_basics_primary_title_trgm_gist",
                "imdb_title_basics_original_title_trgm_gist",
                "imdb_title_akas_title_trgm_gist",
            ]
        );
        assert!(!fts.iter().any(|n| trgm.contains(n)));
    }

    #[test]
    fn source_ids_and_mode() {
        let fts = ImdbIndexSource::new(None, IndexSet::Fts);
        let trgm = ImdbIndexSource::new(None, IndexSet::Trgm);
        assert_eq!(fts.id(), "imdb-fts");
        assert_eq!(trgm.id(), "imdb-trgm");
        assert_eq!(fts.ingest_mode(), IngestMode::IndexMaintenance);
    }
}
