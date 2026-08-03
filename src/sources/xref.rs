//! Cross-ID (xref) store — the `shirabe.xref` table bridging a title's id across
//! providers (SHIB-8).
//!
//! Rows are fed **incrementally** by provider hydration: as the TMDB and TheTVDB
//! facades hydrate a record they read the cross-ids those providers already expose
//! (TMDB `external_ids`, TheTVDB `remoteIds`) and call [`upsert_xref`] to persist
//! the `(wikidata_qid, source, external_id)` rows. There is no bulk dump; the
//! store grows lazily as records are fetched. Upsert
//! (`ON CONFLICT (source, external_id)`) keeps writes idempotent.
//!
//! Writes only the `shirabe` coordination DB.

use sqlx::PgPool;

/// One cross-id row: `(wikidata_qid, source, external_id)`.
pub type XrefRow = (Option<String>, String, String);

/// Upsert a batch of cross-id rows into `shirabe.xref`. Called by per-record
/// hydration (SHIB-6/7 self-links) with the cross-ids providers already expose.
/// `wikidata_qid` may be `None` (e.g. a provider self-link not bridged through
/// Wikidata); the column is nullable. Idempotent via
/// `ON CONFLICT (source, external_id)`.
///
/// Runtime query (no compile-time macro); writes only the `shirabe` schema.
const UPSERT_XREF: &str = "INSERT INTO shirabe.xref (wikidata_qid, source, external_id)
     VALUES ($1, $2, $3)
     ON CONFLICT (source, external_id) DO UPDATE SET
         wikidata_qid = COALESCE(EXCLUDED.wikidata_qid, shirabe.xref.wikidata_qid)";

pub async fn upsert_xref(pool: &PgPool, rows: &[XrefRow]) -> Result<u64, sqlx::Error> {
    let mut affected = 0u64;
    for (qid, source, external_id) in rows {
        let res = sqlx::query(UPSERT_XREF)
            .bind(qid.as_deref())
            .bind(source)
            .bind(external_id)
            .execute(pool)
            .await?;
        affected += res.rows_affected();
    }
    Ok(affected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_batch_is_a_no_op() {
        let pool = PgPool::connect_lazy("postgres://unreachable.invalid/none").unwrap();
        assert_eq!(upsert_xref(&pool, &[]).await.unwrap(), 0);
    }

    #[test]
    fn upsert_statement_invariants() {
        assert!(UPSERT_XREF.trim_start().starts_with("INSERT INTO shirabe.xref"));
        assert!(UPSERT_XREF.contains("ON CONFLICT (source, external_id) DO UPDATE"));
        assert!(
            UPSERT_XREF.contains("COALESCE(EXCLUDED.wikidata_qid, shirabe.xref.wikidata_qid)"),
            "a later NULL qid must never erase a known qid"
        );
        assert!(UPSERT_XREF.contains("$3") && !UPSERT_XREF.contains("$4"));
        for schema_write in ["musicbrainz.", "DELETE", "TRUNCATE"] {
            assert!(!UPSERT_XREF.contains(schema_write));
        }
    }
}
