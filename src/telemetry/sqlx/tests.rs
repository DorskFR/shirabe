use opentelemetry::trace::SpanKind;

use super::classify;
use crate::telemetry::testing::{Capture, attribute};

#[test]
fn classify_select_from() {
    assert_eq!(
        classify("SELECT a.gid FROM musicbrainz.artist a WHERE a.id = $1"),
        ("SELECT", Some("musicbrainz.artist"))
    );
    assert_eq!(classify(r#"select x from "Quoted" where y = 1"#), ("SELECT", Some("Quoted")));
    assert_eq!(classify("SELECT 'FROM decoy' AS s FROM release"), ("SELECT", Some("release")));
}

#[test]
fn classify_select_skips_nested_and_function_froms() {
    assert_eq!(
        classify("SELECT EXTRACT(EPOCH FROM now()) AS t FROM jobs"),
        ("SELECT", Some("jobs"))
    );
    assert_eq!(
        classify("SELECT EXISTS(SELECT 1 FROM jobs WHERE id = $1)"),
        ("SELECT", Some("jobs"))
    );
    assert_eq!(classify("SELECT x FROM unnest($1::int[]) AS x"), ("SELECT", None));
    assert_eq!(classify("SELECT set_limit($1)"), ("SELECT", None));
}

#[test]
fn classify_writes_and_ctes() {
    assert_eq!(
        classify("INSERT INTO xref (a, b) VALUES ($1, $2) ON CONFLICT DO NOTHING"),
        ("INSERT", Some("xref"))
    );
    assert_eq!(classify("UPDATE ONLY sources SET ok = true"), ("UPDATE", Some("sources")));
    assert_eq!(classify("DELETE FROM search_cache WHERE k = $1"), ("DELETE", Some("search_cache")));
    assert_eq!(
        classify("WITH hits AS (SELECT id FROM artist) SELECT * FROM hits -- trailing"),
        ("SELECT", Some("hits"))
    );
    assert_eq!(classify("SET statement_timeout = 500"), ("SET", None));
    assert_eq!(classify("  /* c */ "), ("UNKNOWN", None));
}

fn statement_event(summary: &str, statement: &str, elapsed_secs: f64) {
    tracing::debug!(
        target: "sqlx::query",
        summary,
        db.statement = statement,
        rows_affected = 0_u64,
        rows_returned = 3_u64,
        elapsed_secs,
    );
}

#[test]
fn a_finished_statement_becomes_a_child_client_span() {
    let capture = Capture::install();
    let sql = "SELECT gid, name FROM artist WHERE id = ANY($1)";
    {
        let _enter = tracing::info_span!("shirabe.lookup").entered();
        statement_event("SELECT gid, name FROM …", &format!("\n\n{sql}\n"), 0.25);
    }
    let parent = &capture.named("shirabe.lookup")[0];
    let db = &capture.named("SELECT artist")[0];
    assert_eq!(db.span_kind, SpanKind::Client);
    assert_eq!(db.parent_span_id, parent.span_context.span_id());
    assert_eq!(attribute(db, "db.query.text").as_deref(), Some(sql));
    assert_eq!(attribute(db, "db.statement").as_deref(), Some(sql));
    assert_eq!(attribute(db, "db.collection.name").as_deref(), Some("artist"));
    assert_eq!(attribute(db, "db.response.returned_rows").as_deref(), Some("3"));
    let took = db.end_time.duration_since(db.start_time).unwrap();
    assert!(took.as_secs_f64() > 0.24 && took.as_secs_f64() < 0.3, "{took:?}");
}

#[test]
fn short_statements_come_from_the_summary() {
    let capture = Capture::install();
    {
        let _enter = tracing::info_span!("shirabe.session").entered();
        statement_event("SET jit = off", "", 0.001);
    }
    let db = &capture.named("SET")[0];
    assert_eq!(attribute(db, "db.query.text").as_deref(), Some("SET jit = off"));
}

#[test]
fn statements_outside_a_span_are_not_exported() {
    let capture = Capture::install();
    statement_event("SELECT 1", "", 0.001);
    assert_eq!(capture.spans(), Vec::new());
}
