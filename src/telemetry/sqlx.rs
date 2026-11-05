//! `db.query` client spans for every SQL statement, built from the
//! `sqlx::query` event sqlx emits when a statement finishes (full SQL, row
//! counts, elapsed time). No call site wraps its queries.

use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use opentelemetry::trace::{Span as _, SpanKind, TraceContextExt, Tracer as _};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::trace::Tracer;
use tracing::dispatcher::WeakDispatch;
use tracing::field::{Field, Visit};
use tracing::{Dispatch, Event, Subscriber};
use tracing_opentelemetry::get_otel_context;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::Context as LayerContext;
use tracing_subscriber::registry::LookupSpan;

const TARGET: &str = "sqlx::query";
const STATEMENTS: [&str; 5] = ["SELECT", "INSERT", "UPDATE", "DELETE", "MERGE"];

/// sqlx logs finished statements at DEBUG (slow ones at WARN); the layer must
/// also see the exported spans to find each statement's parent.
#[must_use]
pub fn statement_filter(export_filter: EnvFilter) -> EnvFilter {
    export_filter.add_directive(format!("{TARGET}=debug").parse().expect("static directive"))
}

pub struct StatementSpans {
    tracer: Tracer,
    /// `get_default` is a no-op inside a dispatch; keep our own handle.
    dispatch: OnceLock<WeakDispatch>,
}

impl StatementSpans {
    #[must_use]
    pub const fn new(tracer: Tracer) -> Self {
        Self { tracer, dispatch: OnceLock::new() }
    }
}

impl<S> tracing_subscriber::Layer<S> for StatementSpans
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_register_dispatch(&self, dispatch: &Dispatch) {
        let _ = self.dispatch.set(dispatch.downgrade());
    }

    fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
        if event.metadata().target() != TARGET {
            return;
        }
        let Some(dispatch) = self.dispatch.get().and_then(WeakDispatch::upgrade) else { return };
        // A statement outside any traced work (pool warm-up, startup) is noise.
        let Some(parent) = exported_parent(event, &ctx, &dispatch) else { return };
        let mut fields = Statement::default();
        event.record(&mut fields);
        let Some(sql) = fields.sql() else { return };
        emit(&self.tracer, &parent, sql, &fields);
    }
}

/// The innermost enclosing span that is exported.
fn exported_parent<S>(
    event: &Event<'_>,
    ctx: &LayerContext<'_, S>,
    dispatch: &Dispatch,
) -> Option<Context>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let ids: Vec<_> = ctx.event_scope(event)?.map(|span| span.id()).collect();
    ids.iter().find_map(|id| {
        get_otel_context(id, dispatch).filter(|cx| cx.span().span_context().is_valid())
    })
}

fn emit(tracer: &Tracer, parent: &Context, sql: &str, fields: &Statement) {
    let end = SystemTime::now();
    let elapsed = fields.elapsed_secs.map_or(Duration::ZERO, Duration::from_secs_f64);
    let (operation, table) = classify(sql);
    let name = table.map_or_else(|| operation.to_owned(), |t| format!("{operation} {t}"));
    let mut attributes = vec![
        KeyValue::new("db.system.name", "postgresql"),
        KeyValue::new("db.system", "postgresql"),
        KeyValue::new("db.operation.name", operation.to_owned()),
        KeyValue::new("db.query.text", sql.to_owned()),
        KeyValue::new("db.statement", sql.to_owned()),
    ];
    if let Some(table) = table {
        attributes.push(KeyValue::new("db.collection.name", table.to_owned()));
    }
    if let Some(rows) = fields.rows_returned {
        attributes.push(KeyValue::new("db.response.returned_rows", rows));
    }
    if let Some(rows) = fields.rows_affected.filter(|&n| n > 0) {
        attributes.push(KeyValue::new("db.response.affected_rows", rows));
    }
    if fields.slow {
        attributes.push(KeyValue::new("db.slow_statement", true));
    }
    let mut span = tracer
        .span_builder(name)
        .with_kind(SpanKind::Client)
        .with_start_time(end.checked_sub(elapsed).unwrap_or(end))
        .with_attributes(attributes)
        .start_with_context(tracer, parent);
    span.end_with_timestamp(end);
}

#[derive(Default)]
struct Statement {
    summary: Option<String>,
    statement: Option<String>,
    rows_returned: Option<i64>,
    rows_affected: Option<i64>,
    elapsed_secs: Option<f64>,
    slow: bool,
}

impl Statement {
    /// sqlx puts short statements whole in `summary` and leaves `db.statement` empty.
    fn sql(&self) -> Option<&str> {
        self.statement
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .or_else(|| self.summary.as_deref().map(str::trim).filter(|s| !s.is_empty()))
    }
}

impl Visit for Statement {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "summary" => self.summary = Some(value.to_owned()),
            "db.statement" => self.statement = Some(value.to_owned()),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        let value = i64::try_from(value).unwrap_or(i64::MAX);
        match field.name() {
            "rows_returned" => self.rows_returned = Some(value),
            "rows_affected" => self.rows_affected = Some(value),
            _ => {}
        }
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        if field.name() == "elapsed_secs" && value.is_finite() && value >= 0.0 {
            self.elapsed_secs = Some(value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.slow = format!("{value:?}").starts_with("slow statement"),
            "summary" => self.summary = Some(format!("{value:?}")),
            _ => {}
        }
    }
}

fn classify(sql: &str) -> (&str, Option<&str>) {
    let mut words = Words { sql, pos: 0, depth: 0 };
    let Some(first) = words.next() else { return ("UNKNOWN", None) };
    let verb = if first.text.eq_ignore_ascii_case("WITH") {
        let depth = first.depth;
        let Some(verb) = words.find(|w| w.depth == depth && statement(w.text).is_some()) else {
            return (first.text, None);
        };
        verb
    } else {
        first
    };
    let Some(operation) = statement(verb.text) else { return (verb.text, None) };
    let named = match operation {
        "SELECT" => return (operation, select_table(words, verb.depth)),
        "UPDATE" => table(words, verb.depth),
        "DELETE" => after(words, "FROM", verb.depth),
        _ => after(words, "INTO", verb.depth),
    };
    (operation, named.map(|w| w.text))
}

fn statement(word: &str) -> Option<&'static str> {
    STATEMENTS.iter().find(|k| k.eq_ignore_ascii_case(word)).copied()
}

fn select_table(mut words: Words<'_>, depth: u32) -> Option<&str> {
    let mut nested = None;
    loop {
        let Some(word) = words.next() else { return nested };
        if !word.text.eq_ignore_ascii_case("FROM") {
            continue;
        }
        let found = table(words.clone(), word.depth).filter(|w| !w.call).map(|w| w.text);
        if word.depth == depth {
            return found.or(nested);
        }
        if nested.is_none() {
            nested = found;
        }
    }
}

fn after<'a>(mut words: Words<'a>, keyword: &str, depth: u32) -> Option<Word<'a>> {
    words.find(|w| w.depth == depth && w.text.eq_ignore_ascii_case(keyword))?;
    table(words, depth)
}

fn table(mut words: Words<'_>, depth: u32) -> Option<Word<'_>> {
    let mut word = words.next()?;
    if word.text.eq_ignore_ascii_case("ONLY") {
        word = words.next()?;
    }
    let named =
        word.depth == depth && word.text.bytes().next().is_some_and(|b| !b.is_ascii_digit());
    named.then_some(word)
}

struct Word<'a> {
    text: &'a str,
    depth: u32,
    /// Followed by `(`: a function call, not a table.
    call: bool,
}

#[derive(Clone)]
struct Words<'a> {
    sql: &'a str,
    pos: usize,
    depth: u32,
}

impl<'a> Words<'a> {
    fn skip_past(&mut self, needle: &[u8]) {
        let rest = &self.sql.as_bytes()[self.pos..];
        self.pos += rest
            .windows(needle.len())
            .position(|w| w == needle)
            .map_or(rest.len(), |at| at + needle.len());
    }

    fn word(&self, start: usize, end: usize) -> Word<'a> {
        let call = self.sql.as_bytes()[self.pos..]
            .iter()
            .find(|b| !b.is_ascii_whitespace())
            .is_some_and(|&b| b == b'(');
        Word { text: &self.sql[start..end], depth: self.depth, call }
    }
}

const fn is_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.'
}

impl<'a> Iterator for Words<'a> {
    type Item = Word<'a>;

    fn next(&mut self) -> Option<Word<'a>> {
        let sql = self.sql;
        let bytes = sql.as_bytes();
        while let Some(&byte) = bytes.get(self.pos) {
            let rest = &bytes[self.pos..];
            match byte {
                b'-' if rest.starts_with(b"--") => self.skip_past(b"\n"),
                b'/' if rest.starts_with(b"/*") => self.skip_past(b"*/"),
                b'\'' => {
                    self.pos += 1;
                    self.skip_past(b"'");
                }
                b'(' => {
                    self.depth += 1;
                    self.pos += 1;
                }
                b')' => {
                    self.depth = self.depth.saturating_sub(1);
                    self.pos += 1;
                }
                b'"' => {
                    let start = self.pos + 1;
                    let len = rest[1..].iter().position(|&b| b == b'"').unwrap_or(rest.len() - 1);
                    self.pos = (start + len + 1).min(bytes.len());
                    return Some(self.word(start, start + len));
                }
                _ if is_word(byte) => {
                    let start = self.pos;
                    self.pos += rest.iter().take_while(|&&b| is_word(b)).count();
                    return Some(self.word(start, self.pos));
                }
                _ => self.pos += 1,
            }
        }
        None
    }
}

#[cfg(test)]
mod tests;
