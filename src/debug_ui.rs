//! Opt-in SQL query explorer (SHIB-21), mounted at `/debug/queries` only when
//! `SHIRABE_DEBUG_UI=1`.
//!
//! It renders every statement in [`crate::queries::catalog`] — the exact SQL the
//! handlers run — and lets an operator execute each one, or its
//! `EXPLAIN [ANALYZE]`, against the live pools with adjustable bind params and
//! trigram-session knobs (`set_limit` threshold, `work_mem`, `statement_timeout`,
//! where `0` disables the timeout so a query that would otherwise hit the 10s cap
//! can still be fully `EXPLAIN ANALYZE`d). Built to diagnose slow trigram
//! searches (e.g. a movie search for "dune").
//!
//! Safety: only catalog SQL is runnable (no free-form SQL box); every `$n` is a
//! bound parameter (never string-interpolated); `work_mem` is sanitised via
//! [`crate::search::sanitize_work_mem`]. The page is still gated off by default.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::postgres::PgArguments;
use sqlx::{PgPool, Postgres, Row};
use uuid::Uuid;

use crate::AppState;
use crate::queries::{self, ParamType, QuerySpec, TargetDb};
use crate::search::sanitize_work_mem;

/// Routes for the explorer. Merged into the main router only when enabled.
pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/debug/queries", get(page)).route("/debug/run", post(run))
}

// ── page ──────────────────────────────────────────────────────

/// Serialise the catalog to JSON for the page's embedded `CATALOG` constant, so
/// the whole UI is generated client-side from the same specs the handlers use.
fn catalog_json() -> Value {
    let specs: Vec<Value> = queries::catalog()
        .into_iter()
        .map(|q| {
            let params: Vec<Value> = q
                .params
                .iter()
                .map(|p| {
                    json!({
                        "name": p.name,
                        "type": param_type_str(p.ty),
                        "nullable": p.nullable,
                        "example": p.example,
                    })
                })
                .collect();
            json!({
                "id": q.id,
                "title": q.title,
                "endpoint": q.endpoint,
                "db": db_str(q.db),
                "trigram": q.trigram,
                "sql": q.sql,
                "params": params,
            })
        })
        .collect();
    Value::Array(specs)
}

async fn page() -> impl IntoResponse {
    let catalog = serde_json::to_string(&catalog_json()).unwrap_or_else(|_| "[]".to_string());
    Html(PAGE_HTML.replace("/*__CATALOG__*/", &catalog))
}

// ── runner ────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RunRequest {
    /// Catalog query id.
    id: String,
    /// `run` | `explain` | `explain_analyze`.
    mode: String,
    /// Positional param values (as typed in the UI), in `$1..$n` order.
    #[serde(default)]
    params: Vec<String>,
    /// pg_trgm `%` cutoff (`set_limit`) for trigram queries.
    #[serde(default = "default_threshold")]
    threshold: f64,
    /// `work_mem` for the session (sanitised before splicing).
    #[serde(default = "default_work_mem")]
    work_mem: String,
    /// `statement_timeout` in ms; `0` disables it (so a slow query still EXPLAINs).
    #[serde(default)]
    timeout_ms: i64,
    /// How many times to execute the statement; timings are collected per run and
    /// reported as min/median/max so a single noisy sample can't mislead.
    #[serde(default = "default_iterations")]
    iterations: u32,
}

const fn default_threshold() -> f64 {
    0.3
}
fn default_work_mem() -> String {
    "256MB".to_string()
}
const fn default_iterations() -> u32 {
    1
}

async fn run(State(state): State<Arc<AppState>>, Json(req): Json<RunRequest>) -> impl IntoResponse {
    match run_query(&state, req).await {
        Ok(v) => Json(v),
        Err(e) => Json(json!({ "ok": false, "error": e })),
    }
}

/// Resolve the target pool, honouring optional (unconfigured) provider pools.
fn pool_for(state: &AppState, db: TargetDb) -> Result<&PgPool, String> {
    match db {
        TargetDb::Musicbrainz => Ok(state.pool()),
        TargetDb::Imdb => state
            .pools
            .imdb
            .as_ref()
            .ok_or_else(|| "imdb pool not configured (IMDB_DATABASE_URL unset)".to_string()),
        TargetDb::Tmdb => state
            .pools
            .tmdb
            .as_ref()
            .ok_or_else(|| "tmdb pool not configured (TMDB_DATABASE_URL unset)".to_string()),
    }
}

async fn run_query(state: &AppState, req: RunRequest) -> Result<Value, String> {
    let spec = queries::find(&req.id).ok_or_else(|| format!("unknown query id `{}`", req.id))?;
    if req.params.len() != spec.params.len() {
        return Err(format!(
            "query `{}` expects {} params, got {}",
            spec.id,
            spec.params.len(),
            req.params.len()
        ));
    }
    let pool = pool_for(state, spec.db)?;

    let final_sql = build_sql(&spec, &req.mode)?;

    let mut conn = pool.acquire().await.map_err(|e| format!("acquire connection: {e}"))?;

    let iterations = req.iterations.clamp(1, 25);
    let mut timings_ms: Vec<f64> = Vec::with_capacity(iterations as usize);
    let mut last_rows = Vec::new();

    // statement_timeout: 0 disables (Postgres semantics). Applied once for the
    // session; splicing (not binding) is required — SET takes no parameters.
    let timeout = req.timeout_ms.max(0);
    sqlx::query(&format!("SET statement_timeout = {timeout}"))
        .execute(&mut *conn)
        .await
        .map_err(|e| format!("set statement_timeout: {e}"))?;

    // Trigram queries read the `%` cutoff GUC and benefit from a larger work_mem —
    // mirror how the handler configures the session (crate::search).
    if spec.trigram {
        sqlx::query("SELECT set_limit($1)")
            .bind(req.threshold as f32)
            .execute(&mut *conn)
            .await
            .map_err(|e| format!("set_limit: {e}"))?;
        sqlx::query(&format!("SET work_mem = '{}'", sanitize_work_mem(&req.work_mem)))
            .execute(&mut *conn)
            .await
            .map_err(|e| format!("set work_mem: {e}"))?;
    }

    for _ in 0..iterations {
        let mut q = sqlx::query(&final_sql);
        for (param, raw) in spec.params.iter().zip(req.params.iter()) {
            q = bind_param(q, param.ty, param.nullable, raw.trim())?;
        }

        let started = std::time::Instant::now();
        let rows = q.fetch_all(&mut *conn).await.map_err(|e| e.to_string())?;
        timings_ms.push(started.elapsed().as_secs_f64() * 1000.0);
        last_rows = rows;
    }

    let (min_ms, median_ms, max_ms) = summarize(&timings_ms);

    if req.mode == "explain" || req.mode == "explain_analyze" {
        // Default (TEXT) EXPLAIN → one text column ("QUERY PLAN") per line.
        let plan: Vec<String> =
            last_rows.iter().map(|r| r.try_get::<String, _>(0).unwrap_or_default()).collect();
        Ok(json!({
            "ok": true,
            "mode": req.mode,
            "elapsed_ms": min_ms,
            "min_ms": min_ms,
            "median_ms": median_ms,
            "max_ms": max_ms,
            "iterations": iterations,
            "final_sql": final_sql,
            "plan": plan.join("\n"),
        }))
    } else {
        // `run`: each row is a single ::text column holding the row as JSON.
        let mut out = Vec::with_capacity(last_rows.len());
        for r in &last_rows {
            let s: String = r.try_get::<String, _>(0).unwrap_or_default();
            out.push(serde_json::from_str::<Value>(&s).unwrap_or(Value::String(s)));
        }
        Ok(json!({
            "ok": true,
            "mode": req.mode,
            "elapsed_ms": min_ms,
            "min_ms": min_ms,
            "median_ms": median_ms,
            "max_ms": max_ms,
            "iterations": iterations,
            "final_sql": final_sql,
            "row_count": out.len(),
            "rows": out,
        }))
    }
}

/// (min, median, max) of the collected per-iteration timings.
fn summarize(timings: &[f64]) -> (f64, f64, f64) {
    if timings.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let mut sorted = timings.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let min = sorted[0];
    let max = sorted[sorted.len() - 1];
    let median = sorted[sorted.len() / 2];
    (min, median, max)
}

/// Wrap / prefix the catalog SQL for the requested mode. `run` wraps the SELECT
/// so any row shape comes back as one JSON `::text` column (type-agnostic, needs
/// no sqlx `json` feature) and is capped; EXPLAIN prefixes the raw statement.
fn build_sql(spec: &QuerySpec, mode: &str) -> Result<String, String> {
    match mode {
        "run" => {
            Ok(format!("SELECT (to_jsonb(t.*))::text AS row FROM (\n{}\n) t LIMIT 500", spec.sql))
        }
        "explain" => Ok(format!("EXPLAIN (VERBOSE) {}", spec.sql)),
        "explain_analyze" => Ok(format!("EXPLAIN (ANALYZE, BUFFERS, VERBOSE) {}", spec.sql)),
        other => Err(format!("unknown mode `{other}`")),
    }
}

/// Bind one positional param, parsing the UI string into the declared type. A
/// blank value binds SQL `NULL` for a `nullable` scalar; blank arrays bind empty
/// arrays (the queries gate on `cardinality(...) = 0`).
fn bind_param<'q>(
    q: sqlx::query::Query<'q, Postgres, PgArguments>,
    ty: ParamType,
    nullable: bool,
    raw: &str,
) -> Result<sqlx::query::Query<'q, Postgres, PgArguments>, String> {
    let blank = raw.is_empty();
    Ok(match ty {
        ParamType::Text => {
            if nullable && blank {
                q.bind(Option::<String>::None)
            } else {
                q.bind(raw.to_string())
            }
        }
        ParamType::Int => {
            if nullable && blank {
                q.bind(Option::<i32>::None)
            } else {
                q.bind(raw.parse::<i32>().map_err(|_| format!("`{raw}` is not an int"))?)
            }
        }
        ParamType::BigInt => {
            if nullable && blank {
                q.bind(Option::<i64>::None)
            } else {
                q.bind(raw.parse::<i64>().map_err(|_| format!("`{raw}` is not a bigint"))?)
            }
        }
        ParamType::Uuid => {
            if nullable && blank {
                q.bind(Option::<Uuid>::None)
            } else {
                q.bind(Uuid::parse_str(raw).map_err(|_| format!("`{raw}` is not a uuid"))?)
            }
        }
        ParamType::IntArray => q.bind(parse_int_array::<i32>(raw)?),
        ParamType::BigIntArray => q.bind(parse_int_array::<i64>(raw)?),
        ParamType::TextArray => q.bind(parse_text_array(raw)),
    })
}

fn parse_text_array(raw: &str) -> Vec<String> {
    if raw.is_empty() {
        return Vec::new();
    }
    raw.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect()
}

fn parse_int_array<T: std::str::FromStr>(raw: &str) -> Result<Vec<T>, String> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<T>().map_err(|_| format!("`{s}` is not a valid integer")))
        .collect()
}

const fn param_type_str(ty: ParamType) -> &'static str {
    match ty {
        ParamType::Text => "text",
        ParamType::Int => "int",
        ParamType::BigInt => "bigint",
        ParamType::Uuid => "uuid",
        ParamType::IntArray => "int[]",
        ParamType::BigIntArray => "bigint[]",
        ParamType::TextArray => "text[]",
    }
}

const fn db_str(db: TargetDb) -> &'static str {
    match db {
        TargetDb::Musicbrainz => "musicbrainz",
        TargetDb::Imdb => "imdb",
        TargetDb::Tmdb => "tmdb",
    }
}

// ── static page (data-driven from the embedded CATALOG) ───────

const PAGE_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>shirabe · query explorer</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace;
         background: #0e1116; color: #d7dde5; display: flex; height: 100vh; }
  aside { width: 320px; border-right: 1px solid #232a33; overflow-y: auto; flex: none; }
  aside h1 { font-size: 14px; margin: 0; padding: 12px 14px; border-bottom: 1px solid #232a33;
             position: sticky; top: 0; background: #0e1116; }
  .grp { padding: 8px 14px 4px; color: #7d8896; text-transform: uppercase; font-size: 11px; letter-spacing: .06em; }
  .item { padding: 7px 14px; cursor: pointer; border-left: 3px solid transparent; }
  .item:hover { background: #161c24; }
  .item.active { background: #1b2430; border-left-color: #4c8bf5; }
  .item .t { color: #e8edf3; }
  .item .e { color: #6b7684; font-size: 12px; }
  main { flex: 1; overflow-y: auto; padding: 18px 22px; }
  h2 { margin: 0 0 4px; font-size: 16px; }
  .meta { color: #7d8896; margin-bottom: 14px; }
  .badge { display: inline-block; padding: 1px 7px; border-radius: 10px; background: #22303f;
           color: #9ecbff; font-size: 11px; margin-left: 6px; }
  pre { background: #11161d; border: 1px solid #232a33; border-radius: 6px; padding: 12px;
        overflow-x: auto; white-space: pre; }
  .sql { color: #cfe1ff; }
  label { display: block; color: #9aa5b1; font-size: 12px; margin: 8px 0 2px; }
  input, select { background: #11161d; border: 1px solid #2b3441; color: #e8edf3;
                  border-radius: 5px; padding: 6px 8px; font: inherit; width: 100%; }
  .params { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 14px; }
  .knobs { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px 14px; margin-top: 6px; }
  .row { margin-top: 14px; display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
  button { background: #2b6cf0; border: 0; color: #fff; padding: 8px 16px; border-radius: 6px;
           cursor: pointer; font: inherit; }
  button.secondary { background: #2b3441; }
  button:disabled { opacity: .5; cursor: default; }
  .status { color: #7d8896; }
  .err { color: #ff8f8f; }
  .ok { color: #86e29b; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th, td { border: 1px solid #232a33; padding: 4px 8px; text-align: left; vertical-align: top;
           max-width: 420px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  th { background: #161c24; color: #9ecbff; position: sticky; top: 0; }
  section { margin-top: 18px; }
  .hint { color: #6b7684; font-size: 12px; }
  td.ko { color: #ff8f8f; font-weight: 600; }
  td.pass { color: #86e29b; font-weight: 600; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
</style>
</head>
<body>
<aside>
  <h1>shirabe · queries</h1>
  <div style="padding:10px 14px; border-bottom:1px solid #232a33;">
    <button id="benchBtn" style="width:100%">▶ Benchmark all</button>
  </div>
  <div id="list"></div>
</aside>
<main id="main">
  <p class="status">Select a query on the left.</p>
</main>
<script>
const CATALOG = /*__CATALOG__*/;
const DB_ORDER = ["musicbrainz", "imdb", "tmdb"];
let current = null;

function el(tag, attrs, ...kids) {
  const n = document.createElement(tag);
  for (const k in (attrs||{})) {
    if (k === "class") n.className = attrs[k];
    else if (k === "html") n.innerHTML = attrs[k];
    else n.setAttribute(k, attrs[k]);
  }
  for (const c of kids) n.append(c);
  return n;
}

function renderList() {
  const list = document.getElementById("list");
  list.innerHTML = "";
  for (const db of DB_ORDER) {
    const items = CATALOG.filter(q => q.db === db);
    if (!items.length) continue;
    list.append(el("div", {class:"grp"}, db));
    for (const q of items) {
      const item = el("div", {class:"item", "data-id":q.id},
        el("div", {class:"t"}, q.title),
        el("div", {class:"e"}, q.endpoint));
      item.onclick = () => select(q.id);
      list.append(item);
    }
  }
}

function select(id) {
  current = CATALOG.find(q => q.id === id);
  document.querySelectorAll(".item").forEach(i =>
    i.classList.toggle("active", i.dataset.id === id));
  renderDetail();
}

function renderDetail() {
  const q = current;
  const m = document.getElementById("main");
  m.innerHTML = "";
  m.append(el("h2", {}, q.title, el("span", {class:"badge"}, q.db + (q.trigram ? " · trigram" : ""))));
  m.append(el("div", {class:"meta"}, q.endpoint));
  m.append(el("pre", {class:"sql"}, q.sql.trim()));

  // params
  if (q.params.length) {
    const box = el("div", {class:"params"});
    q.params.forEach((p, i) => {
      const wrap = el("div", {});
      wrap.append(el("label", {}, `$${i+1} ${p.name} : ${p.type}${p.nullable ? " (nullable)" : ""}`));
      const inp = el("input", {value: p.example, id:"p"+i, placeholder: p.nullable ? "(blank = NULL)" : ""});
      wrap.append(inp);
      box.append(wrap);
    });
    m.append(el("section", {}, el("label", {}, "Parameters"), box));
  }

  // session knobs
  const knobs = el("div", {class:"knobs"});
  const mk = (id, label, val) => {
    const w = el("div", {});
    w.append(el("label", {}, label));
    w.append(el("input", {id, value: val}));
    return w;
  };
  knobs.append(mk("threshold", "set_limit (threshold)", "0.3"));
  knobs.append(mk("work_mem", "work_mem", "256MB"));
  knobs.append(mk("timeout_ms", "statement_timeout ms (0=off)", "0"));
  knobs.append(mk("iterations", "iterations (min/med/max)", "1"));
  const section = el("section", {});
  section.append(el("label", {}, "Session"));
  section.append(knobs);
  section.append(el("div", {class:"hint"},
    "iterations run the statement N times on a warm connection; read the reported min as the warm-cache floor."));
  if (!q.trigram) section.append(el("div", {class:"hint"}, "threshold/work_mem ignored (non-trigram query)"));
  m.append(section);

  // actions
  const bar = el("div", {class:"row"});
  const btn = (label, mode, cls) => {
    const b = el("button", cls ? {class:cls} : {}, label);
    b.onclick = () => execute(mode);
    return b;
  };
  bar.append(btn("Run", "run"));
  bar.append(btn("EXPLAIN", "explain", "secondary"));
  bar.append(btn("EXPLAIN ANALYZE", "explain_analyze", "secondary"));
  bar.append(el("span", {class:"status", id:"status"}, ""));
  m.append(bar);

  m.append(el("section", {id:"result"}));
}

async function execute(mode) {
  const q = current;
  const params = q.params.map((_, i) => document.getElementById("p"+i).value);
  const body = {
    id: q.id, mode, params,
    threshold: parseFloat(document.getElementById("threshold").value) || 0.3,
    work_mem: document.getElementById("work_mem").value || "256MB",
    timeout_ms: parseInt(document.getElementById("timeout_ms").value, 10) || 0,
    iterations: parseInt(document.getElementById("iterations").value, 10) || 1,
  };
  const status = document.getElementById("status");
  const result = document.getElementById("result");
  status.textContent = "running…"; status.className = "status";
  result.innerHTML = "";
  const t0 = performance.now();
  try {
    const r = await fetch("/debug/run", {method:"POST", headers:{"content-type":"application/json"}, body: JSON.stringify(body)});
    const data = await r.json();
    const wall = (performance.now() - t0).toFixed(0);
    if (!data.ok) {
      status.textContent = "error"; status.className = "err";
      result.append(el("pre", {class:"err"}, data.error));
      return;
    }
    const iters = data.iterations || 1;
    const dist = iters > 1
      ? ` · db min ${data.min_ms.toFixed(1)} / med ${data.median_ms.toFixed(1)} / max ${data.max_ms.toFixed(1)} ms (${iters}×)`
      : ` · db ${data.elapsed_ms.toFixed(1)} ms`;
    status.innerHTML = `<span class="ok">ok</span>${dist} · round-trip ${wall} ms`;
    if (data.final_sql) result.append(el("pre", {class:"hint"}, data.final_sql.trim()));
    if (mode === "run") renderRows(result, data.rows, data.row_count);
    else result.append(el("pre", {}, data.plan || "(no plan)"));
  } catch (e) {
    status.textContent = "request failed"; status.className = "err";
    result.append(el("pre", {class:"err"}, String(e)));
  }
}

function renderRows(container, rows, count) {
  container.append(el("div", {class:"hint"}, `${count} row(s)` + (count === 500 ? " (capped at 500)" : "")));
  if (!rows || !rows.length) { container.append(el("div", {class:"hint"}, "(no rows)")); return; }
  const cols = Array.from(rows.reduce((s, r) => { Object.keys(r||{}).forEach(k => s.add(k)); return s; }, new Set()));
  const table = el("table", {});
  const thead = el("tr", {});
  cols.forEach(c => thead.append(el("th", {}, c)));
  table.append(thead);
  for (const row of rows) {
    const tr = el("tr", {});
    cols.forEach(c => {
      const v = row ? row[c] : null;
      const cell = el("td", {title: v == null ? "" : (typeof v === "object" ? JSON.stringify(v) : String(v))},
        v == null ? "∅" : (typeof v === "object" ? JSON.stringify(v) : String(v)));
      tr.append(cell);
    });
    table.append(tr);
  }
  container.append(table);
}

// ── benchmark ──────────────────────────────────────────────────
const OBJECTIVE_MS = 100;   // no query should be slower than this.
const BENCH_ITERS = 5;      // per-query iterations; the min is the warm floor.

async function runBenchmark() {
  document.querySelectorAll(".item").forEach(i => i.classList.remove("active"));
  current = null;
  const m = document.getElementById("main");
  m.innerHTML = "";
  m.append(el("h2", {}, "Benchmark", el("span", {class:"badge"}, `objective ≤ ${OBJECTIVE_MS} ms`)));
  m.append(el("div", {class:"meta"},
    `Runs every query ${BENCH_ITERS}× with its example params; reports the min (warm floor).`));
  const table = el("table", {});
  const head = el("tr", {});
  ["", "query", "db", "min ms", "med ms", "max ms", "result"].forEach(h => head.append(el("th", {}, h)));
  table.append(head);
  m.append(table);
  const progress = el("div", {class:"status", id:"benchProgress"}, "");
  m.append(progress);

  const btn = document.getElementById("benchBtn");
  btn.disabled = true;
  let pass = 0, ko = 0;
  for (let i = 0; i < CATALOG.length; i++) {
    const q = CATALOG[i];
    progress.textContent = `running ${i+1}/${CATALOG.length} · ${q.title}…`;
    const tr = el("tr", {});
    tr.append(el("td", {class:"num"}, String(i+1)));
    tr.append(el("td", {}, q.title));
    tr.append(el("td", {}, q.db));
    const body = {
      id: q.id, mode: "run",
      params: q.params.map(p => p.example),
      threshold: 0.3, work_mem: "256MB", timeout_ms: 0,
      iterations: BENCH_ITERS,
    };
    try {
      const r = await fetch("/debug/run", {method:"POST", headers:{"content-type":"application/json"}, body: JSON.stringify(body)});
      const data = await r.json();
      if (!data.ok) {
        tr.append(el("td", {class:"num"}, "—"));
        tr.append(el("td", {class:"num"}, "—"));
        tr.append(el("td", {class:"num"}, "—"));
        tr.append(el("td", {class:"ko", title: data.error}, "ERROR"));
        ko++;
      } else {
        const ok = data.min_ms <= OBJECTIVE_MS;
        if (ok) pass++; else ko++;
        tr.append(el("td", {class:"num"}, data.min_ms.toFixed(1)));
        tr.append(el("td", {class:"num"}, data.median_ms.toFixed(1)));
        tr.append(el("td", {class:"num"}, data.max_ms.toFixed(1)));
        tr.append(el("td", {class: ok ? "pass" : "ko"}, ok ? "OK" : "KO"));
      }
    } catch (e) {
      tr.append(el("td", {class:"num"}, "—"));
      tr.append(el("td", {class:"num"}, "—"));
      tr.append(el("td", {class:"num"}, "—"));
      tr.append(el("td", {class:"ko", title:String(e)}, "FAIL"));
      ko++;
    }
    table.append(tr);
  }
  progress.innerHTML = `done · <span class="ok">${pass} OK</span> · <span class="err">${ko} KO</span> (objective ≤ ${OBJECTIVE_MS} ms)`;
  btn.disabled = false;
}

document.getElementById("benchBtn").onclick = runBenchmark;
renderList();
</script>
</body>
</html>
"#;
