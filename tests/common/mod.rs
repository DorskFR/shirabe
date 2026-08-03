#![allow(dead_code)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use clap::Parser;
use serde_json::Value;
use shirabe::config::Cli;
use shirabe::db::{Pools, connect};
use shirabe::facades::coverart::CoverArtState;
use shirabe::sources::Registry;
use shirabe::sources::tvdb::TokenStore;
use shirabe::{AppState, build_router};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

/// A connection-refused URL: no DB behind it, fails fast and deterministically.
pub const NO_DB: &str = "postgres://shirabe:shirabe@127.0.0.1:1/none";

pub const ARTIST_MBID: &str = "11111111-1111-4111-8111-111111111111";
pub const RELEASE_GROUP_MBID: &str = "22222222-2222-4222-8222-222222222222";
pub const RELEASE_MBID: &str = "33333333-3333-4333-8333-333333333333";
pub const RELEASE2_MBID: &str = "44444444-4444-4444-8444-444444444444";
pub const RECORDING_MBID: &str = "66666666-6666-4666-8666-666666666666";
pub const ABSENT_MBID: &str = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";

pub fn state(extra_args: &[&str]) -> Arc<AppState> {
    state_with_db(NO_DB, extra_args)
}

pub fn state_with_db(musicbrainz_url: &str, extra_args: &[&str]) -> Arc<AppState> {
    let mut args = vec!["shirabe", "--database-url", musicbrainz_url];
    args.extend_from_slice(extra_args);
    let config = Cli::try_parse_from(args).expect("test cli args").config;
    // Short acquire timeout so the NO_DB (connection-refused) paths fail in
    // milliseconds instead of sqlx's 30s default retry window.
    let lazy = |url: &Option<String>| url.as_deref().map(lazy_pool);
    let pools = Pools {
        musicbrainz: lazy_pool(&config.database_url),
        shirabe: lazy(&config.shirabe_database_url),
        imdb: lazy(&config.imdb_database_url),
        tmdb: lazy(&config.tmdb_database_url),
        tvdb: lazy(&config.tvdb_database_url),
        fanart: lazy(&config.fanart_database_url),
    };
    let tvdb_tokens = TokenStore::new();
    let registry = Registry::with_defaults(pools.clone(), config.clone(), tvdb_tokens.clone());
    let coverart = CoverArtState::new(&config);
    Arc::new(AppState { pools, config, registry, tvdb_tokens, coverart })
}

fn lazy_pool(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(std::time::Duration::from_millis(500))
        .connect_lazy(url)
        .unwrap()
}

pub async fn send(state: &Arc<AppState>, method: &str, path: &str) -> Response {
    let req = Request::builder().method(method).uri(path).body(Body::empty()).unwrap();
    build_router(state.clone()).oneshot(req).await.unwrap()
}

pub async fn body_json(resp: Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("non-JSON body ({e}): {}", String::from_utf8_lossy(&bytes)))
}

pub async fn body_bytes(resp: Response) -> Vec<u8> {
    axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap().to_vec()
}

pub async fn get(state: &Arc<AppState>, path: &str) -> (StatusCode, Value) {
    let resp = send(state, "GET", path).await;
    let status = resp.status();
    (status, body_json(resp).await)
}

// ── DB-gated tier ──
//
// Gated tests are #[ignore]d and run via `make test-integration`, which sets
// DATABASE_URL_TEST to a THROWAWAY postgres with CREATE DATABASE rights. Each
// helper creates a uniquely-named database there; the app itself still only
// SELECTs from the MB fixture — only the fixture/migration setup writes.

pub fn admin_url() -> String {
    std::env::var("DATABASE_URL_TEST").expect(
        "DATABASE_URL_TEST must point at a throwaway postgres (run `make test-integration`)",
    )
}

fn with_db_name(url: &str, name: &str) -> String {
    let (base, query) = url.split_once('?').map_or((url, None), |(b, q)| (b, Some(q)));
    let cut = base.rfind('/').expect("postgres url with a path");
    let rebased = format!("{}/{name}", &base[..cut]);
    query.map_or_else(|| rebased.clone(), |q| format!("{rebased}?{q}"))
}

pub async fn create_test_db(prefix: &str) -> String {
    let admin = admin_url();
    let name = format!("shirabe_it_{prefix}_{}", uuid::Uuid::new_v4().simple());
    let pool = connect(&admin, 1).await.expect("connect DATABASE_URL_TEST");
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(&pool).await.expect("create db");
    pool.close().await;
    with_db_name(&admin, &name)
}

/// A fresh database carrying the musicbrainz-schema fixture subset + seed rows.
pub async fn mb_fixture_db() -> String {
    let url = create_test_db("mb").await;
    let pool = connect(&url, 1).await.unwrap();
    sqlx::raw_sql(include_str!("../fixtures/musicbrainz.sql"))
        .execute(&pool)
        .await
        .expect("apply musicbrainz fixture");
    pool.close().await;
    url
}

/// A fresh provider database (`shirabe`|`imdb`|`tmdb`|`tvdb`|`fanart`) with the
/// repo's real embedded migrations applied via `shirabe migrate <db>`.
pub async fn provider_db(kind: &str) -> String {
    let url = create_test_db(kind).await;
    let flag = format!("--{kind}-database-url");
    let args = ["shirabe", "--database-url", NO_DB, &flag, &url];
    let config = Cli::try_parse_from(args).unwrap().config;
    shirabe::migrate::run(&config, kind).await.expect("provider migrations");
    if kind == "imdb" {
        let pool = connect(&url, 1).await.unwrap();
        sqlx::raw_sql(include_str!("../../migrations/imdb/0004_imdb_search_titles.sql"))
            .execute(&pool)
            .await
            .expect("imdb search titles migration");
        pool.close().await;
    }
    url
}
