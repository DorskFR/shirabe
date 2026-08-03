//! Executes EVERY SQL statement in the src/queries.rs catalog against real
//! Postgres (MB fixture + migrated imdb/tmdb DBs) with the catalog's own example
//! params, so column-name/type drift fails here instead of as a runtime 500.
//! #[ignore]d; run via `make test-integration`.

mod common;

use common::{mb_fixture_db, provider_db};
use shirabe::queries::{self, Param, ParamType, TargetDb};
use shirabe::search::configure_search_session;
use sqlx::postgres::PgArguments;
use sqlx::{PgPool, Postgres};
use uuid::Uuid;

fn bind_example<'q>(
    q: sqlx::query::Query<'q, Postgres, PgArguments>,
    p: &Param,
) -> sqlx::query::Query<'q, Postgres, PgArguments> {
    let raw = p.example.trim();
    let null = p.nullable && raw.is_empty();
    match p.ty {
        ParamType::Text => {
            if null {
                q.bind(None::<String>)
            } else {
                q.bind(raw.to_string())
            }
        }
        ParamType::Int => {
            if null {
                q.bind(None::<i32>)
            } else {
                q.bind(raw.parse::<i32>().unwrap())
            }
        }
        ParamType::BigInt => {
            if null {
                q.bind(None::<i64>)
            } else {
                q.bind(raw.parse::<i64>().unwrap())
            }
        }
        ParamType::Uuid => {
            if null {
                q.bind(None::<Uuid>)
            } else {
                q.bind(Uuid::parse_str(raw).unwrap())
            }
        }
        ParamType::IntArray => q.bind(parse_array::<i32>(raw)),
        ParamType::BigIntArray => q.bind(parse_array::<i64>(raw)),
        ParamType::TextArray => q.bind(
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        ),
    }
}

fn parse_array<T: std::str::FromStr>(raw: &str) -> Vec<T>
where
    T::Err: std::fmt::Debug,
{
    raw.split(',').map(str::trim).filter(|s| !s.is_empty()).map(|s| s.parse().unwrap()).collect()
}

async fn seed_provider_dbs(imdb: &PgPool, tmdb: &PgPool) {
    sqlx::query(
        "INSERT INTO imdb_title_basics (tconst, title_type, primary_title, original_title)
         VALUES ('tt0000001', 'movie', 'Dune Harbour', 'Dune Harbour')",
    )
    .execute(imdb)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO imdb_title_akas (title_id, ordering, title) VALUES ('tt0000001', 1, '砂丘港')",
    )
    .execute(imdb)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!("../migrations/imdb/0004_imdb_search_titles.sql"))
        .execute(imdb)
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO tmdb_id_index (id, kind, name, popularity, adult)
         VALUES (438631, 'movie', 'Dune Harbour', 52.4, false)",
    )
    .execute(tmdb)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tmdb_cache (id, kind, payload)
         VALUES (438631, 'movie', '{\"external_ids\":{\"imdb_id\":\"tt0000001\"}}'::jsonb)",
    )
    .execute(tmdb)
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn every_catalog_statement_executes_against_real_schemas() {
    let mb = shirabe::db::connect(&mb_fixture_db().await, 2).await.unwrap();
    let imdb = shirabe::db::connect(&provider_db("imdb").await, 2).await.unwrap();
    let tmdb = shirabe::db::connect(&provider_db("tmdb").await, 2).await.unwrap();
    seed_provider_dbs(&imdb, &tmdb).await;

    for spec in queries::catalog() {
        let pool = match spec.db {
            TargetDb::Musicbrainz => &mb,
            TargetDb::Imdb => &imdb,
            TargetDb::Tmdb => &tmdb,
        };
        let mut q = sqlx::query(spec.sql);
        for p in &spec.params {
            q = bind_example(q, p);
        }
        let mut conn = pool.acquire().await.unwrap();
        if spec.trigram {
            configure_search_session(&mut conn, 0.3, "64MB").await.unwrap();
        }
        if let Err(e) = q.fetch_all(&mut *conn).await {
            panic!("catalog query `{}` failed to execute: {e}", spec.id);
        }
    }
}
