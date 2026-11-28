//! `imdb-fts` search-table rebuild against a real migrated imdb DB. #[ignore]d;
//! run via `make test-integration`.

mod common;

use common::{NO_DB, provider_db};
use shirabe::db::Pools;
use shirabe::sources::imdb_index::{ImdbIndexSource, IndexSet};
use shirabe::sources::{RefreshCtx, Source};
use sqlx::PgPool;

async fn titles(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar("SELECT title_ua FROM imdb_search_titles ORDER BY title_ua")
        .fetch_all(pool)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "needs postgres (DATABASE_URL_TEST); run via make test-integration"]
async fn fts_rebuild_swaps_in_a_complete_table_and_keeps_the_old_one_on_failure() {
    let url = provider_db("imdb").await;
    let pool = shirabe::db::connect(&url, 2).await.unwrap();
    sqlx::query(
        "INSERT INTO imdb_title_basics (tconst, title_type, primary_title, original_title)
         VALUES ('tt0000001', 'movie', 'Dune Harbour', 'Dune Harbour')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let ctx = RefreshCtx {
        pools: Pools {
            musicbrainz: sqlx::postgres::PgPoolOptions::new().connect_lazy(NO_DB).unwrap(),
            shirabe: None,
            imdb: Some(pool.clone()),
            tmdb: None,
            tvdb: None,
            fanart: None,
        },
    };
    let source = ImdbIndexSource::new(Some(pool.clone()), IndexSet::Fts);

    let report = source.refresh(&ctx).await;
    assert!(report.ok, "{}", report.summary);
    assert_eq!(titles(&pool).await, vec!["Dune Harbour"]);
    assert!(source.health().await.reachable, "swapped-in table carries its canonical indexes");

    sqlx::query("INSERT INTO imdb_title_basics (tconst, title_type, primary_title) VALUES ('tt0000002', 'movie', 'Second')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE imdb_title_ratings RENAME TO imdb_title_ratings_gone")
        .execute(&pool)
        .await
        .unwrap();
    let report = source.refresh(&ctx).await;
    assert!(!report.ok, "build must fail without the ratings table");
    assert_eq!(titles(&pool).await, vec!["Dune Harbour"], "old table keeps serving");

    sqlx::query("ALTER TABLE imdb_title_ratings_gone RENAME TO imdb_title_ratings")
        .execute(&pool)
        .await
        .unwrap();
    let report = source.refresh(&ctx).await;
    assert!(report.ok, "{}", report.summary);
    assert_eq!(titles(&pool).await, vec!["Dune Harbour", "Second"]);
    assert!(source.health().await.reachable);
    pool.close().await;
}
