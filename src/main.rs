use clap::Parser;
use shirabe::config::{Cli, Command};
use shirabe::db::Pools;
use shirabe::sources::Registry;
use shirabe::sources::tvdb::TokenStore;
use shirabe::{migrate, run_sync, serve};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer())
        .init();

    let cli = Cli::parse();
    let config = cli.config;

    // `migrate` short-circuits before the full pool bundle is built: it connects
    // only the one target DB itself (not the read-only mirror or the others), so an
    // empty fresh cluster can be bootstrapped without every URL being set.
    if let Some(Command::Migrate { db }) = &cli.command {
        return migrate::run(&config, db).await;
    }

    let pools = Pools::connect(
        &config.database_url,
        config.shirabe_database_url.as_deref(),
        config.imdb_database_url.as_deref(),
        config.tmdb_database_url.as_deref(),
        config.tvdb_database_url.as_deref(),
        config.fanart_database_url.as_deref(),
        config.db_pool_size,
    )
    .await?;
    let tvdb_tokens = TokenStore::new();
    let registry = Registry::with_defaults(pools.clone(), config.clone(), tvdb_tokens.clone());

    match cli.command {
        // CronJob entrypoint: refresh one source and exit.
        Some(Command::Sync { source }) => run_sync(&registry, &source).await,
        Some(Command::Migrate { .. }) => unreachable!("handled above"),
        // Default: start the HTTP server exactly as before.
        None => serve(config, pools, registry, tvdb_tokens).await,
    }
}
