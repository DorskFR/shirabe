//! TheTVDB v4 source — lazy, per-query API fetch + cache (SHIB-7).
//!
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │ ATTRIBUTION / LICENSING — TheTVDB                                           │
//! │                                                                            │
//! │ Metadata provided by TheTVDB. Please consider adding missing information   │
//! │ or subscribing (https://thetvdb.com/subscribe). This deployment uses a     │
//! │ single licensed project API key (+ optional operator PIN) held strictly    │
//! │ server-side; the real key/PIN are NEVER re-exposed to clients. The `/v4`   │
//! │ facade mints its own Shirabe token instead.                                │
//! └──────────────────────────────────────────────────────────────────────────┘
//!
//! TheTVDB publishes no full data dump and no id export — the v4 API is the only
//! access path, and it is licensed. This is therefore an [`IngestMode::LazyScrape`]
//! source: nothing is enumerated up front. The `/v4` facade fetches a record from
//! the upstream v4 API on demand (with the in-memory bearer token obtained from the
//! server-side key) and caches the payload in `tvdb_cache` (dedicated `tvdb` DB).
//!
//! Auth: the v4 API uses a bearer token obtained from `POST {base}/login
//! {apikey, pin}`. The token is long-lived (~1 month). We hold it in memory
//! ([`TokenStore`]), shared between this source and the facade, and refresh it on
//! expiry or a 401. The real key/PIN never leave the server.
//!
//! `refresh()` for a LazyScrape source performs no bulk ingest; it simply verifies
//! that a login can be obtained (a cheap liveness probe) and records the result.
//! `health()` reports token validity + the `tvdb_cache` row count.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::RwLock;

use super::{IngestMode, RefreshCtx, RefreshReport, Source, SourceHealth};
use crate::config::Config;

/// Upstream TheTVDB v4 API base.
pub const API_BASE: &str = "https://api4.thetvdb.com/v4";

/// A bearer token obtained from TheTVDB `/login`, with the wall-clock second at
/// which we consider it expired. TheTVDB tokens live ~1 month; we refresh ahead of
/// the nominal lifetime to avoid using a token that expires mid-flight.
#[derive(Debug, Clone)]
pub struct Token {
    /// The opaque bearer string returned by `/login`.
    pub bearer: String,
    /// Unix second at/after which the token is treated as expired and refreshed.
    pub expires_at_secs: u64,
}

/// Nominal token lifetime: TheTVDB tokens are valid for roughly one month. We
/// expire ours a day early so a request never rides a token about to lapse.
pub const TOKEN_TTL_SECS: u64 = 30 * 86_400;
/// Refresh margin: treat a token as expired this many seconds before its nominal
/// expiry, so we never hand out a token on the very edge of lapsing.
pub const TOKEN_REFRESH_MARGIN_SECS: u64 = 86_400;

impl Token {
    /// Build a token valid for [`TOKEN_TTL_SECS`] from `now`.
    #[must_use]
    pub const fn new(bearer: String, now_secs: u64) -> Self {
        Self { bearer, expires_at_secs: now_secs.saturating_add(TOKEN_TTL_SECS) }
    }

    /// Should this token be refreshed at `now_secs`? True once we are within the
    /// refresh margin of (or past) the nominal expiry. Pure — unit-tested.
    #[must_use]
    pub const fn needs_refresh(&self, now_secs: u64) -> bool {
        now_secs.saturating_add(TOKEN_REFRESH_MARGIN_SECS) >= self.expires_at_secs
    }
}

/// Current wall-clock time in whole Unix seconds.
fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// In-memory, shared store of the current TheTVDB bearer token. Cloned cheaply
/// (it's an `Arc`) into both [`TvdbSource`] and the `/v4` facade via `AppState`.
#[derive(Clone, Default)]
pub struct TokenStore {
    inner: Arc<RwLock<Option<Token>>>,
}

/// The `/login` request body. The key/PIN come from server-side config, never the
/// client.
#[derive(Debug, serde::Serialize)]
struct LoginBody<'a> {
    apikey: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pin: Option<&'a str>,
}

/// The `/login` success response shape: `{ "data": { "token": "<jwt>" } }`.
#[derive(Debug, Deserialize)]
struct LoginResponse {
    data: LoginData,
}

#[derive(Debug, Deserialize)]
struct LoginData {
    token: String,
}

impl TokenStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a valid bearer, logging in (or refreshing) as needed. Errors when no
    /// server-side key is configured or the upstream login fails — callers map that
    /// to a clean failure response, never a panic.
    pub async fn bearer(&self, config: &Config) -> Result<String, String> {
        // Fast path: a still-fresh cached token under a read lock.
        let fresh = {
            let guard = self.inner.read().await;
            guard
                .as_ref()
                .filter(|token| !token.needs_refresh(now_secs()))
                .map(|token| token.bearer.clone())
        };
        if let Some(bearer) = fresh {
            return Ok(bearer);
        }
        self.refresh_locked(config).await
    }

    /// Force a fresh login (used on a 401 from upstream), replacing the cached
    /// token. Returns the new bearer.
    pub async fn force_refresh(&self, config: &Config) -> Result<String, String> {
        self.refresh_locked(config).await
    }

    /// Acquire the write lock, log in against TheTVDB, and store the new token.
    async fn refresh_locked(&self, config: &Config) -> Result<String, String> {
        let key = config
            .tvdb_api_key
            .as_deref()
            .ok_or_else(|| "TVDB_API_KEY is not configured".to_string())?;
        // Log in WITHOUT holding the lock (never hold a lock across an .await):
        // a rare concurrent double-login is harmless — last write wins and both
        // tokens are valid. The store is updated under a brief write lock.
        let bearer = login(API_BASE, key, config.tvdb_pin.as_deref()).await?;
        let token = Token::new(bearer.clone(), now_secs());
        *self.inner.write().await = Some(token);
        Ok(bearer)
    }

    /// Snapshot the current token (for `health()`), without triggering a login.
    pub async fn peek(&self) -> Option<Token> {
        self.inner.read().await.clone()
    }
}

/// Perform a `POST {base}/login {apikey, pin}` against TheTVDB and return the
/// bearer token. Network/parse failures surface as an error string.
async fn login(base: &str, apikey: &str, pin: Option<&str>) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("shirabe/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| format!("http client: {e}"))?;
    let url = format!("{base}/login");
    let body = serde_json::to_vec(&LoginBody { apikey, pin })
        .map_err(|e| format!("tvdb login body: {e}"))?;
    let resp = client
        .post(&url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| format!("tvdb login request: {e}"))?
        .error_for_status()
        .map_err(|e| format!("tvdb login status: {e}"))?;
    let bytes = resp.bytes().await.map_err(|e| format!("tvdb login body: {e}"))?;
    let parsed: LoginResponse =
        serde_json::from_slice(&bytes).map_err(|e| format!("tvdb login json: {e}"))?;
    Ok(parsed.data.token)
}

/// The TheTVDB lazy-scrape source. Holds the optional writable `tvdb` pool (for
/// the `health()` cache row count), the shared in-memory [`TokenStore`], and a
/// clone of the runtime config (for the key/PIN on a liveness login).
pub struct TvdbSource {
    pool: Option<PgPool>,
    tokens: TokenStore,
    config: Config,
}

impl TvdbSource {
    /// Source id / `shirabe.source.name` primary key.
    pub const ID: &'static str = "tvdb";

    #[must_use]
    pub const fn new(pool: Option<PgPool>, tokens: TokenStore, config: Config) -> Self {
        Self { pool, tokens, config }
    }
}

#[async_trait]
impl Source for TvdbSource {
    fn id(&self) -> &str {
        Self::ID
    }

    fn ingest_mode(&self) -> IngestMode {
        IngestMode::LazyScrape
    }

    /// A LazyScrape source ingests nothing in bulk. `refresh()` is a liveness
    /// probe: when a key is configured it verifies a login can be obtained;
    /// otherwise it records that the source is unconfigured (not an error — the
    /// operator simply has not supplied a key yet).
    async fn refresh(&self, _ctx: &RefreshCtx) -> RefreshReport {
        if self.config.tvdb_api_key.is_none() {
            return RefreshReport::ok(
                "tvdb is a lazy-scrape source; no bulk ingest. TVDB_API_KEY not set — \
                 /v4 will degrade gracefully until a key is configured",
            )
            .with_detail(json!({
                "ingest": "lazy_scrape",
                "configured": false,
                "attribution": "Metadata provided by TheTVDB (https://thetvdb.com).",
            }));
        }
        match self.tokens.bearer(&self.config).await {
            Ok(_) => RefreshReport::ok(
                "tvdb login verified; lazy-scrape source ready (records fetched on demand)",
            )
            .with_detail(json!({
                "ingest": "lazy_scrape",
                "configured": true,
                "login": "ok",
                "attribution": "Metadata provided by TheTVDB (https://thetvdb.com).",
            })),
            Err(e) => RefreshReport::failed(format!("tvdb login failed: {e}")),
        }
    }

    async fn health(&self) -> SourceHealth {
        let token_valid = self.tokens.peek().await.is_some_and(|t| !t.needs_refresh(now_secs()));
        let Some(pool) = self.pool.as_ref() else {
            return SourceHealth {
                source: self.id().to_string(),
                reachable: false,
                detail: "TVDB_DATABASE_URL is not set; tvdb cache unavailable".to_string(),
            };
        };
        match sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tvdb_cache").fetch_one(pool).await
        {
            Ok(n) => SourceHealth {
                source: self.id().to_string(),
                reachable: true,
                detail: format!("tvdb_cache reachable; {n} cached rows; token_valid={token_valid}"),
            },
            Err(e) => SourceHealth {
                source: self.id().to_string(),
                reachable: false,
                detail: format!("tvdb_cache unreachable: {e}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A freshly minted token is not due for refresh; one near or past its nominal
    /// expiry is. This is the login-token expiry/refresh decision.
    #[test]
    fn token_refresh_decision() {
        let now = 1_000_000_u64;
        let token = Token::new("jwt".to_string(), now);
        // Just minted: ~30 days of life, well outside the refresh margin.
        assert!(!token.needs_refresh(now));
        assert!(!token.needs_refresh(now + TOKEN_TTL_SECS - TOKEN_REFRESH_MARGIN_SECS - 1));
        // Within the refresh margin of expiry → refresh.
        assert!(token.needs_refresh(now + TOKEN_TTL_SECS - TOKEN_REFRESH_MARGIN_SECS));
        // Past nominal expiry → refresh.
        assert!(token.needs_refresh(now + TOKEN_TTL_SECS));
        assert!(token.needs_refresh(now + TOKEN_TTL_SECS + 10_000));
    }

    /// A token older than the refresh window (minted a month-plus ago) is expired.
    #[test]
    fn old_token_is_expired() {
        let minted = 1_000_000_u64;
        let token = Token::new("jwt".to_string(), minted);
        let now = minted + TOKEN_TTL_SECS + 1;
        assert!(token.needs_refresh(now));
    }
}
