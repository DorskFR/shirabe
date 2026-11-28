//! Native Cover Art Archive proxy, folding in the standalone `caache`.
//!
//! Two layers, mirroring `caache`'s nginx behaviour so existing clients are
//! unchanged:
//!
//! - Redirect layer: `/release/{*}` and `/release-group/{*}` proxy to
//!   `coverartarchive.org`. CAA answers image requests with a 3xx to
//!   `archive.org`; the redirect is NOT followed server-side — its `Location` is
//!   rewritten to the local `/_ia/<host>/<path>` form and returned, so the client
//!   comes back through this proxy for the bytes. Non-redirect 200/404 bodies
//!   (JSON manifests) are disk-cached with a short TTL.
//! - Byte layer: `/_ia/{host}/{*path}` streams from `https://<host>/<path>`,
//!   bouncing any further `archive.org` CDN redirect back through `/_ia/`, and
//!   caches the bytes on disk (30d positive, 6h negative) with single-flight per
//!   key and an `X-Cache-Status` header.
//!
//! SSRF: the byte layer only fetches over HTTPS from an allowlist of artwork
//! hosts (`archive.org` and its subdomains, `coverartarchive.org`, and the TMDB,
//! TVDB and fanart.tv image hosts), building the upstream URL from the vetted
//! host alone. Those names are still resolved through a guard that refuses any
//! non-public address, so the vetted addresses are the ones the socket connects
//! to. Responses carry `nosniff`, a sandboxing CSP and only image content types,
//! so nothing proxied can run on this origin.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::extract::{OriginalUri, Path, State};
use axum::http::header::{
    CONTENT_DISPOSITION, CONTENT_SECURITY_POLICY, CONTENT_TYPE, LOCATION, X_CONTENT_TYPE_OPTIONS,
};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::config::Config;
use crate::telemetry::client::TracedSend;

/// Header advertising cache disposition (`HIT` served from disk, `MISS` fetched).
const CACHE_STATUS: &str = "x-cache-status";

const MANIFEST_TTL_SECS: u64 = 300;
/// Largest upstream body the proxy will buffer, cache or relay.
pub const MAX_BODY_BYTES: u64 = 20 * 1024 * 1024;

/// Runtime state for the Cover Art facade: the redirect-disabled HTTP clients
/// (`ia_client` resolves through the public-only guard unless the insecure seam
/// is on), the on-disk byte cache parameters, and the per-key single-flight lock
/// table.
pub struct CoverArtState {
    client: reqwest::Client,
    ia_client: reqwest::Client,
    cache_dir: PathBuf,
    max_bytes: u64,
    positive_ttl: Duration,
    negative_ttl: Duration,
    upstream_base: String,
    insecure_ia: bool,
    locks: KeyedLocks,
}

struct KeyedLocks {
    map: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

struct KeyedLockGuard<'a> {
    locks: &'a KeyedLocks,
    key: String,
    entry: Arc<tokio::sync::Mutex<()>>,
    permit: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl KeyedLocks {
    fn new() -> Self {
        Self { map: Mutex::new(HashMap::new()) }
    }

    async fn acquire(&self, key: &str) -> KeyedLockGuard<'_> {
        let entry = {
            let mut map = self.map.lock().unwrap_or_else(PoisonError::into_inner);
            map.entry(key.to_string()).or_default().clone()
        };
        let permit = entry.clone().lock_owned().await;
        KeyedLockGuard { locks: self, key: key.to_string(), entry, permit: Some(permit) }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.lock().unwrap_or_else(PoisonError::into_inner).len()
    }
}

impl Drop for KeyedLockGuard<'_> {
    fn drop(&mut self) {
        drop(self.permit.take());
        let mut map = self.locks.map.lock().unwrap_or_else(PoisonError::into_inner);
        // strong_count == 2 means only the map and this guard still reference the entry.
        if let Some(cur) = map.get(&self.key)
            && Arc::ptr_eq(cur, &self.entry)
            && Arc::strong_count(cur) == 2
        {
            map.remove(&self.key);
        }
    }
}

impl CoverArtState {
    /// Build the facade state from config, creating the cache directory.
    #[must_use]
    pub fn new(config: &Config) -> Self {
        let cache_dir = PathBuf::from(&config.coverart_cache_dir);
        let _ = std::fs::create_dir_all(&cache_dir);
        let client = client_builder().build().unwrap_or_default();
        let ia_client = if config.coverart_insecure_ia {
            client.clone()
        } else {
            client_builder().dns_resolver(Arc::new(PublicOnlyResolver)).build().unwrap_or_default()
        };
        Self {
            client,
            ia_client,
            cache_dir,
            max_bytes: config.coverart_cache_max_bytes,
            positive_ttl: Duration::from_secs(config.coverart_positive_ttl_secs),
            negative_ttl: Duration::from_secs(config.coverart_negative_ttl_secs),
            upstream_base: config.coverart_upstream_base.trim_end_matches('/').to_string(),
            insecure_ia: config.coverart_insecure_ia,
            locks: KeyedLocks::new(),
        }
    }

    fn paths(&self, key: &str) -> (PathBuf, PathBuf) {
        let stem = key_hash(key);
        (self.cache_dir.join(format!("{stem}.json")), self.cache_dir.join(format!("{stem}.bin")))
    }
}

fn client_builder() -> reqwest::ClientBuilder {
    crate::http::client_builder()
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("shirabe/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
}

/// On-disk cache metadata sidecar for a byte-cache entry. `body_len` lets a
/// reader detect a sidecar paired with a body from a different write.
#[derive(Serialize, Deserialize)]
struct Meta {
    status: u16,
    content_type: Option<String>,
    fetched_at: u64,
    #[serde(default)]
    body_len: Option<u64>,
}

/// Build the Cover Art route group, merged in `build_router` alongside the other
/// facades.
pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/release/{*rest}", get(release))
        .route("/release-group/{*rest}", get(release_group))
        .route("/_ia/{host}/{*path}", get(ia_passthrough))
}

#[tracing::instrument(skip_all)]
async fn release(state: State<Arc<AppState>>, uri: OriginalUri) -> Response {
    redirect_layer(&state, &uri).await
}

#[tracing::instrument(skip_all)]
async fn release_group(state: State<Arc<AppState>>, uri: OriginalUri) -> Response {
    redirect_layer(&state, &uri).await
}

/// Proxy a `/release` or `/release-group` request to Cover Art Archive, rewriting
/// any redirect `Location` to the local `/_ia/` bounce and passing other responses
/// through unchanged.
#[tracing::instrument(skip_all, fields(coverart.key = tracing::field::Empty, cache.hit = tracing::field::Empty))]
async fn redirect_layer(state: &AppState, uri: &OriginalUri) -> Response {
    let ca = &state.coverart;
    let pq = uri.path_and_query().map_or_else(|| uri.path(), axum::http::uri::PathAndQuery::as_str);
    let key = strip_mount_prefix(pq);
    let span = tracing::Span::current();
    span.record("coverart.key", key);
    if let Some((status, ct, body)) = cache_get(ca, key, MANIFEST_TTL_SECS, MANIFEST_TTL_SECS).await
    {
        span.record("cache.hit", true);
        return cached_response(status, ct.as_deref(), body);
    }
    let _lease = ca.locks.acquire(key).await;
    if let Some((status, ct, body)) = cache_get(ca, key, MANIFEST_TTL_SECS, MANIFEST_TTL_SECS).await
    {
        span.record("cache.hit", true);
        return cached_response(status, ct.as_deref(), body);
    }
    span.record("cache.hit", false);
    let url = format!("{}{}", ca.upstream_base, key);

    let resp = match ca.client.get(&url).send_traced().await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(error = %e, url, "coverart upstream request failed");
            return (StatusCode::BAD_GATEWAY, "upstream error").into_response();
        }
    };
    let status = to_axum_status(resp.status());
    if status.is_redirection()
        && let Some(local) = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(rewrite_location_to_ia)
    {
        return redirect_response(status, &local);
    }
    let ct = resp_content_type(&resp);
    let body = match read_capped(resp, MAX_BODY_BYTES).await {
        Ok(body) => body,
        Err(e) => return e.into_response("coverart upstream"),
    };
    if status == StatusCode::OK || status == StatusCode::NOT_FOUND {
        cache_put(ca, key, status.as_u16(), ct.as_deref(), &body).await;
    }
    body_response(status, ct.as_deref(), body, "MISS")
}

#[tracing::instrument(skip_all, fields(server.address = %host, cache.hit = tracing::field::Empty))]
async fn ia_passthrough(
    State(state): State<Arc<AppState>>,
    Path((host, _path)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let mut resp = ia_fetch(&state.coverart, &host, &uri).await;
    let headers = resp.headers_mut();
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers
        .insert(CONTENT_SECURITY_POLICY, HeaderValue::from_static("default-src 'none'; sandbox"));
    headers.insert(CONTENT_DISPOSITION, HeaderValue::from_static("inline"));
    resp
}

async fn ia_fetch(ca: &CoverArtState, host: &str, uri: &axum::http::Uri) -> Response {
    let url = match ia_upstream_url(ia_rest(uri.path()), uri.query(), ca.insecure_ia) {
        Ok(url) => url,
        Err(reason) => {
            tracing::warn!(host, reason, "coverart /_ia host rejected");
            return (StatusCode::FORBIDDEN, "forbidden host").into_response();
        }
    };

    let key = ia_cache_key(uri.path(), uri.query());
    let pos = ca.positive_ttl.as_secs();
    let neg = ca.negative_ttl.as_secs();
    let span = tracing::Span::current();
    if let Some((status, ct, body)) = cache_get(ca, &key, pos, neg).await {
        span.record("cache.hit", true);
        return cached_response(status, Some(ia_content_type(ct.as_deref())), body);
    }

    let _lease = ca.locks.acquire(&key).await;
    if let Some((status, ct, body)) = cache_get(ca, &key, pos, neg).await {
        span.record("cache.hit", true);
        return cached_response(status, Some(ia_content_type(ct.as_deref())), body);
    }
    span.record("cache.hit", false);

    let resp = match ca.ia_client.get(url.clone()).send_traced().await {
        Ok(resp) => resp,
        Err(e) => {
            if let Some(reason) = blocked_host_reason(&e) {
                tracing::warn!(host, reason, "coverart /_ia host rejected");
                return (StatusCode::FORBIDDEN, "forbidden host").into_response();
            }
            tracing::warn!(error = %e, host, "coverart /_ia upstream request failed");
            return (StatusCode::BAD_GATEWAY, "upstream error").into_response();
        }
    };
    let status = to_axum_status(resp.status());
    if status.is_redirection()
        && let Some(local) = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(rewrite_location_to_ia)
    {
        return redirect_response(status, &local);
    }

    let ct = resp_content_type(&resp);
    let body = match read_capped(resp, MAX_BODY_BYTES).await {
        Ok(body) => body,
        Err(e) => return e.into_response("coverart /_ia"),
    };
    if status == StatusCode::OK || status == StatusCode::NOT_FOUND {
        cache_put(ca, &key, status.as_u16(), ct.as_deref(), &body).await;
    }
    body_response(status, Some(ia_content_type(ct.as_deref())), body, "MISS")
}

/// Upstream `Content-Type` if it is an image type, else `application/octet-stream`.
fn ia_content_type(upstream: Option<&str>) -> &str {
    match upstream {
        Some(ct) if ct.trim_start().get(..6).is_some_and(|p| p.eq_ignore_ascii_case("image/")) => {
            ct
        }
        _ => "application/octet-stream",
    }
}

enum BodyError {
    TooLarge(u64),
    Read(reqwest::Error),
}

impl BodyError {
    fn into_response(self, what: &str) -> Response {
        match self {
            Self::TooLarge(cap) => {
                tracing::warn!(cap, "{what} body exceeds size cap");
                (StatusCode::BAD_GATEWAY, "upstream body too large").into_response()
            }
            Self::Read(e) => {
                tracing::warn!(error = %e, "{what} body read failed");
                (StatusCode::BAD_GATEWAY, "upstream body error").into_response()
            }
        }
    }
}

/// Buffer the body, refusing a declared `Content-Length` over `cap` before
/// reading and aborting a chunked body as soon as it crosses `cap`.
#[tracing::instrument(skip_all, fields(http.body.cap = cap))]
async fn read_capped(mut resp: reqwest::Response, cap: u64) -> Result<Vec<u8>, BodyError> {
    if resp.content_length().is_some_and(|len| len > cap) {
        return Err(BodyError::TooLarge(cap));
    }
    let hint = resp.content_length().unwrap_or(0).min(cap);
    let mut body = Vec::with_capacity(usize::try_from(hint).unwrap_or(0));
    while let Some(chunk) = resp.chunk().await.map_err(BodyError::Read)? {
        if (body.len() as u64).saturating_add(chunk.len() as u64) > cap {
            return Err(BodyError::TooLarge(cap));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Read a fresh cache entry for `key`, or `None` on miss / stale / error.
#[tracing::instrument(skip_all, fields(cache.hit = tracing::field::Empty))]
async fn cache_get(
    ca: &CoverArtState,
    key: &str,
    positive_ttl: u64,
    negative_ttl: u64,
) -> Option<(StatusCode, Option<String>, Vec<u8>)> {
    let hit = cache_lookup(ca, key, positive_ttl, negative_ttl).await;
    tracing::Span::current().record("cache.hit", hit.is_some());
    hit
}

async fn cache_lookup(
    ca: &CoverArtState,
    key: &str,
    positive_ttl: u64,
    negative_ttl: u64,
) -> Option<(StatusCode, Option<String>, Vec<u8>)> {
    let (meta_path, body_path) = ca.paths(key);
    let meta: Meta = serde_json::from_slice(&tokio::fs::read(meta_path).await.ok()?).ok()?;
    let age = now_secs().saturating_sub(meta.fetched_at);
    if !is_cache_fresh(meta.status, age, positive_ttl, negative_ttl) {
        return None;
    }
    let body = tokio::fs::read(body_path).await.ok()?;
    if meta.body_len.is_some_and(|len| len != body.len() as u64) {
        return None;
    }
    Some((StatusCode::from_u16(meta.status).unwrap_or(StatusCode::OK), meta.content_type, body))
}

/// Atomically store a cache entry (temp file + rename), then evict oldest entries
/// if the cache is over its byte budget. Best-effort: failures are logged only.
#[tracing::instrument(skip_all, fields(http.response.status_code = status, http.body.size = body.len()))]
async fn cache_put(ca: &CoverArtState, key: &str, status: u16, ct: Option<&str>, body: &[u8]) {
    let (meta_path, body_path) = ca.paths(key);
    let meta = Meta {
        status,
        content_type: ct.map(ToString::to_string),
        fetched_at: now_secs(),
        body_len: Some(body.len() as u64),
    };
    let Ok(meta_bytes) = serde_json::to_vec(&meta) else {
        return;
    };
    if let Err(e) = write_atomic(&body_path, body).await {
        tracing::warn!(error = %e, "coverart cache body write failed");
        return;
    }
    if let Err(e) = write_atomic(&meta_path, &meta_bytes).await {
        tracing::warn!(error = %e, "coverart cache meta write failed");
        return;
    }
    let dir = ca.cache_dir.clone();
    let max = ca.max_bytes;
    let span = tracing::Span::current();
    let evict = move || span.in_scope(|| evict_over_budget(&dir, max));
    if let Err(e) = tokio::task::spawn_blocking(evict).await {
        tracing::warn!(error = %e, "coverart cache eviction task failed");
    }
}

/// Write through a per-call temp file so concurrent writers of one path never
/// share (or rename out from under) each other's partial output.
#[tracing::instrument(skip_all, fields(file.size = bytes.len()), err(level = "info"))]
async fn write_atomic(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = path.with_extension(format!("{}-{seq}.tmp", std::process::id()));
    if let Err(e) = tokio::fs::write(&tmp, bytes).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(e);
    }
    if let Err(e) = tokio::fs::rename(&tmp, path).await {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(e);
    }
    Ok(())
}

/// Evict oldest (`.bin`, `.json`) entry pairs by mtime until the total on-disk
/// size is back under `max_bytes`.
#[tracing::instrument(skip_all, fields(cache.max_bytes = max_bytes))]
fn evict_over_budget(dir: &std::path::Path, max_bytes: u64) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut total: u64 = 0;
    let mut stems: HashMap<String, (u64, SystemTime)> = HashMap::new();
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        total = total.saturating_add(meta.len());
        let path = entry.path();
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()).map(ToString::to_string) else {
            continue;
        };
        let mtime = meta.modified().unwrap_or(UNIX_EPOCH);
        let slot = stems.entry(stem).or_insert((0, mtime));
        slot.0 = slot.0.saturating_add(meta.len());
        if mtime < slot.1 {
            slot.1 = mtime;
        }
    }
    if total <= max_bytes {
        return;
    }
    let mut ordered: Vec<(String, u64, SystemTime)> =
        stems.into_iter().map(|(s, (len, mt))| (s, len, mt)).collect();
    ordered.sort_by_key(|(_, _, mt)| *mt);
    for (stem, len, _) in ordered {
        if total <= max_bytes {
            break;
        }
        let _ = std::fs::remove_file(dir.join(format!("{stem}.bin")));
        let _ = std::fs::remove_file(dir.join(format!("{stem}.json")));
        total = total.saturating_sub(len);
    }
}

/// Is a cache entry still fresh? 200s use the positive TTL, everything else (404
/// negatives) the negative TTL.
#[must_use]
const fn is_cache_fresh(status: u16, age_secs: u64, positive_ttl: u64, negative_ttl: u64) -> bool {
    let ttl = if status == 200 { positive_ttl } else { negative_ttl };
    age_secs <= ttl
}

/// `OriginalUri` keeps the `/coverart` nest prefix, which CAA upstream must not see.
#[must_use]
fn strip_mount_prefix(path_and_query: &str) -> &str {
    match path_and_query.strip_prefix("/coverart") {
        Some(rest) if rest.starts_with("/release") => rest,
        _ => path_and_query,
    }
}

/// `host/path` remainder of an `/_ia/` request path, whether root-mounted or
/// nested under `/coverart`.
#[must_use]
fn ia_rest(path: &str) -> &str {
    let path = match path.strip_prefix("/coverart") {
        Some(rest) if rest.starts_with("/_ia/") => rest,
        _ => path,
    };
    path.strip_prefix("/_ia/").unwrap_or(path)
}

/// Mount-invariant byte-cache key: canonical `/_ia/<host>/<path>[?<query>]`.
#[must_use]
fn ia_cache_key(path: &str, query: Option<&str>) -> String {
    let rest = ia_rest(path);
    query.map_or_else(|| format!("/_ia/{rest}"), |q| format!("/_ia/{rest}?{q}"))
}

/// Rewrite an absolute upstream redirect `Location` to the local `/_ia/` bounce so
/// the client fetches the bytes back through this proxy. Non-absolute values pass
/// through unchanged.
#[must_use]
fn rewrite_location_to_ia(location: &str) -> String {
    let host_and_path = if let Some(rest) = location.strip_prefix("https://") {
        rest
    } else if let Some(rest) = location.strip_prefix("http://") {
        rest
    } else {
        return location.to_string();
    };
    format!("/_ia/{host_and_path}")
}

/// A host the byte layer refuses to connect to.
#[derive(Debug)]
struct BlockedHost(&'static str);

impl std::fmt::Display for BlockedHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for BlockedHost {}

/// DNS resolver that refuses names with any non-public address. Installed in the
/// `/_ia` client so the connection uses exactly the vetted addresses.
struct PublicOnlyResolver;

impl Resolve for PublicOnlyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let addrs = resolve_public(name.as_str()).await?;
            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

/// Resolve `host` and return its addresses (port 0, hyper fills in the URL port)
/// unless any of them is blocked.
async fn resolve_public(host: &str) -> Result<Vec<SocketAddr>, BlockedHost> {
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, 0u16))
        .await
        .map_err(|_| BlockedHost("host did not resolve"))?
        .collect();
    if addrs.is_empty() {
        return Err(BlockedHost("host did not resolve"));
    }
    if addrs.iter().any(|addr| ip_is_blocked(addr.ip())) {
        return Err(BlockedHost("host resolves to a non-public address"));
    }
    Ok(addrs)
}

/// The guard reason if `e` was raised by [`PublicOnlyResolver`].
fn blocked_host_reason(e: &reqwest::Error) -> Option<&'static str> {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(err) = cur {
        if let Some(blocked) = err.downcast_ref::<BlockedHost>() {
            return Some(blocked.0);
        }
        cur = err.source();
    }
    None
}

/// Upstream hosts the byte layer may fetch from: where Cover Art Archive
/// redirects to and where the facades' rewritten artwork URLs point.
const IA_ALLOWED_HOSTS: &[&str] = &[
    "archive.org",
    "coverartarchive.org",
    "image.tmdb.org",
    "artworks.thetvdb.com",
    "assets.fanart.tv",
];

fn ia_host_allowed(host: &str) -> bool {
    IA_ALLOWED_HOSTS.contains(&host)
        || host.strip_suffix(".archive.org").is_some_and(|sub| !sub.is_empty())
}

/// Build the upstream URL for a raw `host/path` remainder from the vetted host
/// only: an allowlisted DNS name, no userinfo, no port, HTTPS. The insecure test
/// seam accepts any `host[:port]` over plain HTTP.
fn ia_upstream_url(
    rest: &str,
    query: Option<&str>,
    insecure: bool,
) -> Result<reqwest::Url, &'static str> {
    let (raw_host, path) = rest.split_once('/').unwrap_or((rest, ""));
    let host = raw_host.to_ascii_lowercase();
    if host.is_empty() {
        return Err("empty host");
    }
    let scheme = if insecure { "http" } else { "https" };
    if !insecure {
        if !host
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
        {
            return Err("invalid host");
        }
        if !ia_host_allowed(&host) {
            return Err("host not allowed");
        }
    }
    let mut url =
        reqwest::Url::parse(&format!("{scheme}://{host}/")).map_err(|_| "invalid host")?;
    url.set_path(path);
    url.set_query(query);
    if !insecure
        && (url.scheme() != "https"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
            || url.host_str() != Some(host.as_str()))
    {
        return Err("invalid host");
    }
    Ok(url)
}

/// Is `ip` outside the public unicast space the byte layer may reach?
#[must_use]
fn ip_is_blocked(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => ipv4_is_blocked(v4),
        IpAddr::V6(v6) => ipv6_is_blocked(v6),
    }
}

const fn ipv4_is_blocked(v4: Ipv4Addr) -> bool {
    let [a, b, c, _] = v4.octets();
    v4.is_private()
        || v4.is_loopback()
        || v4.is_link_local()
        || v4.is_unspecified()
        || v4.is_broadcast()
        || v4.is_multicast()
        || v4.is_documentation()
        || a == 0
        || (a == 100 && (b & 0xc0) == 64)
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 88 && c == 99)
        || (a == 198 && (b & 0xfe) == 18)
        || a >= 240
}

fn ipv6_is_blocked(v6: Ipv6Addr) -> bool {
    if v6.is_loopback() || v6.is_unspecified() || v6.is_multicast() {
        return true;
    }
    if let Some(v4) = embedded_ipv4(v6) {
        return ipv4_is_blocked(v4);
    }
    let seg = v6.segments();
    (seg[0] & 0xfe00) == 0xfc00
        || (seg[0] & 0xffc0) == 0xfe80
        || (seg[0] & 0xffc0) == 0xfec0
        || (seg[0] == 0x2001 && seg[1] == 0)
        || (seg[0] == 0x2001 && seg[1] == 0x0db8)
        || (seg[0] == 0x0064 && seg[1] == 0xff9b && seg[2] == 1)
        || (seg[0] == 0x0100 && seg[1] == 0 && seg[2] == 0 && seg[3] == 0)
}

/// The IPv4 address carried by a mapped (`::ffff:a.b.c.d`), compatible
/// (`::a.b.c.d`), NAT64 (`64:ff9b::a.b.c.d`) or 6to4 (`2002:abcd::`) address.
fn embedded_ipv4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    let seg = v6.segments();
    let tail = |hi: u16, lo: u16| Ipv4Addr::from((u32::from(hi) << 16) | u32::from(lo));
    if let Some(v4) = v6.to_ipv4_mapped() {
        return Some(v4);
    }
    if seg[..6] == [0, 0, 0, 0, 0, 0] {
        return Some(tail(seg[6], seg[7]));
    }
    if seg[..6] == [0x0064, 0xff9b, 0, 0, 0, 0] {
        return Some(tail(seg[6], seg[7]));
    }
    if seg[0] == 0x2002 {
        return Some(tail(seg[1], seg[2]));
    }
    None
}

fn to_axum_status(status: reqwest::StatusCode) -> StatusCode {
    StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY)
}

fn resp_content_type(resp: &reqwest::Response) -> Option<String> {
    resp.headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(ToString::to_string)
}

fn redirect_response(status: StatusCode, location: &str) -> Response {
    let mut resp = (status, Body::empty()).into_response();
    if let Ok(value) = HeaderValue::from_str(location) {
        resp.headers_mut().insert(LOCATION, value);
    }
    resp.headers_mut().insert(CACHE_STATUS, HeaderValue::from_static("MISS"));
    resp
}

fn cached_response(status: StatusCode, content_type: Option<&str>, body: Vec<u8>) -> Response {
    let mut resp = (status, body).into_response();
    apply_content_type(&mut resp, content_type);
    resp.headers_mut().insert(CACHE_STATUS, HeaderValue::from_static("HIT"));
    resp
}

fn body_response(
    status: StatusCode,
    content_type: Option<&str>,
    body: Vec<u8>,
    cache_status: &'static str,
) -> Response {
    let mut resp = (status, body).into_response();
    apply_content_type(&mut resp, content_type);
    resp.headers_mut().insert(CACHE_STATUS, HeaderValue::from_static(cache_status));
    resp
}

fn apply_content_type(resp: &mut Response, content_type: Option<&str>) {
    if let Some(ct) = content_type
        && let Ok(value) = HeaderValue::from_str(ct)
    {
        resp.headers_mut().insert(CONTENT_TYPE, value);
    }
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or_default()
}

/// 128-bit FNV-1a over `key`, hex-encoded — a filesystem-safe, collision-resilient
/// cache stem.
fn key_hash(key: &str) -> String {
    let mut lo: u64 = 0xcbf2_9ce4_8422_2325;
    let mut hi: u64 = 0x8422_2325_cbf2_9ce4;
    for b in key.as_bytes() {
        lo ^= u64::from(*b);
        lo = lo.wrapping_mul(0x0000_0100_0000_01b3);
        hi ^= u64::from(*b).rotate_left(3);
        hi = hi.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{lo:016x}{hi:016x}")
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn strips_nest_prefix_only_before_release_paths() {
        assert_eq!(strip_mount_prefix("/coverart/release/m/front-500"), "/release/m/front-500");
        assert_eq!(strip_mount_prefix("/coverart/release-group/m?x=1"), "/release-group/m?x=1");
        assert_eq!(strip_mount_prefix("/release/m/front-500"), "/release/m/front-500");
        assert_eq!(strip_mount_prefix("/coverartist/x"), "/coverartist/x");
    }

    #[test]
    fn rewrites_caa_location_to_local_ia() {
        assert_eq!(
            rewrite_location_to_ia("https://archive.org/download/mbid-x/front.jpg"),
            "/_ia/archive.org/download/mbid-x/front.jpg"
        );
        assert_eq!(
            rewrite_location_to_ia("http://ia800000.us.archive.org/12/items/x/y.jpg"),
            "/_ia/ia800000.us.archive.org/12/items/x/y.jpg"
        );
    }

    #[test]
    fn rewrite_location_preserves_query_and_passes_relative() {
        assert_eq!(
            rewrite_location_to_ia("https://host/path?sig=abc&e=1"),
            "/_ia/host/path?sig=abc&e=1"
        );
        assert_eq!(rewrite_location_to_ia("/already/relative"), "/already/relative");
    }

    #[test]
    fn public_ips_allowed() {
        assert!(!ip_is_blocked(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
        assert!(!ip_is_blocked(IpAddr::V4(Ipv4Addr::new(207, 241, 224, 2))));
        assert!(!ip_is_blocked(IpAddr::V6("2606:4700:4700::1111".parse().unwrap())));
    }

    #[test]
    fn private_and_local_ips_blocked() {
        assert!(ip_is_blocked(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(ip_is_blocked(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5))));
        assert!(ip_is_blocked(IpAddr::V4(Ipv4Addr::new(172, 16, 3, 4))));
        assert!(ip_is_blocked(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))));
        assert!(ip_is_blocked(IpAddr::V4(Ipv4Addr::new(169, 254, 10, 1))));
        assert!(ip_is_blocked(IpAddr::V4(Ipv4Addr::UNSPECIFIED)));
        assert!(ip_is_blocked(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        assert!(ip_is_blocked(IpAddr::V6(Ipv6Addr::UNSPECIFIED)));
        assert!(ip_is_blocked(IpAddr::V6("fc00::1".parse().unwrap())));
        assert!(ip_is_blocked(IpAddr::V6("fd12:3456::1".parse().unwrap())));
        assert!(ip_is_blocked(IpAddr::V6("fe80::1".parse().unwrap())));
        assert!(ip_is_blocked(IpAddr::V6("::ffff:127.0.0.1".parse().unwrap())));
    }

    #[test]
    fn reserved_special_purpose_v4_ranges_blocked() {
        for ip in [
            "0.1.2.3",
            "100.64.0.1",
            "100.127.255.254",
            "192.0.0.8",
            "192.0.2.1",
            "192.88.99.1",
            "198.18.0.1",
            "198.19.255.255",
            "198.51.100.7",
            "203.0.113.9",
            "224.0.0.1",
            "239.255.255.250",
            "240.0.0.1",
            "255.255.255.255",
        ] {
            assert!(ip_is_blocked(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["100.63.255.255", "100.128.0.1", "192.0.1.1", "198.17.255.255", "198.20.0.1"] {
            assert!(!ip_is_blocked(ip.parse().unwrap()), "{ip}");
        }
    }

    #[test]
    fn tunnel_and_translation_v6_prefixes_blocked_by_embedded_v4() {
        for ip in [
            "::ffff:10.0.0.1",
            "::ffff:100.64.1.1",
            "::10.0.0.1",
            "::7f00:1",
            "64:ff9b::7f00:1",
            "64:ff9b::192.168.0.1",
            "64:ff9b:1::1",
            "2002:7f00:1::1",
            "2002:c0a8:101::1",
            "2001::1",
            "2001:db8::1",
            "fec0::1",
            "ff02::1",
            "ff0e::1",
            "100::1",
        ] {
            assert!(ip_is_blocked(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["::ffff:1.1.1.1", "64:ff9b::101:101", "2002:101:101::1", "2001:4860:4860::8888"]
        {
            assert!(!ip_is_blocked(ip.parse().unwrap()), "{ip}");
        }
    }

    #[test]
    fn ia_upstream_url_only_dials_allowlisted_names() {
        for rest in [
            "",
            "/x.jpg",
            "archive.org:443/x.jpg",
            "x@127.0.0.1/x.jpg",
            "x@archive.org/x.jpg",
            "archive.org@evil.example/x.jpg",
            "x%40127.0.0.1/x.jpg",
            "2130706433/x.jpg",
            "127.1/x.jpg",
            "0x7f.0.0.1/x.jpg",
            "10.1/x.jpg",
            "[::1]/x.jpg",
            "127.0.0.1/x.jpg",
            "1.1.1.1/x.jpg",
            "localhost/x.jpg",
            "evil.example/x.jpg",
            "archive.org.evil.example/x.jpg",
            "evilarchive.org/x.jpg",
            ".archive.org/x.jpg",
        ] {
            assert!(ia_upstream_url(rest, None, false).is_err(), "{rest}");
        }
        for (rest, want) in [
            ("archive.org/download/x/front.jpg", "https://archive.org/download/x/front.jpg"),
            (
                "ia800000.us.archive.org/12/items/x.jpg",
                "https://ia800000.us.archive.org/12/items/x.jpg",
            ),
            ("coverartarchive.org/release/m/front", "https://coverartarchive.org/release/m/front"),
            ("image.tmdb.org/t/p/original/a.jpg", "https://image.tmdb.org/t/p/original/a.jpg"),
            ("artworks.thetvdb.com/banners/b.jpg", "https://artworks.thetvdb.com/banners/b.jpg"),
            ("Assets.Fanart.TV/fanart/c.jpg", "https://assets.fanart.tv/fanart/c.jpg"),
        ] {
            assert_eq!(ia_upstream_url(rest, None, false).unwrap().as_str(), want, "{rest}");
        }
        assert_eq!(
            ia_upstream_url("archive.org/a@b/c.jpg", Some("x=1"), false).unwrap().as_str(),
            "https://archive.org/a@b/c.jpg?x=1"
        );
    }

    #[test]
    fn ia_content_type_passes_only_images() {
        assert_eq!(ia_content_type(Some("image/jpeg")), "image/jpeg");
        assert_eq!(ia_content_type(Some("IMAGE/PNG")), "IMAGE/PNG");
        for ct in [Some("text/html"), Some("application/json"), Some("imagex/y"), Some("img"), None]
        {
            assert_eq!(ia_content_type(ct), "application/octet-stream", "{ct:?}");
        }
    }

    #[tokio::test]
    async fn resolver_refuses_names_with_a_non_public_address() {
        let err = resolve_public("localhost").await.expect_err("localhost must be blocked");
        assert_eq!(err.0, "host resolves to a non-public address");
        assert!(resolve_public("no-such-host.invalid").await.is_err());
    }

    #[tokio::test]
    async fn ia_client_resolver_error_surfaces_as_blocked_host() {
        let client = client_builder().dns_resolver(Arc::new(PublicOnlyResolver)).build().unwrap();
        let err = client.get("https://localhost/x.jpg").send().await.expect_err("blocked");
        assert_eq!(blocked_host_reason(&err), Some("host resolves to a non-public address"));
        let plain = client_builder().build().unwrap();
        let err = plain.get("http://127.0.0.1:1/x").send().await.expect_err("refused");
        assert_eq!(blocked_host_reason(&err), None);
    }

    #[tokio::test]
    async fn concurrent_writers_never_interleave_or_lose_the_file() {
        let dir = std::env::temp_dir().join(format!("shirabe-coverart-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = Arc::new(dir.join("entry.bin"));
        let bodies: Vec<Vec<u8>> = (0u8..16).map(|i| vec![i; 64 * 1024 + usize::from(i)]).collect();
        let tasks: Vec<_> = bodies
            .iter()
            .cloned()
            .map(|body| {
                let path = Arc::clone(&path);
                tokio::spawn(async move { write_atomic(&path, &body).await })
            })
            .collect();
        for task in tasks {
            task.await.unwrap().expect("every write succeeds");
        }
        let written = std::fs::read(&*path).unwrap();
        assert!(bodies.contains(&written), "file must equal exactly one writer's body");
        let leftovers: Vec<_> = std::fs::read_dir(&dir).unwrap().flatten().collect();
        assert_eq!(leftovers.len(), 1, "no temp files left behind");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn cache_get_misses_when_meta_and_body_disagree() {
        let dir = std::env::temp_dir().join(format!("shirabe-coverart-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca = CoverArtState {
            client: crate::http::client(),
            ia_client: crate::http::client(),
            cache_dir: dir.clone(),
            max_bytes: 1_048_576,
            positive_ttl: Duration::from_secs(60),
            negative_ttl: Duration::from_secs(60),
            upstream_base: "https://example.invalid".to_string(),
            insecure_ia: false,
            locks: KeyedLocks::new(),
        };
        cache_put(&ca, "/_ia/h/x", 200, Some("image/png"), b"new-body").await;
        assert!(cache_get(&ca, "/_ia/h/x", 60, 60).await.is_some());
        let (_, body_path) = ca.paths("/_ia/h/x");
        std::fs::write(&body_path, b"stale-body-from-older-write").unwrap();
        assert!(cache_get(&ca, "/_ia/h/x", 60, 60).await.is_none());
        std::fs::remove_file(&body_path).unwrap();
        assert!(cache_get(&ca, "/_ia/h/x", 60, 60).await.is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn cache_ttl_positive_and_negative() {
        let day = 86_400;
        let pos = 30 * day;
        let neg = 6 * 3_600;
        assert!(is_cache_fresh(200, 0, pos, neg));
        assert!(is_cache_fresh(200, pos, pos, neg));
        assert!(!is_cache_fresh(200, pos + 1, pos, neg));
        assert!(is_cache_fresh(404, neg, pos, neg));
        assert!(!is_cache_fresh(404, neg + 1, pos, neg));
    }

    #[test]
    fn cache_key_is_stable_and_distinct() {
        assert_eq!(key_hash("/_ia/archive.org/a.jpg"), key_hash("/_ia/archive.org/a.jpg"));
        assert_ne!(key_hash("/_ia/archive.org/a.jpg"), key_hash("/_ia/archive.org/b.jpg"));
        assert_eq!(key_hash("/_ia/archive.org/a.jpg").len(), 32);
    }

    #[test]
    fn ia_rest_strips_both_mount_forms() {
        assert_eq!(
            ia_rest("/_ia/archive.org/download/x/front.jpg"),
            "archive.org/download/x/front.jpg"
        );
        assert_eq!(
            ia_rest("/coverart/_ia/archive.org/download/x/front.jpg"),
            "archive.org/download/x/front.jpg"
        );
        assert_eq!(ia_rest("/coverartist/_ia/x"), "/coverartist/_ia/x");
        assert_eq!(ia_rest("/coverart/release/x"), "/coverart/release/x");
    }

    #[test]
    fn ia_cache_key_is_mount_invariant() {
        let root = ia_cache_key("/_ia/archive.org/x.jpg", None);
        let nested = ia_cache_key("/coverart/_ia/archive.org/x.jpg", None);
        assert_eq!(root, "/_ia/archive.org/x.jpg");
        assert_eq!(root, nested);
        assert_eq!(
            ia_cache_key("/coverart/_ia/host/p", Some("sig=1")),
            ia_cache_key("/_ia/host/p", Some("sig=1"))
        );
        assert_eq!(ia_cache_key("/_ia/host/p", Some("sig=1")), "/_ia/host/p?sig=1");
        assert_ne!(ia_cache_key("/_ia/host/p", Some("sig=1")), ia_cache_key("/_ia/host/p", None));
    }

    #[tokio::test]
    async fn keyed_locks_prune_on_release() {
        let locks = KeyedLocks::new();
        {
            let _lease = locks.acquire("k").await;
            assert_eq!(locks.len(), 1);
        }
        assert_eq!(locks.len(), 0);
    }

    #[tokio::test]
    async fn keyed_locks_survive_while_contended() {
        let locks = Arc::new(KeyedLocks::new());
        let lease = locks.acquire("k").await;
        let contender = {
            let locks = Arc::clone(&locks);
            tokio::spawn(async move {
                let _lease = locks.acquire("k").await;
            })
        };
        while Arc::strong_count(&lease.entry) < 3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(locks.len(), 1);
        drop(lease);
        contender.await.unwrap();
        assert_eq!(locks.len(), 0);
    }

    #[tokio::test]
    async fn manifest_cache_round_trip_is_mount_invariant() {
        let dir = std::env::temp_dir().join(format!("shirabe-coverart-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca = CoverArtState {
            client: crate::http::client(),
            ia_client: crate::http::client(),
            cache_dir: dir.clone(),
            max_bytes: 1_048_576,
            positive_ttl: Duration::from_secs(60),
            negative_ttl: Duration::from_secs(60),
            upstream_base: "https://example.invalid".to_string(),
            insecure_ia: false,
            locks: KeyedLocks::new(),
        };
        let key = strip_mount_prefix("/coverart/release/mbid-1");
        cache_put(&ca, key, 200, Some("application/json"), b"{\"images\":[]}").await;
        let hit = cache_get(
            &ca,
            strip_mount_prefix("/release/mbid-1"),
            MANIFEST_TTL_SECS,
            MANIFEST_TTL_SECS,
        )
        .await
        .expect("cached manifest");
        assert_eq!(hit.0, StatusCode::OK);
        assert_eq!(hit.1.as_deref(), Some("application/json"));
        assert_eq!(hit.2, b"{\"images\":[]}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
