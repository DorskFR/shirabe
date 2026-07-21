//! Native Cover Art Archive proxy, folding in the standalone `caache`.
//!
//! Two layers, mirroring `caache`'s nginx behaviour so existing clients are
//! unchanged:
//!
//! - Redirect layer: `/release/{*}` and `/release-group/{*}` proxy to
//!   `coverartarchive.org`. CAA answers image requests with a 3xx to
//!   `archive.org`; the redirect is NOT followed server-side — its `Location` is
//!   rewritten to the local `/_ia/<host>/<path>` form and returned, so the client
//!   comes back through this proxy for the bytes.
//! - Byte layer: `/_ia/{host}/{*path}` streams from `https://<host>/<path>`,
//!   bouncing any further `archive.org` CDN redirect back through `/_ia/`, and
//!   caches the bytes on disk (30d positive, 6h negative) with single-flight per
//!   key and an `X-Cache-Status` header.
//!
//! SSRF: the byte layer is HTTPS-only and refuses any host that resolves to a
//! private, loopback, link-local, unique-local, or unspecified address — the
//! in-app equivalent of `caache`'s public-:443-only egress NetworkPolicy.

use std::collections::HashMap;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::extract::{OriginalUri, Path, State};
use axum::http::header::{CONTENT_TYPE, LOCATION};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::config::Config;

/// Header advertising cache disposition (`HIT` served from disk, `MISS` fetched).
const CACHE_STATUS: &str = "x-cache-status";

/// Runtime state for the Cover Art facade: the redirect-disabled HTTP client, the
/// on-disk byte cache parameters, and the per-key single-flight lock table.
pub struct CoverArtState {
    client: reqwest::Client,
    cache_dir: PathBuf,
    max_bytes: u64,
    positive_ttl: Duration,
    negative_ttl: Duration,
    upstream_base: String,
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl CoverArtState {
    /// Build the facade state from config, creating the cache directory.
    #[must_use]
    pub fn new(config: &Config) -> Self {
        let cache_dir = PathBuf::from(&config.coverart_cache_dir);
        let _ = std::fs::create_dir_all(&cache_dir);
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("shirabe/", env!("CARGO_PKG_VERSION")))
            .build()
            .unwrap_or_default();
        Self {
            client,
            cache_dir,
            max_bytes: config.coverart_cache_max_bytes,
            positive_ttl: Duration::from_secs(config.coverart_positive_ttl_secs),
            negative_ttl: Duration::from_secs(config.coverart_negative_ttl_secs),
            upstream_base: config.coverart_upstream_base.trim_end_matches('/').to_string(),
            locks: Mutex::new(HashMap::new()),
        }
    }

    fn paths(&self, key: &str) -> (PathBuf, PathBuf) {
        let stem = key_hash(key);
        (self.cache_dir.join(format!("{stem}.json")), self.cache_dir.join(format!("{stem}.bin")))
    }

    fn lock_for(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.locks.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        map.entry(key.to_string()).or_default().clone()
    }
}

/// On-disk cache metadata sidecar for a byte-cache entry.
#[derive(Serialize, Deserialize)]
struct Meta {
    status: u16,
    content_type: Option<String>,
    fetched_at: u64,
}

/// Build the Cover Art route group, merged in `build_router` alongside the other
/// facades.
pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/release/{*rest}", get(release))
        .route("/release-group/{*rest}", get(release_group))
        .route("/_ia/{host}/{*path}", get(ia_passthrough))
}

async fn release(state: State<Arc<AppState>>, uri: OriginalUri) -> Response {
    redirect_layer(&state, &uri).await
}

async fn release_group(state: State<Arc<AppState>>, uri: OriginalUri) -> Response {
    redirect_layer(&state, &uri).await
}

/// Proxy a `/release` or `/release-group` request to Cover Art Archive, rewriting
/// any redirect `Location` to the local `/_ia/` bounce and passing other responses
/// through unchanged.
async fn redirect_layer(state: &AppState, uri: &OriginalUri) -> Response {
    let ca = &state.coverart;
    let pq = uri.path_and_query().map_or_else(|| uri.path(), axum::http::uri::PathAndQuery::as_str);
    let url = format!("{}{}", ca.upstream_base, strip_mount_prefix(pq));

    let resp = match ca.client.get(&url).send().await {
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
    match resp.bytes().await {
        Ok(bytes) => body_response(status, ct.as_deref(), bytes.to_vec(), "MISS"),
        Err(e) => {
            tracing::warn!(error = %e, "coverart upstream body read failed");
            (StatusCode::BAD_GATEWAY, "upstream body error").into_response()
        }
    }
}

async fn ia_passthrough(
    State(state): State<Arc<AppState>>,
    Path((host, _path)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let ca = &state.coverart;
    let key =
        uri.path_and_query().map_or_else(|| uri.path(), axum::http::uri::PathAndQuery::as_str);

    if let Some((status, ct, body)) = cache_get(ca, key).await {
        return cached_response(status, ct.as_deref(), body);
    }

    if let Err(reason) = guard_host(&host).await {
        tracing::warn!(host, reason, "coverart /_ia host rejected");
        return (StatusCode::FORBIDDEN, "forbidden host").into_response();
    }

    let lock = ca.lock_for(key);
    let _guard = lock.lock().await;
    if let Some((status, ct, body)) = cache_get(ca, key).await {
        return cached_response(status, ct.as_deref(), body);
    }

    let path_rest = uri.path().strip_prefix("/_ia/").unwrap_or_else(|| uri.path());
    let mut url = format!("https://{path_rest}");
    if let Some(q) = uri.query() {
        url.push('?');
        url.push_str(q);
    }

    let resp = match ca.client.get(&url).send().await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::warn!(error = %e, url, "coverart /_ia upstream request failed");
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
    let bytes = match resp.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::warn!(error = %e, "coverart /_ia body read failed");
            return (StatusCode::BAD_GATEWAY, "upstream body error").into_response();
        }
    };
    let body = bytes.to_vec();
    if status == StatusCode::OK || status == StatusCode::NOT_FOUND {
        cache_put(ca, key, status.as_u16(), ct.as_deref(), &body).await;
    }
    body_response(status, ct.as_deref(), body, "MISS")
}

/// Read a fresh cache entry for `key`, or `None` on miss / stale / error.
async fn cache_get(ca: &CoverArtState, key: &str) -> Option<(StatusCode, Option<String>, Vec<u8>)> {
    let (meta_path, body_path) = ca.paths(key);
    let meta: Meta = serde_json::from_slice(&tokio::fs::read(meta_path).await.ok()?).ok()?;
    let age = now_secs().saturating_sub(meta.fetched_at);
    if !is_cache_fresh(meta.status, age, ca.positive_ttl.as_secs(), ca.negative_ttl.as_secs()) {
        return None;
    }
    let body = tokio::fs::read(body_path).await.unwrap_or_default();
    Some((StatusCode::from_u16(meta.status).unwrap_or(StatusCode::OK), meta.content_type, body))
}

/// Atomically store a cache entry (temp file + rename), then evict oldest entries
/// if the cache is over its byte budget. Best-effort: failures are logged only.
async fn cache_put(ca: &CoverArtState, key: &str, status: u16, ct: Option<&str>, body: &[u8]) {
    let (meta_path, body_path) = ca.paths(key);
    let meta = Meta { status, content_type: ct.map(ToString::to_string), fetched_at: now_secs() };
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
    if let Err(e) = tokio::task::spawn_blocking(move || evict_over_budget(&dir, max)).await {
        tracing::warn!(error = %e, "coverart cache eviction task failed");
    }
}

async fn write_atomic(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, bytes).await?;
    tokio::fs::rename(&tmp, path).await
}

/// Evict oldest (`.bin`, `.json`) entry pairs by mtime until the total on-disk
/// size is back under `max_bytes`.
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

/// SSRF guard: resolve `host` on :443 and reject if it has no public address, or
/// if it carries an explicit port. HTTPS is enforced by construction.
async fn guard_host(host: &str) -> Result<(), &'static str> {
    if host.contains(':') {
        return Err("explicit port not allowed");
    }
    let hostname = host.to_string();
    let resolved = tokio::task::spawn_blocking(move || {
        (hostname.as_str(), 443u16).to_socket_addrs().map(Iterator::collect::<Vec<_>>)
    })
    .await
    .map_err(|_| "resolver task failed")?
    .map_err(|_| "host did not resolve")?;
    if resolved.is_empty() {
        return Err("host did not resolve");
    }
    if resolved.iter().any(|addr| ip_is_blocked(addr.ip())) {
        return Err("host resolves to a non-public address");
    }
    Ok(())
}

/// Is `ip` in a range the byte layer must refuse (private, loopback, link-local,
/// unique-local, unspecified)?
#[must_use]
fn ip_is_blocked(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.octets()[0] == 0
        }
        IpAddr::V6(v6) => {
            if v6.is_loopback() || v6.is_unspecified() {
                return true;
            }
            if let Some(v4) = v6.to_ipv4_mapped() {
                return ip_is_blocked(IpAddr::V4(v4));
            }
            let head = v6.segments()[0];
            (head & 0xfe00) == 0xfc00 || (head & 0xffc0) == 0xfe80
        }
    }
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
        // IPv4-mapped loopback must be caught through the embedded address.
        assert!(ip_is_blocked(IpAddr::V6("::ffff:127.0.0.1".parse().unwrap())));
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
}
