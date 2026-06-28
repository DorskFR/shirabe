//! Image/artwork caching via the existing `caache` proxy (SHIB-9).
//!
//! TMDB/TVDB posters and artwork cannot be bulk-dumped, so instead of fetching
//! image bytes ourselves we route the upstream image URLs through the `caache`
//! image proxy already running in the `media` namespace. `caache` exposes a
//! generic passthrough route `/_ia/<host>/<path>` which proxies to
//! `https://<host>/<path>` and caches the bytes. Rewriting an absolute upstream
//! image URL is therefore just: strip the scheme and prefix `{base}/_ia/`.
//!
//! ```text
//! https://image.tmdb.org/t/p/original/abc.jpg
//!   -> {base}/_ia/image.tmdb.org/t/p/original/abc.jpg
//! https://artworks.thetvdb.com/banners/xyz.jpg
//!   -> {base}/_ia/artworks.thetvdb.com/banners/xyz.jpg
//! ```
//!
//! Shirabe stays **stateless** on image bytes: we only rewrite the URL strings in
//! facade payloads; `caache` does the fetch + cache. The `shirabe.image_cache`
//! table (source, external_id, kind, remote_url, caache_url, fetched_at) exists but
//! is intentionally **reserved / unused** in this stateless design — it would only
//! be needed if we later wanted per-image URL bookkeeping.

/// The TMDB image base for absolute upstream URLs. TMDB returns relative paths
/// (e.g. `/abc.jpg`); prepend this to get the absolute upstream URL before
/// rewriting through caache. The `original` size is what Kusaritoi expects.
pub const TMDB_IMAGE_BASE: &str = "https://image.tmdb.org/t/p/original";

/// Build the absolute upstream TMDB image URL for a relative `poster_path`-style
/// value (e.g. `/abc.jpg`). Leading slashes are normalised so the result is always
/// `{TMDB_IMAGE_BASE}/abc.jpg`. An already-absolute http(s) value is returned as-is.
#[must_use]
pub fn tmdb_poster_url(path: &str) -> String {
    if path.starts_with("http://") || path.starts_with("https://") {
        return path.to_string();
    }
    format!("{TMDB_IMAGE_BASE}/{}", path.trim_start_matches('/'))
}

/// Rewrite an absolute upstream image URL to route through the `caache` proxy.
///
/// - `base` is the externally-reachable caache base (e.g. `https://caache.dorsk.dev`).
///   When it is empty, rewriting is **disabled** and the original URL is returned
///   unchanged (graceful no-op).
/// - Only `http(s)` URLs are rewritten; anything else (relative, data:, etc.) is
///   passed through untouched.
/// - A URL already pointing at `base` is passed through unchanged (idempotent).
#[must_use]
pub fn rewrite_through_caache(base: &str, upstream_absolute_url: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.is_empty() {
        return upstream_absolute_url.to_string();
    }
    // Already routed through caache → idempotent passthrough.
    if upstream_absolute_url.starts_with(base) {
        return upstream_absolute_url.to_string();
    }
    let host_and_path = if let Some(rest) = upstream_absolute_url.strip_prefix("https://") {
        rest
    } else if let Some(rest) = upstream_absolute_url.strip_prefix("http://") {
        rest
    } else {
        // Not an absolute http(s) URL — nothing to rewrite.
        return upstream_absolute_url.to_string();
    };
    format!("{base}/_ia/{host_and_path}")
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "https://caache.dorsk.dev";

    /// A TMDB relative path becomes an absolute upstream URL, then routes through
    /// caache under `image.tmdb.org`.
    #[test]
    fn rewrites_tmdb_relative_path() {
        let upstream = tmdb_poster_url("/abc.jpg");
        assert_eq!(upstream, "https://image.tmdb.org/t/p/original/abc.jpg");
        assert_eq!(
            rewrite_through_caache(BASE, &upstream),
            "https://caache.dorsk.dev/_ia/image.tmdb.org/t/p/original/abc.jpg"
        );
    }

    /// A TheTVDB absolute artworks URL routes through caache under
    /// `artworks.thetvdb.com`.
    #[test]
    fn rewrites_tvdb_absolute_url() {
        let upstream = "https://artworks.thetvdb.com/banners/xyz.jpg";
        assert_eq!(
            rewrite_through_caache(BASE, upstream),
            "https://caache.dorsk.dev/_ia/artworks.thetvdb.com/banners/xyz.jpg"
        );
    }

    /// An empty base disables rewriting — the original URL is returned unchanged.
    #[test]
    fn empty_base_is_noop() {
        let upstream = "https://image.tmdb.org/t/p/original/abc.jpg";
        assert_eq!(rewrite_through_caache("", upstream), upstream);
    }

    /// A URL already pointing at caache is passed through unchanged (idempotent).
    #[test]
    fn already_caache_passes_through() {
        let already = "https://caache.dorsk.dev/_ia/image.tmdb.org/t/p/original/abc.jpg";
        assert_eq!(rewrite_through_caache(BASE, already), already);
    }

    /// Non-http(s) values (relative paths, data URIs) are not touched.
    #[test]
    fn non_http_passes_through() {
        assert_eq!(rewrite_through_caache(BASE, "/relative/path.jpg"), "/relative/path.jpg");
        assert_eq!(
            rewrite_through_caache(BASE, "data:image/png;base64,AAAA"),
            "data:image/png;base64,AAAA"
        );
    }

    /// `tmdb_poster_url` leaves an already-absolute URL alone and normalises
    /// missing/extra leading slashes.
    #[test]
    fn tmdb_poster_url_handles_absolute_and_slashes() {
        assert_eq!(
            tmdb_poster_url("https://image.tmdb.org/t/p/original/x.jpg"),
            "https://image.tmdb.org/t/p/original/x.jpg"
        );
        assert_eq!(tmdb_poster_url("abc.jpg"), "https://image.tmdb.org/t/p/original/abc.jpg");
    }
}
