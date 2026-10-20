//! In-process response cache for the ws/2 search endpoints, with single-flight
//! collapsing of concurrent identical searches.
//!
//! Lidarr retries a slow search several times and fans the same query out
//! across its own workers, so one expensive trigram search can be running
//! N times at once against the mirror. Every search is keyed by its normalised
//! inputs; a hit serves the serialised JSON bytes, a miss runs the DB search
//! once while identical concurrent callers wait for that result. Only
//! successful responses are stored; errors propagate and are never cached.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use tokio::time::Instant;

/// Normalised identity of one search request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SearchKey {
    pub entity: &'static str,
    pub query: String,
    pub limit: i64,
    pub offset: i64,
    pub fmt: String,
}

impl SearchKey {
    /// Build a key, folding the query the same way Postgres does for scoring:
    /// trimmed, lower-cased, inner whitespace runs collapsed. `pg_trgm`,
    /// `to_tsvector('simple')` and `f_unaccent` are all case-insensitive and
    /// split on whitespace, so two queries that fold to the same key produce
    /// byte-identical responses.
    #[must_use]
    pub fn new(
        entity: &'static str,
        query: &str,
        limit: i64,
        offset: i64,
        fmt: Option<&str>,
    ) -> Self {
        let query = query.split_whitespace().map(str::to_lowercase).collect::<Vec<_>>().join(" ");
        let fmt = fmt.unwrap_or("json").trim().to_lowercase();
        Self { entity, query, limit, offset, fmt }
    }
}

/// One in-flight search: the leader parks its result here for the waiters.
type Slot = tokio::sync::Mutex<Option<Bytes>>;

struct Entry {
    body: Bytes,
    expires_at: Instant,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<SearchKey, Entry>,
    order: VecDeque<SearchKey>,
    inflight: HashMap<SearchKey, Arc<Slot>>,
}

/// Bounded TTL cache of serialised search responses. Cloning shares the store.
#[derive(Clone)]
pub struct SearchCache {
    inner: Arc<Mutex<Inner>>,
    ttl: Duration,
    max_entries: usize,
}

impl SearchCache {
    /// A TTL of zero or a zero capacity disables storage; single-flight still
    /// collapses concurrent identical searches into one DB round-trip.
    #[must_use]
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self { inner: Arc::new(Mutex::new(Inner::default())), ttl, max_entries }
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        !self.ttl.is_zero() && self.max_entries > 0
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn get(&self, key: &SearchKey) -> Option<Bytes> {
        if !self.enabled() {
            return None;
        }
        let mut inner = self.inner.lock().unwrap();
        match inner.entries.get(key) {
            Some(e) if e.expires_at > Instant::now() => Some(e.body.clone()),
            Some(_) => {
                inner.entries.remove(key);
                None
            }
            None => None,
        }
    }

    pub fn insert(&self, key: SearchKey, body: Bytes) {
        if !self.enabled() {
            return;
        }
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        if inner.entries.len() >= self.max_entries {
            inner.entries.retain(|_, e| e.expires_at > now);
        }
        while inner.entries.len() >= self.max_entries {
            match inner.order.pop_front() {
                Some(oldest) => {
                    inner.entries.remove(&oldest);
                }
                None => break,
            }
        }
        inner.order.retain(|k| k != &key);
        inner.order.push_back(key.clone());
        inner.entries.insert(key, Entry { body, expires_at: now + self.ttl });
    }

    /// Serve `key` from cache, or run `compute` once for all concurrent callers
    /// of the same key. A failed compute is not cached: waiters re-run it
    /// themselves, one at a time.
    pub async fn get_or_compute<F, Fut, E>(&self, key: SearchKey, compute: F) -> Result<Bytes, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Bytes, E>>,
    {
        if let Some(hit) = self.get(&key) {
            return Ok(hit);
        }
        let slot = {
            let mut inner = self.inner.lock().unwrap();
            inner.inflight.entry(key.clone()).or_default().clone()
        };
        let mut guard = slot.lock().await;
        let shared = guard.clone().or_else(|| self.get(&key));
        if let Some(hit) = shared {
            drop(guard);
            self.release_inflight(&key, &slot);
            return Ok(hit);
        }
        let result = compute().await;
        if let Ok(body) = &result {
            *guard = Some(body.clone());
            self.insert(key.clone(), body.clone());
        }
        drop(guard);
        self.release_inflight(&key, &slot);
        result
    }

    fn release_inflight(&self, key: &SearchKey, slot: &Arc<Slot>) {
        let mut inner = self.inner.lock().unwrap();
        // The map's own clone plus ours: nobody else is waiting on this key.
        if Arc::strong_count(slot) <= 2 {
            inner.inflight.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn key(q: &str) -> SearchKey {
        SearchKey::new("release", q, 25, 0, None)
    }

    #[test]
    fn key_normalises_case_whitespace_and_fmt() {
        let a = SearchKey::new("release", "  Harbour   LIGHTS ", 25, 0, None);
        let b = SearchKey::new("release", "harbour lights", 25, 0, Some("JSON"));
        assert_eq!(a, b);
        assert_ne!(a, SearchKey::new("artist", "harbour lights", 25, 0, None));
        assert_ne!(a, SearchKey::new("release", "harbour lights", 10, 0, None));
        assert_ne!(a, SearchKey::new("release", "harbour lights", 25, 25, None));
        assert_ne!(a, SearchKey::new("release", "harbour lights", 25, 0, Some("xml")));
    }

    #[tokio::test]
    async fn entries_expire_after_ttl() {
        let cache = SearchCache::new(Duration::from_millis(40), 10);
        cache.insert(key("a"), Bytes::from_static(b"1"));
        assert_eq!(cache.get(&key("a")).as_deref(), Some(&b"1"[..]));
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(cache.get(&key("a")).is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn capacity_evicts_oldest_insert() {
        let cache = SearchCache::new(Duration::from_secs(60), 2);
        cache.insert(key("a"), Bytes::from_static(b"1"));
        cache.insert(key("b"), Bytes::from_static(b"2"));
        cache.insert(key("c"), Bytes::from_static(b"3"));
        assert_eq!(cache.len(), 2);
        assert!(cache.get(&key("a")).is_none());
        assert!(cache.get(&key("b")).is_some());
        assert!(cache.get(&key("c")).is_some());
    }

    #[test]
    fn zero_ttl_or_capacity_disables_storage() {
        for cache in
            [SearchCache::new(Duration::ZERO, 10), SearchCache::new(Duration::from_secs(60), 0)]
        {
            assert!(!cache.enabled());
            cache.insert(key("a"), Bytes::from_static(b"1"));
            assert!(cache.get(&key("a")).is_none());
        }
    }

    #[tokio::test]
    async fn concurrent_identical_searches_compute_once() {
        let cache = SearchCache::new(Duration::from_secs(60), 10);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let calls = calls.clone();
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_compute(key("same"), || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, ()>(Bytes::from_static(b"body"))
                    })
                    .await
            }));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap().unwrap(), Bytes::from_static(b"body"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cache.inner.lock().unwrap().inflight.is_empty());
    }

    #[tokio::test]
    async fn errors_are_not_cached_and_do_not_poison_waiters() {
        let cache = SearchCache::new(Duration::from_secs(60), 10);
        let r: Result<Bytes, &str> = cache.get_or_compute(key("x"), || async { Err("db") }).await;
        assert_eq!(r, Err("db"));
        assert!(cache.get(&key("x")).is_none());
        let r: Result<Bytes, &str> =
            cache.get_or_compute(key("x"), || async { Ok(Bytes::from_static(b"ok")) }).await;
        assert_eq!(r.unwrap(), Bytes::from_static(b"ok"));
        assert!(cache.inner.lock().unwrap().inflight.is_empty());
    }

    #[tokio::test]
    async fn disabled_cache_still_single_flights() {
        let cache = SearchCache::new(Duration::ZERO, 0);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let cache = cache.clone();
            let calls = calls.clone();
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_compute(key("same"), || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(30)).await;
                        Ok::<_, ()>(Bytes::from_static(b"body"))
                    })
                    .await
            }));
        }
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cache.is_empty());
        assert!(cache.inner.lock().unwrap().inflight.is_empty());
    }
}
