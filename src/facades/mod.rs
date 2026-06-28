//! Native-shape provider facades.
//!
//! Shirabe exposes each upstream provider's *native* API surface under a
//! version-native prefix on one host, so Kusaritoi consumes Shirabe by pointing
//! that provider's `base_url` at Shirabe with no client code change:
//!
//! - `/ws/2/*` — MusicBrainz ws/2 subset (implemented; see [`crate::handlers`]).
//! - `/v4/*`   — TheTVDB v4 facade (implemented; see [`tvdb`]).
//! - `/3/*`    — TMDB v3 facade (implemented; see [`tmdb`]).
//!
//! Both the TVDB `/v4` (SHIB-7) and TMDB `/3` (SHIB-6) facades are cache-first over
//! their respective `shirabe.*_cache` tables, with lazy upstream hydration using a
//! server-side key (TVDB additionally mints its own bearer token from the project
//! key + optional PIN). The full contract is documented in
//! `docs/shirabe-api-contract.md`.

pub mod tmdb;
pub mod tvdb;
