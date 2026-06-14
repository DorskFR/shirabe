//! Serde response models emitting MusicBrainz-compatible hyphenated-key JSON.
//!
//! These mirror the parsing structs in
//! `kusaritoi/src/search/providers/musicbrainz.rs` (the contract). Only the
//! fields kusaritoi actually deserializes are emitted.

use serde::Serialize;

/// `GET /ws/2/artist?query=` response.
#[derive(Debug, Serialize)]
pub struct ArtistSearchResponse {
    pub artists: Vec<Artist>,
}

#[derive(Debug, Serialize)]
pub struct Artist {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<i32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<Alias>,
}

#[derive(Debug, Serialize, Clone)]
pub struct Alias {
    pub name: String,
    #[serde(rename = "sort-name", skip_serializing_if = "Option::is_none")]
    pub sort_name: Option<String>,
}

/// A reference to an artist inside an artist-credit.
#[derive(Debug, Serialize, Clone)]
pub struct ArtistRef {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<Alias>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ArtistCredit {
    pub artist: ArtistRef,
}

#[derive(Debug, Serialize, Clone)]
pub struct ReleaseGroup {
    pub id: String,
    #[serde(rename = "primary-type", skip_serializing_if = "Option::is_none")]
    pub primary_type: Option<String>,
}

/// `GET /ws/2/release?query=` response.
#[derive(Debug, Serialize)]
pub struct ReleaseSearchResponse {
    pub releases: Vec<Release>,
}

/// A release shape. Used both as a search result and (with `media`/`relations`
/// populated) as a detail lookup payload.
#[derive(Debug, Serialize, Default)]
pub struct Release {
    pub id: String,
    pub title: String,
    /// MusicBrainz partial date: "YYYY", "YYYY-MM", "YYYY-MM-DD" or "".
    pub date: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disambiguation: Option<String>,
    #[serde(rename = "artist-credit", skip_serializing_if = "Vec::is_empty")]
    pub artist_credit: Vec<ArtistCredit>,
    #[serde(rename = "track-count", skip_serializing_if = "Option::is_none")]
    pub track_count: Option<u32>,
    #[serde(rename = "release-group", skip_serializing_if = "Option::is_none")]
    pub release_group: Option<ReleaseGroup>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub media: Vec<Medium>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<Relation>,
}

#[derive(Debug, Serialize, Default)]
pub struct Medium {
    pub id: String,
    pub position: u32,
    #[serde(rename = "track-count")]
    pub track_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tracks: Vec<Track>,
}

#[derive(Debug, Serialize)]
pub struct Track {
    pub id: String,
    pub title: String,
    pub position: u32,
    /// MB track number is TEXT ("1", "A1", ...).
    pub number: String,
    pub recording: RecordingRef,
    #[serde(rename = "artist-credit", skip_serializing_if = "Vec::is_empty")]
    pub artist_credit: Vec<ArtistCredit>,
}

#[derive(Debug, Serialize)]
pub struct RecordingRef {
    pub id: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct Relation {
    pub direction: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release: Option<ReleaseStub>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recording: Option<RecordingStub>,
}

#[derive(Debug, Serialize)]
pub struct ReleaseStub {
    pub id: String,
    pub title: String,
}

#[derive(Debug, Serialize)]
pub struct RecordingStub {
    pub id: String,
    pub title: String,
}

/// `GET /ws/2/recording?query=` response.
#[derive(Debug, Serialize)]
pub struct RecordingSearchResponse {
    pub recordings: Vec<Recording>,
}

#[derive(Debug, Serialize, Default)]
pub struct Recording {
    pub id: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<i32>,
    #[serde(rename = "artist-credit", skip_serializing_if = "Vec::is_empty")]
    pub artist_credit: Vec<ArtistCredit>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub releases: Vec<Release>,
}
