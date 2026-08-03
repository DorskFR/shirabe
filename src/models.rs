//! Serde response models emitting MusicBrainz-compatible hyphenated-key JSON.
//!
//! These mirror the parsing structs the consumer service deserializes (the
//! contract). Only the fields the consumer actually reads are emitted.

use serde::Serialize;

/// `GET /ws/2/artist?query=` response. `count` is the total number of matches,
/// not the page size.
#[derive(Debug, Serialize)]
pub struct ArtistSearchResponse {
    pub count: i64,
    pub offset: i64,
    pub artists: Vec<Artist>,
}

/// `GET /ws/2/artist/{mbid}` lookup payload. inc-gated blocks are `None` when
/// their token was not requested (absent from the JSON); requested-but-empty
/// serializes as `[]` (or `null` for annotation), matching upstream MB.
#[derive(Debug, Serialize, Default)]
pub struct ArtistLookup {
    pub id: String,
    pub name: String,
    #[serde(rename = "sort-name")]
    pub sort_name: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub artist_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disambiguation: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<UrlRelation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub genres: Option<Vec<Genre>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<Tag>>,
    #[allow(clippy::option_option)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotation: Option<Option<String>>,
}

/// `id` is the genre entity's gid.
#[derive(Debug, Serialize)]
pub struct Genre {
    pub id: String,
    pub name: String,
    pub count: i32,
}

#[derive(Debug, Serialize)]
pub struct Tag {
    pub name: String,
    pub count: i32,
}

/// A URL relationship in the ws/2 `relations[]` shape: `{ type, direction, url:
/// { resource } }`. The consumer matches on `type == "image"` and reads
/// `url.resource`.
#[derive(Debug, Serialize)]
pub struct UrlRelation {
    #[serde(rename = "type")]
    pub rel_type: String,
    pub direction: String,
    pub url: UrlResource,
}

#[derive(Debug, Serialize)]
pub struct UrlResource {
    pub resource: String,
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

/// `GET /ws/2/release-group?artist=` response.
#[derive(Debug, Serialize)]
pub struct ReleaseGroupBrowseResponse {
    #[serde(rename = "release-group-count")]
    pub release_group_count: i64,
    #[serde(rename = "release-group-offset")]
    pub release_group_offset: i64,
    #[serde(rename = "release-groups")]
    pub release_groups: Vec<ReleaseGroupDetail>,
}

/// A release-group in the browse/lookup shape. `releases` is `Some` only on the
/// lookup path (serialized even when the group has none, matching upstream MB).
#[derive(Debug, Serialize)]
pub struct ReleaseGroupDetail {
    pub id: String,
    pub title: String,
    #[serde(rename = "primary-type")]
    pub primary_type: Option<String>,
    #[serde(rename = "secondary-types")]
    pub secondary_types: Vec<String>,
    #[serde(rename = "first-release-date")]
    pub first_release_date: String,
    pub disambiguation: String,
    #[serde(rename = "artist-credit")]
    pub artist_credit: Vec<ArtistCredit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub releases: Option<Vec<ReleaseGroupRelease>>,
}

#[derive(Debug, Serialize)]
pub struct ReleaseGroupRelease {
    pub id: String,
    pub date: String,
    pub status: Option<String>,
}

/// `GET /ws/2/release?query=` response.
#[derive(Debug, Serialize)]
pub struct ReleaseSearchResponse {
    pub count: i64,
    pub offset: i64,
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
    pub count: i64,
    pub offset: i64,
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

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn detail() -> ReleaseGroupDetail {
        ReleaseGroupDetail {
            id: "0b0c25f4-f31c-46a5-a4fb-ccbf53d663bd".into(),
            title: "Homogenic".into(),
            primary_type: Some("Album".into()),
            secondary_types: vec![],
            first_release_date: "1997-09-20".into(),
            disambiguation: String::new(),
            artist_credit: vec![ArtistCredit {
                artist: ArtistRef {
                    id: "87c5dedd-371d-4a53-9f7f-80522fb7f3cb".into(),
                    name: "Björk".into(),
                    aliases: vec![],
                },
            }],
            releases: None,
        }
    }

    #[test]
    fn release_search_result_carries_release_group_id() {
        let v = serde_json::to_value(ReleaseSearchResponse {
            count: 1,
            offset: 0,
            releases: vec![Release {
                id: "b1392450-e666-3926-a536-22c65f834433".into(),
                title: "Homogenic".into(),
                date: "1997-09-20".into(),
                score: Some(100),
                status: Some("Official".into()),
                release_group: Some(ReleaseGroup {
                    id: "0b0c25f4-f31c-46a5-a4fb-ccbf53d663bd".into(),
                    primary_type: Some("Album".into()),
                }),
                ..Default::default()
            }],
        })
        .unwrap();
        let rel = &v["releases"][0];
        assert_eq!(rel["release-group"]["id"], json!("0b0c25f4-f31c-46a5-a4fb-ccbf53d663bd"));
        assert_eq!(rel["release-group"]["primary-type"], json!("Album"));
        assert_eq!(rel["status"], json!("Official"));
    }

    #[test]
    fn search_envelopes_carry_count_and_offset() {
        let v =
            serde_json::to_value(ArtistSearchResponse { count: 342, offset: 25, artists: vec![] })
                .unwrap();
        assert_eq!(v["count"], json!(342));
        assert_eq!(v["offset"], json!(25));
        assert_eq!(v["artists"], json!([]));
        assert!(v.get("created").is_none());

        let v =
            serde_json::to_value(ReleaseSearchResponse { count: 7, offset: 0, releases: vec![] })
                .unwrap();
        assert_eq!(v["count"], json!(7));
        assert_eq!(v["offset"], json!(0));
        assert_eq!(v["releases"], json!([]));

        let v = serde_json::to_value(RecordingSearchResponse {
            count: 0,
            offset: 100,
            recordings: vec![],
        })
        .unwrap();
        assert_eq!(v["count"], json!(0));
        assert_eq!(v["offset"], json!(100));
        assert_eq!(v["recordings"], json!([]));
    }

    #[test]
    fn browse_response_shape() {
        let v = serde_json::to_value(ReleaseGroupBrowseResponse {
            release_group_count: 342,
            release_group_offset: 100,
            release_groups: vec![detail()],
        })
        .unwrap();
        assert_eq!(v["release-group-count"], json!(342));
        assert_eq!(v["release-group-offset"], json!(100));
        let rg = &v["release-groups"][0];
        assert_eq!(rg["id"], json!("0b0c25f4-f31c-46a5-a4fb-ccbf53d663bd"));
        assert_eq!(rg["title"], json!("Homogenic"));
        assert_eq!(rg["primary-type"], json!("Album"));
        assert_eq!(rg["secondary-types"], json!([]));
        assert_eq!(rg["first-release-date"], json!("1997-09-20"));
        assert_eq!(rg["disambiguation"], json!(""));
        assert_eq!(
            rg["artist-credit"][0]["artist"]["id"],
            json!("87c5dedd-371d-4a53-9f7f-80522fb7f3cb")
        );
        assert_eq!(rg["artist-credit"][0]["artist"]["name"], json!("Björk"));
        assert!(rg.get("releases").is_none(), "browse entries must not carry releases");
    }

    #[test]
    fn lookup_shape_serializes_releases_even_when_empty() {
        let mut d = detail();
        d.releases = Some(vec![]);
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v["releases"], json!([]));
    }

    #[test]
    fn artist_lookup_omits_blocks_unless_requested() {
        let v = serde_json::to_value(ArtistLookup {
            id: "87c5dedd-371d-4a53-9f7f-80522fb7f3cb".into(),
            name: "Björk".into(),
            sort_name: "Björk".into(),
            ..Default::default()
        })
        .unwrap();
        assert!(v.get("genres").is_none());
        assert!(v.get("tags").is_none());
        assert!(v.get("annotation").is_none());
        assert!(v.get("relations").is_none());
    }

    #[test]
    fn artist_lookup_requested_blocks_shapes() {
        let v = serde_json::to_value(ArtistLookup {
            id: "87c5dedd-371d-4a53-9f7f-80522fb7f3cb".into(),
            name: "Björk".into(),
            sort_name: "Björk".into(),
            genres: Some(vec![Genre {
                id: "89255676-1f14-4dd8-bbad-fca839d6aff4".into(),
                name: "electronic".into(),
                count: 7,
            }]),
            tags: Some(vec![Tag { name: "icelandic".into(), count: 3 }]),
            annotation: Some(Some("Icelandic singer.".into())),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            v["genres"][0],
            json!({ "id": "89255676-1f14-4dd8-bbad-fca839d6aff4", "name": "electronic", "count": 7 })
        );
        assert_eq!(v["tags"][0], json!({ "name": "icelandic", "count": 3 }));
        assert_eq!(v["annotation"], json!("Icelandic singer."));
    }

    #[test]
    fn artist_lookup_requested_but_empty_blocks() {
        let v = serde_json::to_value(ArtistLookup {
            id: "87c5dedd-371d-4a53-9f7f-80522fb7f3cb".into(),
            name: "Björk".into(),
            sort_name: "Björk".into(),
            genres: Some(vec![]),
            tags: Some(vec![]),
            annotation: Some(None),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(v["genres"], json!([]));
        assert_eq!(v["tags"], json!([]));
        assert_eq!(v["annotation"], Value::Null);
        assert!(v.as_object().unwrap().contains_key("annotation"));
    }

    #[test]
    fn lookup_release_entry_shape() {
        let mut d = detail();
        d.primary_type = None;
        d.secondary_types = vec!["Live".into(), "Compilation".into()];
        d.releases = Some(vec![ReleaseGroupRelease {
            id: "b1392450-e666-3926-a536-22c65f834433".into(),
            date: "1997".into(),
            status: None,
        }]);
        let v = serde_json::to_value(&d).unwrap();
        assert_eq!(v["primary-type"], Value::Null);
        assert_eq!(v["secondary-types"], json!(["Live", "Compilation"]));
        let rel = &v["releases"][0];
        assert_eq!(rel["id"], json!("b1392450-e666-3926-a536-22c65f834433"));
        assert_eq!(rel["date"], json!("1997"));
        assert_eq!(rel["status"], Value::Null);
    }
}
