//! A deliberately small parser for the Lucene-ish query strings kusaritoi sends.
//!
//! We do **not** implement Lucene. kusaritoi only ever produces a handful of
//! fixed query shapes (see `kusaritoi/src/search/providers/musicbrainz.rs`):
//!
//! - artist:    bare string (the whole query is the artist name)
//! - release:   `release:(title) AND artist:(name)` [`AND date:(YYYY*)`]
//! - recording: `recording:"title" AND artist:"name"`
//!
//! Fields may be wrapped in `(...)` or `"..."`. The `date:` field may carry a
//! trailing `*` wildcard (`date:(1973*)`), which we treat as a year prefix.

/// Extracted, normalised fields from a search query.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ParsedQuery {
    pub release: Option<String>,
    pub artist: Option<String>,
    pub recording: Option<String>,
    /// Year prefix from `date:(YYYY*)`, digits only.
    pub date_year: Option<String>,
    /// Whole-string fallback for bare queries with no field markers.
    pub bare: Option<String>,
}

/// The fields shirabe understands. Anything else is ignored.
const KNOWN_FIELDS: &[&str] = &["release", "artist", "recording", "date"];

/// Parse a Lucene-subset query string into known fields.
///
/// If no `field:` markers are present, the entire (trimmed) input is returned as
/// [`ParsedQuery::bare`] — this is the artist-endpoint case.
#[must_use]
pub fn parse(input: &str) -> ParsedQuery {
    let mut out = ParsedQuery::default();
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return out;
    }

    if !has_known_field(trimmed) {
        out.bare = Some(unescape(trimmed));
        return out;
    }

    let bytes: Vec<char> = trimmed.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        // Find the next `field:` token.
        let Some((field, value, next)) = read_field(&bytes, i) else {
            break;
        };
        i = next;
        match field.as_str() {
            "release" => out.release = non_empty(unescape(&value)),
            "artist" => out.artist = non_empty(unescape(&value)),
            "recording" => out.recording = non_empty(unescape(&value)),
            "date" => out.date_year = non_empty(extract_year(&value)),
            _ => {}
        }
    }
    out
}

/// Does the string contain a recognised `field:` marker?
fn has_known_field(s: &str) -> bool {
    KNOWN_FIELDS.iter().any(|f| {
        s.match_indices(&format!("{f}:")).any(|(idx, _)| {
            // Must be at start or preceded by whitespace / `(` so we don't match
            // mid-word (e.g. a release titled "release:foo").
            idx == 0 || matches!(s.as_bytes().get(idx - 1), Some(b' ' | b'(' | b'\t'))
        })
    })
}

/// Starting at `start`, scan for the next `field:value` pair. Returns the field
/// name (lowercased), the raw value, and the index just past the value.
fn read_field(chars: &[char], start: usize) -> Option<(String, String, usize)> {
    let mut i = start;
    loop {
        // Locate a candidate `field:` at a word boundary.
        while i < chars.len() {
            let at_boundary = i == 0 || matches!(chars[i - 1], ' ' | '(' | '\t');
            if at_boundary && chars[i].is_ascii_alphabetic() {
                break;
            }
            i += 1;
        }
        if i >= chars.len() {
            return None;
        }

        let field_start = i;
        while i < chars.len() && chars[i].is_ascii_alphabetic() {
            i += 1;
        }
        let field: String = chars[field_start..i].iter().collect::<String>().to_lowercase();

        if chars.get(i) == Some(&':') && KNOWN_FIELDS.contains(&field.as_str()) {
            i += 1; // consume ':'
            let (value, next) = read_value(chars, i);
            return Some((field, value, next));
        }
        // Not a known field token; keep scanning past this word.
    }
}

/// Read a field value: a `"..."` phrase, a `(...)` group, or a bare token.
fn read_value(chars: &[char], start: usize) -> (String, usize) {
    let mut i = start;
    while i < chars.len() && chars[i] == ' ' {
        i += 1;
    }
    match chars.get(i) {
        Some('"') => {
            i += 1;
            let s = i;
            while i < chars.len() && chars[i] != '"' {
                i += 1;
            }
            let value: String = chars[s..i].iter().collect();
            if i < chars.len() {
                i += 1; // closing quote
            }
            (value, i)
        }
        Some('(') => {
            i += 1;
            let s = i;
            let mut depth = 1;
            while i < chars.len() && depth > 0 {
                match chars[i] {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
            let value: String = chars[s..i].iter().collect();
            if i < chars.len() {
                i += 1; // closing paren
            }
            (value, i)
        }
        _ => {
            // Bare token: read until whitespace, stopping before a trailing
            // `AND`/`OR` boolean operator.
            let s = i;
            while i < chars.len() && chars[i] != ' ' {
                i += 1;
            }
            (chars[s..i].iter().collect(), i)
        }
    }
}

/// Pull the leading run of digits out of a date value (handles `1973`, `1973*`,
/// `1973-10`, `1973-10-24`).
fn extract_year(value: &str) -> String {
    value.trim().chars().take_while(char::is_ascii_digit).collect()
}

/// Remove Lucene escape backslashes and collapse surrounding whitespace.
fn unescape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\'
            && let Some(&next) = chars.peek()
        {
            out.push(next);
            chars.next();
            continue;
        }
        out.push(c);
    }
    out.trim().to_string()
}

fn non_empty(s: String) -> Option<String> {
    if s.is_empty() { None } else { Some(s) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_artist_query() {
        let q = parse("Pink Floyd");
        assert_eq!(q.bare.as_deref(), Some("Pink Floyd"));
        assert!(q.artist.is_none());
    }

    #[test]
    fn release_and_artist_parens() {
        let q = parse("release:(Dark Side of the Moon) AND artist:(Pink Floyd)");
        assert_eq!(q.release.as_deref(), Some("Dark Side of the Moon"));
        assert_eq!(q.artist.as_deref(), Some("Pink Floyd"));
        assert!(q.bare.is_none());
    }

    #[test]
    fn release_artist_with_date_wildcard() {
        let q = parse("release:(Wish You Were Here) AND artist:(Pink Floyd) AND date:(1975*)");
        assert_eq!(q.release.as_deref(), Some("Wish You Were Here"));
        assert_eq!(q.artist.as_deref(), Some("Pink Floyd"));
        assert_eq!(q.date_year.as_deref(), Some("1975"));
    }

    #[test]
    fn recording_quoted() {
        let q = parse("recording:\"Time\" AND artist:\"Pink Floyd\"");
        assert_eq!(q.recording.as_deref(), Some("Time"));
        assert_eq!(q.artist.as_deref(), Some("Pink Floyd"));
    }

    #[test]
    fn recording_only() {
        let q = parse("recording:\"Money\"");
        assert_eq!(q.recording.as_deref(), Some("Money"));
        assert!(q.artist.is_none());
    }

    #[test]
    fn release_only_parens() {
        let q = parse("release:(Animals)");
        assert_eq!(q.release.as_deref(), Some("Animals"));
        assert!(q.artist.is_none());
    }

    #[test]
    fn handles_parens_inside_title() {
        let q = parse("release:(Greatest Hits (Deluxe)) AND artist:(Queen)");
        assert_eq!(q.release.as_deref(), Some("Greatest Hits (Deluxe)"));
        assert_eq!(q.artist.as_deref(), Some("Queen"));
    }

    #[test]
    fn japanese_values() {
        let q = parse("recording:\"メインタイトル\" AND artist:\"長谷川智樹\"");
        assert_eq!(q.recording.as_deref(), Some("メインタイトル"));
        assert_eq!(q.artist.as_deref(), Some("長谷川智樹"));
    }

    #[test]
    fn escaped_characters() {
        let q = parse(r"release:(AC\/DC\: Live) AND artist:(AC\/DC)");
        assert_eq!(q.release.as_deref(), Some("AC/DC: Live"));
        assert_eq!(q.artist.as_deref(), Some("AC/DC"));
    }

    #[test]
    fn empty_query() {
        let q = parse("   ");
        assert_eq!(q, ParsedQuery::default());
    }

    #[test]
    fn date_year_month_day() {
        let q = parse("release:(X) AND date:(1973-03-01)");
        assert_eq!(q.date_year.as_deref(), Some("1973"));
    }

    #[test]
    fn title_containing_field_word_is_not_split() {
        // A bare query that happens to contain the word "artist" without a colon
        // should remain a bare query.
        let q = parse("the artist formerly known");
        assert_eq!(q.bare.as_deref(), Some("the artist formerly known"));
    }
}
