//! Release-date selection.
//!
//! MusicBrainz stores release dates as per-country *events* in
//! `release_country` (+ `release_unknown_country`), not on the release row.
//! A release may have several events; we pick the earliest meaningful one and
//! render it in MB's partial-date format ("YYYY", "YYYY-MM", "YYYY-MM-DD", or
//! "" when unknown).

/// A single date event: any of the components may be missing (NULL in the DB).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateEvent {
    pub year: Option<i32>,
    pub month: Option<i32>,
    pub day: Option<i32>,
    /// True for the `release_unknown_country` table (no country code).
    pub is_xw: bool,
}

impl DateEvent {
    /// Sort key: events without a year sort last; otherwise earliest
    /// year/month/day wins, and `XW` (worldwide) breaks ties.
    fn order_key(self) -> (i32, i32, i32, u8) {
        (
            self.year.unwrap_or(i32::MAX),
            self.month.unwrap_or(i32::MAX),
            self.day.unwrap_or(i32::MAX),
            u8::from(!self.is_xw),
        )
    }

    /// Render as a MusicBrainz partial date string.
    #[must_use]
    pub fn render(self) -> String {
        match (self.year, self.month, self.day) {
            (Some(y), Some(m), Some(d)) => format!("{y:04}-{m:02}-{d:02}"),
            (Some(y), Some(m), None) => format!("{y:04}-{m:02}"),
            (Some(y), None, _) => format!("{y:04}"),
            _ => String::new(),
        }
    }
}

/// Pick the earliest date event and render it. Returns "" when none usable.
#[must_use]
pub fn select_release_date(events: &[DateEvent]) -> String {
    events
        .iter()
        .copied()
        .filter(|e| e.year.is_some())
        .min_by_key(|e| e.order_key())
        .map_or_else(String::new, DateEvent::render)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(y: Option<i32>, m: Option<i32>, d: Option<i32>, xw: bool) -> DateEvent {
        DateEvent { year: y, month: m, day: d, is_xw: xw }
    }

    #[test]
    fn empty_is_blank() {
        assert_eq!(select_release_date(&[]), "");
    }

    #[test]
    fn picks_earliest_year() {
        let events =
            [ev(Some(1979), Some(1), Some(1), false), ev(Some(1973), Some(3), Some(1), false)];
        assert_eq!(select_release_date(&events), "1973-03-01");
    }

    #[test]
    fn year_only() {
        assert_eq!(select_release_date(&[ev(Some(1973), None, None, false)]), "1973");
    }

    #[test]
    fn year_month_only() {
        assert_eq!(select_release_date(&[ev(Some(1973), Some(3), None, false)]), "1973-03");
    }

    #[test]
    fn xw_breaks_tie() {
        let events =
            [ev(Some(1973), Some(3), Some(1), false), ev(Some(1973), Some(3), Some(1), true)];
        // Same date, XW preferred.
        let chosen = events.iter().copied().min_by_key(|e| e.order_key()).unwrap();
        assert!(chosen.is_xw);
        assert_eq!(select_release_date(&events), "1973-03-01");
    }

    #[test]
    fn events_without_year_ignored() {
        let events = [ev(None, Some(5), None, false), ev(Some(1980), None, None, false)];
        assert_eq!(select_release_date(&events), "1980");
    }

    #[test]
    fn earlier_month_within_same_year() {
        let events = [ev(Some(1973), Some(10), None, false), ev(Some(1973), Some(3), None, false)];
        assert_eq!(select_release_date(&events), "1973-03");
    }
}
