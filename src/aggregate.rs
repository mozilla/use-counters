use std::collections::HashMap;

use chrono::{Datelike, NaiveDate};
use serde::Serialize;

use crate::fetch::{Platform, SourceRecord};

#[derive(Debug, Default)]
pub struct AggregateMap(HashMap<AggKey, Accum>);

/// Key used to group records for weekly aggregation.
///
/// We aggregate `country` and `version_major`, keeping
/// `metric` and `platform` as separate dimensions.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AggKey {
    metric: String,
    platform: Platform,
    version_major: u32,
    /// ISO week-year (can differ from calendar year near year boundaries).
    iso_year: i32,
    iso_week: u32,
}

#[derive(Debug, Default)]
pub struct Accum {
    cnt: u64,
    /// Sum of per-segment denominators (unique (date, version, country, platform) tuples
    /// that contributed to this metric's count).
    denominator: u64,
}

/// One aggregated data point: a single metric × platform × ISO week.
#[derive(Debug, Serialize)]
pub struct AggregatedEntry {
    pub metric: String,
    pub platform: Platform,
    pub version_major: u32,
    /// Monday of the ISO week, formatted as "YYYY-MM-DD".
    pub week_start: String,
    /// Sum of hit counts across all countries and versions in this week.
    pub cnt: u64,
    /// Sum of denominator values across contributing segments.
    /// What this counts depends on the metric type (documents, pages, or workers destroyed).
    pub denominator: u64,
}

/// Aggregate a flat list of records into weekly entries.
pub fn aggregate_into(records: &[SourceRecord], result: &mut AggregateMap) {
    for record in records {
        let date = match NaiveDate::parse_from_str(&record.submission_date, "%Y-%m-%d") {
            Ok(d) => d,
            Err(_) => {
                eprintln!(
                    "Warning: unparseable date {:?}, skipping record {:?}",
                    record.submission_date, record
                );
                continue;
            }
        };

        if record.cnt < 0 {
            eprintln!(
                "Warning: negative count {}, skipping record {:?}",
                record.cnt, record
            );
            continue;
        }

        let iso = date.iso_week();
        let key = AggKey {
            metric: record.metric.clone(),
            platform: record.platform.clone(),
            version_major: record.version_major.clone(),
            iso_year: iso.year(),
            iso_week: iso.week(),
        };

        let entry = result.0.entry(key).or_default();
        entry.cnt += record.cnt as u64;
        entry.denominator += record.denominator();
    }
}

impl AggregateMap {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Turns this into a json array.
    pub fn into_entries(self) -> Vec<AggregatedEntry> {
        self.0
            .into_iter()
            .map(|(key, acc)| {
                // Reconstruct the Monday of this ISO week.
                let monday =
                    NaiveDate::from_isoywd_opt(key.iso_year, key.iso_week, chrono::Weekday::Mon)
                        .expect("valid ISO year+week from parsed date");
                AggregatedEntry {
                    week_start: monday.format("%Y-%m-%d").to_string(),
                    metric: key.metric,
                    platform: key.platform,
                    version_major: key.version_major,
                    cnt: acc.cnt,
                    denominator: acc.denominator,
                }
            })
            .collect()
    }
}
