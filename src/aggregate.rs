use std::{collections::HashMap, io::BufRead};

use chrono::{Datelike, NaiveDate};
use serde::Serialize;
use serde_json::{StreamDeserializer, de::IoRead};
use std::collections::BTreeMap;

use crate::{
    ProcessingMode,
    fetch::{Platform, SourceRecord},
};

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
pub fn aggregate_record(record: &SourceRecord, result: &mut AggregateMap) -> bool {
    if record.metric.contains(".doc.") {
        // Skip document-level metrics, which are not as useful as page metrics.
        return false;
    }
    let date = match NaiveDate::parse_from_str(&record.submission_date, "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => {
            eprintln!(
                "Warning: unparseable date {:?}, skipping record {:?}",
                record.submission_date, record
            );
            return false;
        }
    };

    if record.cnt < 0 {
        eprintln!(
            "warning: negative count {}, skipping record {:?}",
            record.cnt, record
        );
        return false;
    }

    let denom = record.denominator();
    if denom < 0 {
        eprintln!(
            "warning: negative denominator {}, skipping record {:?}",
            denom, record
        );
        return false;
    }

    if denom < record.cnt {
        eprintln!(
            "warning: smaller denominator {} than count {}, skipping record {:?}",
            denom, record.cnt, record
        );
        return false;
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
    entry.denominator += denom as u64;
    true
}

#[derive(Debug, Serialize, Default)]
pub struct MetricOverview {
    cnt: u64,
    denominator: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct Overview {
    last_updated: Vec<String>,
    all_metrics: BTreeMap<String, MetricOverview>,
}

impl Overview {
    pub fn len(&self) -> usize {
        self.all_metrics.len()
    }
}

#[derive(Default)]
pub struct Output {
    pub overview: Overview,
    pub per_metric: HashMap<String, Vec<AggregatedEntry>>,
}

impl AggregateMap {
    pub fn to_output(&self, last_updated: Vec<String>) -> Output {
        let mut out = Output::default();
        out.overview.last_updated = last_updated;
        for (key, acc) in &self.0 {
            let entry = out
                .overview
                .all_metrics
                .entry(key.metric.clone())
                .or_default();
            entry.cnt += acc.cnt;
            entry.denominator += acc.denominator;
            out.per_metric
                .entry(key.metric.clone())
                .or_default()
                .push(AggregatedEntry {
                    week_start: NaiveDate::from_isoywd_opt(
                        key.iso_year,
                        key.iso_week,
                        chrono::Weekday::Mon,
                    )
                    .expect("valid ISO year+week from parsed date")
                    .format("%Y-%m-%d")
                    .to_string(),
                    platform: key.platform.clone(),
                    version_major: key.version_major,
                    cnt: acc.cnt,
                    denominator: acc.denominator,
                });
        }
        out
    }

    pub fn merge_with(&mut self, other: AggregateMap) {
        for (key, acc) in other.0 {
            let entry = self.0.entry(key).or_default();
            entry.cnt += acc.cnt;
            entry.denominator += acc.denominator;
        }
    }
}

pub fn aggregate_file_into(
    file: std::fs::File,
    processing_mode: ProcessingMode,
    result: &mut AggregateMap,
) -> anyhow::Result<usize> {
    if processing_mode == ProcessingMode::Memory {
        let reader = std::io::BufReader::new(file);
        let records: Vec<SourceRecord> = serde_json::from_reader(reader)?;
        let mut count = 0;
        for record in &records {
            if aggregate_record(record, result) {
                count += 1;
            }
        }
        return Ok(count);
    }
    let mut reader = std::io::BufReader::new(file);
    let mut count = 0;
    // Skip over the initial `[`
    reader.seek_relative(1)?;
    while let Some(r) = StreamDeserializer::new(IoRead::new(&mut reader)).next() {
        match r {
            Ok(record) => {
                if aggregate_record(&record, result) {
                    count += 1;
                }
                // Go to the next comma or EOF (most likely just skip one character).
                reader.skip_until(b',')?;
            }
            Err(e) => {
                if e.is_eof() {
                    break;
                } else {
                    return Err(e.into());
                }
            }
        }
    }
    Ok(count)
}
