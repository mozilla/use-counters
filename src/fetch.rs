use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::aggregate::{AggregateMap, aggregate_into};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dataset {
    Fenix,
    Desktop,
}

impl Dataset {
    pub fn name(self) -> &'static str {
        match self {
            Dataset::Fenix => "fenix",
            Dataset::Desktop => "firefox_desktop",
        }
    }

    pub fn files_url(self) -> String {
        let name = self.name();
        format!(
            "https://public-data.telemetry.mozilla.org/api/v1/tables/{name}_derived/{name}_use_counters/v2/files"
        )
    }
}

/// A single row from the use-counters telemetry tables.
///
/// All numeric fields are stored as strings in the source JSON (BigQuery export).
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SourceRecord {
    pub submission_date: String,
    pub version_major: String,
    pub country: String,
    pub platform: String,
    /// Total content documents destroyed in this (date, version, country, platform) segment.
    /// Denominator for doc-level use counters.
    pub use_counter_content_documents_destroyed: String,
    /// Total top-level documents destroyed. Denominator for page-level use counters.
    pub use_counter_top_level_content_documents_destroyed: String,
    pub use_counter_service_workers_destroyed: String,
    pub use_counter_shared_workers_destroyed: String,
    pub use_counter_dedicated_workers_destroyed: String,
    /// The use counter name, e.g. "use.counter.css.doc.css_moz_animation_name".
    pub metric: String,
    /// Number of times this counter was hit in this segment (as a string).
    pub cnt: String,
    /// Pre-computed rate = cnt / denominator for this segment.
    pub rate: String,
}

impl SourceRecord {
    /// Parse `cnt` to u64, returning 0 on failure.
    pub fn cnt_u64(&self) -> u64 {
        self.cnt.parse().unwrap_or(0)
    }

    /// Parse the appropriate denominator based on the metric name.
    ///
    /// - `use.counter.page.*`                 → top_level_content_documents_destroyed
    /// - `use.counter.worker.dedicated.*`     → dedicated_workers_destroyed
    /// - `use.counter.worker.shared.*`        → shared_workers_destroyed
    /// - `use.counter.worker.service.*`       → service_workers_destroyed
    /// - everything else (doc-level counters) → content_documents_destroyed
    pub fn denominator_u64(&self) -> u64 {
        let m = &self.metric;
        let raw = if m.contains(".page.") {
            &self.use_counter_top_level_content_documents_destroyed
        } else if m.contains(".worker.dedicated.") {
            &self.use_counter_dedicated_workers_destroyed
        } else if m.contains(".worker.shared.") {
            &self.use_counter_shared_workers_destroyed
        } else if m.contains(".worker.service.") {
            &self.use_counter_service_workers_destroyed
        } else {
            &self.use_counter_content_documents_destroyed
        };
        raw.parse().unwrap_or(0)
    }
}

/// Download all records for the given dataset, using an optional local cache directory.
///
/// If `max_files` is `Some(n)`, only the first `n` files are downloaded (useful for testing).
pub async fn fetch_and_aggregate_dataset(
    client: &reqwest::Client,
    dataset: Dataset,
    jobs: usize,
    max_files: Option<usize>,
    cache_dir: Option<&Path>,
    aggregate: &mut AggregateMap,
) -> Result<()> {
    let url = dataset.files_url();
    eprintln!("[{}] Fetching file list from {}", dataset.name(), url);

    let cache_dir = cache_dir.map(|c| c.join(dataset.name()));
    let cache_dir = cache_dir.as_ref();
    if let Some(cache_dir) = cache_dir {
        tokio::fs::create_dir_all(cache_dir).await?;
    }

    let file_urls: Vec<String> = client
        .get(url)
        .send()
        .await
        .context("Failed to request file list")?
        .json()
        .await
        .context("Failed to parse file list JSON")?;

    let file_urls: Vec<String> = match max_files {
        Some(n) => file_urls.into_iter().take(n).collect(),
        None => file_urls,
    };

    eprintln!("[{}] {} files to download", dataset.name(), file_urls.len());

    let total = file_urls.len();
    let mut stream = stream::iter(file_urls.iter().enumerate().map(|(i, url)| async move {
        eprintln!("[{}] {}/{} start ({})", dataset.name(), i + 1, total, url);
        let cache_path = cache_dir.map(|c| {
            let basename_start = url.rfind("/").unwrap_or(0);
            c.join(&url[basename_start..])
        });
        if let Some(ref cache_path) = cache_path {
            if let Ok(bytes) = tokio::fs::read(&cache_path).await {
                eprintln!("[{}] {}/{} loaded from cache", dataset.name(), i + 1, total);
                return anyhow::Ok((i, bytes::Bytes::from(bytes)));
            }
        }
        let bytes = client
            .get(url)
            .send()
            .await?
            .bytes()
            .await
            .with_context(|| format!("Failed to fetch file {i}/{total}: {url}"))?;
        if let Some(cache_path) = cache_path {
            if let Err(e) = tokio::fs::write(&cache_path, &bytes).await {
                eprintln!(
                    "[{}] {}/{} failed to write cache: {}",
                    dataset.name(),
                    i + 1,
                    total,
                    e
                );
            } else {
                eprintln!("[{}] {}/{} saved to cache", dataset.name(), i + 1, total);
            }
        }
        anyhow::Ok((i, bytes))
    }))
    .buffer_unordered(jobs);

    while let Some(result) = stream.next().await {
        let (i, bytes) = result?;
        let recs = parse_records(&bytes)?;
        eprintln!(
            "[{}] {}/{} done ({} records)",
            dataset.name(),
            i + 1,
            total,
            recs.len()
        );
        aggregate_into(&recs, aggregate);
    }
    Ok(())
}

pub async fn parse_file(path: &Path) -> Result<Vec<SourceRecord>> {
    let bytes = tokio::fs::read(path).await?;
    parse_records(&bytes)
}

fn parse_records(bytes: &[u8]) -> Result<Vec<SourceRecord>> {
    serde_json::from_slice(bytes).context("Failed to parse records JSON")
}
