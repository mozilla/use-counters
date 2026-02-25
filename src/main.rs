mod aggregate;
mod fetch;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, ValueEnum};

use crate::aggregate::AggregateMap;

#[derive(Copy, Clone, Eq, PartialEq, Debug, ValueEnum)]
pub enum ProcessingMode {
    Memory,
    Streaming,
}

#[derive(Parser)]
#[command(
    name = "uc-fetch",
    about = "Download and aggregate Mozilla use-counter telemetry data by metric and ISO week"
)]
struct Args {
    /// Which dataset(s) to fetch.
    #[arg(long, value_enum, default_value = "all")]
    dataset: DatasetArg,

    /// List of local inputs, for testing. Overrides the dataset option.
    #[arg(long, short)]
    input: Vec<PathBuf>,

    /// Write output JSON to FILE instead of stdout.
    #[arg(long, short)]
    output: Option<PathBuf>,

    /// Cache files here in the {dataset}-{filename} form.
    #[arg(long, short)]
    cache_dir: Option<PathBuf>,

    /// Number of files to download concurrently.
    #[arg(short, long, default_value_t = 8)]
    jobs: usize,

    /// How is data processed for aggregation. Streaming uses significantly less
    /// memory but is slower.
    #[arg(short, long, default_value = "memory")]
    mode: ProcessingMode,

    /// Only download the first N files per dataset (useful for testing).
    #[arg(long)]
    max_files: Option<usize>,
}

#[derive(Clone, ValueEnum)]
enum DatasetArg {
    Fenix,
    Desktop,
    All,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let client = reqwest::Client::builder()
        .user_agent("use-counters/0.1 (https://github.com/emilio/use-counters)")
        .build()?;

    let mut aggregate = AggregateMap::default();
    if !args.input.is_empty() {
        for input in &args.input {
            let file = std::fs::File::open(input)?;
            let records = aggregate::aggregate_file_into(file, args.mode, &mut aggregate)?;
            eprintln!("[{}] {} records aggregated", input.display(), records,);
        }
    } else {
        let datasets: Vec<fetch::Dataset> = match args.dataset {
            DatasetArg::Fenix => vec![fetch::Dataset::Fenix],
            DatasetArg::Desktop => vec![fetch::Dataset::Desktop],
            DatasetArg::All => vec![fetch::Dataset::Fenix, fetch::Dataset::Desktop],
        };
        for dataset in datasets {
            fetch::fetch_and_aggregate_dataset(
                &client,
                dataset,
                args.jobs,
                args.max_files,
                args.cache_dir.as_deref(),
                args.mode,
                &mut aggregate,
            )
            .await?;
        }
    }

    let aggregated = aggregate.into_entries();

    eprintln!("{} weekly entries produced", aggregated.len());
    let json = serde_json::to_string_pretty(&aggregated)?;

    match args.output {
        Some(path) => {
            std::fs::write(&path, &json)?;
            eprintln!("Output written to {}", path.display());
        }
        None => println!("{json}"),
    }

    Ok(())
}
