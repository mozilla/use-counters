mod aggregate;
mod fetch;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures::stream::{self, StreamExt};

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
struct Cli {
    /// Disable gzip compression for downloads. Gzip allows significantly smaller transfers, but
    /// higher CPU usage.
    #[arg(long)]
    no_gzip: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download raw telemetry from Mozilla's public data and aggregate it locally.
    Aggregate(AggregateArgs),
    /// Mirror pre-built artifact data from mozilla.github.io/use-counters/data.
    Artifact(ArtifactArgs),
}

#[derive(Args)]
struct AggregateArgs {
    /// Which dataset(s) to fetch.
    #[arg(long, value_enum, default_value = "all")]
    dataset: DatasetArg,

    /// List of local inputs, for testing. Overrides the dataset option.
    #[arg(long, short)]
    input: Vec<PathBuf>,

    /// Write to output directory.
    #[arg(long, short)]
    output: PathBuf,

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

#[derive(Args)]
struct ArtifactArgs {
    /// Write to output directory.
    #[arg(long, short)]
    output: PathBuf,

    /// Number of files to download concurrently.
    #[arg(short, long, default_value_t = 8)]
    jobs: usize,
}

#[derive(Clone, ValueEnum)]
enum DatasetArg {
    Fenix,
    Desktop,
    All,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let client = reqwest::Client::builder()
        .user_agent("use-counters/0.1 (https://github.com/mozilla/use-counters)")
        .gzip(!cli.no_gzip)
        .build()?;

    match cli.command {
        Command::Aggregate(args) => fetch_command(args, client).await,
        Command::Artifact(args) => artifact_command(args, client).await,
    }
}

async fn fetch_command(args: AggregateArgs, client: reqwest::Client) -> Result<()> {
    let mut last_updated = vec![];
    let mut aggregate = AggregateMap::default();

    if !args.input.is_empty() {
        for input in &args.input {
            let file = std::fs::File::open(input)?;
            let records = aggregate::aggregate_file_into(file, args.mode, &mut aggregate)?;
            eprintln!("[{}] {} records aggregated", input.display(), records);
        }
    } else {
        let datasets: Vec<fetch::Dataset> = match args.dataset {
            DatasetArg::Fenix => vec![fetch::Dataset::Fenix],
            DatasetArg::Desktop => vec![fetch::Dataset::Desktop],
            DatasetArg::All => vec![fetch::Dataset::Fenix, fetch::Dataset::Desktop],
        };
        for dataset in datasets {
            last_updated.push(
                fetch::fetch_and_aggregate_dataset(
                    &client,
                    dataset,
                    args.jobs,
                    args.max_files,
                    args.cache_dir.as_deref(),
                    args.mode,
                    &mut aggregate,
                )
                .await?,
            );
        }
    }

    let output = aggregate.to_output(last_updated);

    eprintln!("{} metrics processed", output.overview.len());
    tokio::fs::create_dir_all(&args.output).await?;

    let overview = args.output.join("overview.json");
    std::fs::write(&overview, serde_json::to_string_pretty(&output.overview)?)?;
    eprintln!("Overview written to {}", overview.display());

    for (metric, entries) in &output.per_metric {
        let path = args.output.join(format!("{}.json", metric));
        std::fs::write(&path, serde_json::to_string_pretty(entries)?)?;
        eprintln!("{} entries written to {}", entries.len(), path.display());
    }

    Ok(())
}

async fn artifact_command(args: ArtifactArgs, client: reqwest::Client) -> Result<()> {
    const BASE_URL: &str = "https://mozilla.github.io/use-counters/data";

    tokio::fs::create_dir_all(&args.output).await?;

    let overview_url = format!("{BASE_URL}/overview.json");
    eprintln!("Fetching {overview_url}");
    let overview_bytes = client.get(&overview_url).send().await?.bytes().await?;

    let overview: aggregate::Overview = serde_json::from_slice(&overview_bytes)?;
    let metrics: Vec<String> = overview.all_metrics.into_keys().collect();

    let overview_path = args.output.join("overview.json");
    std::fs::write(&overview_path, &overview_bytes)?;
    eprintln!(
        "{} metrics found, overview written to {}",
        metrics.len(),
        overview_path.display()
    );

    let total = metrics.len();
    let mut stream = stream::iter(metrics.into_iter().enumerate().map(|(i, metric)| {
        let url = format!("{BASE_URL}/{metric}.json");
        let client = client.clone();
        let path = args.output.join(format!("{metric}.json"));
        async move {
            eprintln!("[{}/{}] fetching {}", i + 1, total, url);
            let bytes = client.get(&url).send().await?.bytes().await?;
            std::fs::write(&path, &bytes)?;
            eprintln!("[{}/{}] written to {}", i + 1, total, path.display());
            anyhow::Ok(())
        }
    }))
    .buffer_unordered(args.jobs);

    while let Some(result) = stream.next().await {
        result?;
    }

    Ok(())
}
