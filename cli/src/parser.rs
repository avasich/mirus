use std::{path::PathBuf, time::Duration};

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use mirus::{
    measure::ConnectionConfig,
    mirror::Mirrorlist,
    pipeline::{FilterCriteria, Pipeline, SortDir},
};
use url::Url;


#[allow(clippy::struct_excessive_bools)]
#[derive(Parser, Debug)]
pub struct Cli {
    /// connection timeout in ms
    #[arg(long, default_value_t = 3000)]
    connection_timeout: u64,

    /// total timeout in ms
    #[arg(long, default_value_t = 5000)]
    timeout: u64,

    /// mirrorlist url
    #[arg(long, default_value = Mirrorlist::URL)]
    url: Url,

    /// max number of concurrent measurements
    #[arg(long, default_value_t = 8)]
    max_concurrent: usize,

    /// sort in ascending order
    #[arg(long, value_name = "KEY")]
    sort_asc: Vec<SortKey>,

    /// sort in descending order
    #[arg(long, value_name = "KEY")]
    sort_des: Vec<SortKey>,

    /// take first N mirrors
    #[arg(long, value_name = "N")]
    take: Vec<usize>,

    /// filter: match one of the given protocols.
    #[arg(long, value_delimiter = ',')]
    protocol: Vec<Protocol>,

    /// filter: restrict mirrors to selected countries.
    #[arg(long, value_delimiter = ',')]
    country: Vec<String>,

    /// filter: mirror must support ipv4
    #[arg(long, default_value_t = false)]
    ipv4: bool,

    /// filter: mirror must support ipv6
    #[arg(long, default_value_t = false)]
    ipv6: bool,

    /// filter: mirror must host ISO
    #[arg(long)]
    isos: bool,

    /// verbose output
    #[arg(long, default_value_t = false)]
    verbose: bool,

    /// output file path
    #[arg(long)]
    save: Option<PathBuf>,

    /// ttl of the local mirrorlist cache, in seconds
    #[arg(long, default_value_t = 300)]
    cache_ttl: u64,
}


pub fn parse() -> Result<(Config, Pipeline), crate::error::Error> {
    #[derive(Clone, Copy)]
    enum PipelineOps {
        SortAsc,
        SortDesc,
        Take,
    }

    let (cli, ops) = {
        // relative order of the pipeline operations matters,
        // so we sort them according to their indices in the raw command
        let cli = Cli::command();
        let ms = cli.get_matches();

        let sort_asc = ms.indices_of("sort_asc").into_iter().flatten().map(|i| (i, PipelineOps::SortAsc));
        let sort_des = ms.indices_of("sort_des").into_iter().flatten().map(|i| (i, PipelineOps::SortDesc));
        let take = ms.indices_of("take").into_iter().flatten().map(|i| (i, PipelineOps::Take));

        let mut ops: Vec<_> = sort_asc.chain(sort_des).chain(take).collect();
        ops.sort_by_key(|(i, _)| *i);

        let cli = Cli::from_arg_matches(&ms)?;
        (cli, ops)
    };

    let config = ConnectionConfig {
        connection_timeout: Duration::from_millis(cli.connection_timeout),
        timeout: Duration::from_millis(cli.timeout),
        max_concurrent_measurements: cli.max_concurrent,
    };

    let pipeline = {
        // first, we filter out all the mirrors we can...
        let filter_protocol = (!cli.protocol.is_empty()).then(|| {
            let allowed: Vec<_> = cli.protocol.into_iter().map(mirus::mirror::Protocol::from).collect();
            FilterCriteria::protocol(allowed)
        });
        let filter_country = (!cli.country.is_empty()).then(|| FilterCriteria::country(cli.country));
        let filter_ipv = (cli.ipv4 || cli.ipv6).then_some(FilterCriteria::IpV { v4: cli.ipv4, v6: cli.ipv6 });

        [filter_protocol, filter_country, filter_ipv].into_iter().flatten().fold(Pipeline::new(), Pipeline::filter)
    };

    let pipeline = {
        use mirus::pipeline::SortKey;
        // ...and then apply the rest of the pipeline in the correct order
        let (mut sa, mut sd, mut t) = (0, 0, 0);
        ops.into_iter().fold(pipeline, |pipeline, (_, key)| match key {
            PipelineOps::SortAsc => {
                sa += 1;
                let sort_key = cli.sort_asc[sa - 1].into();
                if sort_key == SortKey::Rate { pipeline.measure_rate(config) } else { pipeline }
                    .sort(sort_key, SortDir::Asc)
            },
            PipelineOps::SortDesc => {
                sd += 1;
                let sort_key = cli.sort_des[sd - 1].into();
                if sort_key == SortKey::Rate { pipeline.measure_rate(config) } else { pipeline }
                    .sort(sort_key, SortDir::Desc)
            },
            PipelineOps::Take => {
                t += 1;
                pipeline.take(cli.take[t - 1])
            },
        })
    };

    let cache_file = std::env::var("XDG_CACHE_HOME")
        .ok()
        .map(PathBuf::from)
        .or_else(|| std::env::home_dir().map(|home| home.join(".cache")))
        .map(|cache_dir| cache_dir.join("mirus.json"));

    let save_file = cli.save.and_then(|path| {
        if path.is_absolute() {
            Some(path)
        } else {
            std::env::current_dir().ok().map(|dir| dir.join(path))?.normalize_lexically().ok()
        }
    });

    let config = Config {
        save_file,
        verbose: cli.verbose,
        mirrorlist_url: cli.url,
        cache_ttl: Duration::from_secs(cli.cache_ttl),
        cache_file,
    };

    Ok((config, pipeline))
}


#[derive(Subcommand)]
enum Commands {
    ListCountries,
    Measure,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SortKey {
    CompPct,
    Country,
    Delay,
    Duration,
    IpV4,
    IpV6,
    LastSync,
    Protocol,
    Score,
    Rate,
    Url,
}


impl From<SortKey> for mirus::pipeline::SortKey {
    fn from(value: SortKey) -> Self {
        match value {
            SortKey::CompPct => Self::CompPct,
            SortKey::Country => Self::Country,
            SortKey::Delay => Self::Delay,
            SortKey::Duration => Self::Duration,
            SortKey::IpV4 => Self::IpV4,
            SortKey::IpV6 => Self::IpV6,
            SortKey::LastSync => Self::LastSync,
            SortKey::Protocol => Self::Protocol,
            SortKey::Score => Self::Score,
            SortKey::Rate => Self::Rate,
            SortKey::Url => Self::Url,
        }
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Protocol {
    Http,
    Https,
    Rsync,
}


impl From<Protocol> for mirus::mirror::Protocol {
    fn from(value: Protocol) -> Self {
        match value {
            Protocol::Http => Self::Http,
            Protocol::Https => Self::Https,
            Protocol::Rsync => Self::Rsync,
        }
    }
}


#[derive(Debug, Clone)]
pub struct Config {
    pub save_file: Option<PathBuf>,
    pub verbose: bool,
    pub mirrorlist_url: Url,
    pub cache_ttl: Duration,
    pub cache_file: Option<PathBuf>,
}
