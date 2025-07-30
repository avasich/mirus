use std::{collections::HashMap, sync::Arc};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use mirus::{
    measure::MeasureEvent,
    mirror::{MirrorId, Mirrorlist},
};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::event::{CliParseEvent, Event, InitMirrorlistEvent, SaveMirrorlistEvent};


struct ProgressUI {
    mirrorlist: Arc<Mirrorlist>,
    multi: MultiProgress,
    bars: HashMap<MirrorId, ProgressBar>,
}


impl ProgressUI {
    fn new(mirrorlist: Arc<Mirrorlist>) -> Self {
        Self {
            mirrorlist,
            multi: MultiProgress::new(),
            bars: HashMap::new(),
        }
    }

    #[allow(clippy::literal_string_with_formatting_args)]
    fn update(&mut self, id: MirrorId, event: MeasureEvent) {
        let bar = self.bars.entry(id).or_insert_with(|| {
            let url = &self.mirrorlist.get(id).url;
            let url = format!("{}://{}", url.scheme(), url.host_str().unwrap_or("??"));
            let pb = ProgressBar::new(0)
                .with_style(ProgressStyle::with_template("{spinner}{prefix:<40}").unwrap())
                .with_prefix(url);
            self.multi.add(pb)
        });

        match event {
            MeasureEvent::Connecting => {},
            MeasureEvent::Connected { file_size, .. } => {
                bar.set_length(file_size.unwrap_or_default());
                bar.set_style(
                    ProgressStyle::with_template(" {prefix:<40} {wide_bar:.blue/dim.blue} {bytes_per_sec:>13}")
                        .unwrap()
                        .progress_chars("#--"),
                );
            },
            MeasureEvent::BytesReceived { rate } => bar.set_position(rate.bytes_downloaded),
            MeasureEvent::Finished { rate } => {
                if bar.length().is_some_and(|len| rate.bytes_downloaded < len) {
                    bar.set_style(
                        ProgressStyle::with_template(" {prefix:<40} {wide_bar:.blue/dim.blue} {bytes_per_sec:>13}")
                            .unwrap()
                            .progress_chars("###"),
                    );
                }
                bar.set_position(rate.bytes_downloaded);
                bar.abandon();
            },
            MeasureEvent::Failed(err) => {
                use mirus::error::NetworkError;
                bar.set_style(ProgressStyle::with_template(" {prefix:<40} {msg:.red}").unwrap());
                match err {
                    NetworkError::Status(status) => bar.finish_with_message(status.to_string()),
                    NetworkError::Other(_) => bar.finish_with_message("network error"),
                    err => bar.finish_with_message(err.to_string()),
                }
            },
        }
    }
}


#[must_use]
pub fn start_ui(mut rx: UnboundedReceiver<Event>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let verbose = match rx.recv().await {
            Some(Event::CliParse(event)) => match event {
                // TODO: logging
                CliParseEvent::Error => return,
                CliParseEvent::Parsed { verbose } => verbose,
            },
            _ => unreachable!("we parse cli first"),
        };

        let mirrorlist = loop {
            let Some(event) = rx.recv().await else {
                return;
            };

            match event {
                Event::InitMirrorlist(event) => match event {
                    InitMirrorlistEvent::Success { mirrorlist } => break mirrorlist,
                    InitMirrorlistEvent::CheckingCache { path } => {
                        eprintln!("checking cache at {}...", path.display());
                    },
                    // nothing to do here
                    InitMirrorlistEvent::CacheExpired => {
                        eprintln!("cache expired");
                    },
                    // maybe a warning?
                    InitMirrorlistEvent::CacheReadFailure => {
                        eprintln!("failed to read cache file");
                    },
                    // maybe a spinner?
                    InitMirrorlistEvent::Fetching { url } => {
                        eprintln!("fetching mirrorlist from {url}");
                    },
                    // notify the user
                    InitMirrorlistEvent::FetchFailure => {
                        eprintln!("failed to fetch mirrorlist");
                    },
                    // nothing to do here
                    InitMirrorlistEvent::CacheSaveSuccess { path } => {
                        eprintln!("mirrolist cache saved to {}", path.display());
                    },
                    // maybe a warning?
                    InitMirrorlistEvent::CacheSaveFailure { path, error } => {
                        eprintln!("failed to save mirrorlist cache to {}: {error}", path.display());
                    },
                },
                Event::CliParse(_) => unreachable!("cli is already parsed"),
                Event::Measure { .. } | Event::SaveMirrorlist(_) => unreachable!("mirror ranking is not started yet"),
            }
        };

        let mut ui = ProgressUI::new(mirrorlist);

        while let Some(event) = rx.recv().await {
            match event {
                Event::Measure { id, event } => ui.update(id, event),
                Event::SaveMirrorlist(event) => match event {
                    // notify the user
                    SaveMirrorlistEvent::NoMirrorsFound => {
                        eprintln!("no mirrors found");
                    },
                    // maybe print?
                    SaveMirrorlistEvent::MirrorsFound(ids) => {
                        eprintln!("found {} mirrors", ids.len());
                    },
                    // also notify the user
                    SaveMirrorlistEvent::MirrorlistSaveSuccess { path } => {
                        eprintln!("mirrorlist saved to {}", path.display());
                    },
                    // most definitely notify the user
                    SaveMirrorlistEvent::MirrorlistSaveFailure { path, error } => {
                        eprintln!("failed to save mirrorlist to {}: {error}", path.display());
                    },
                },
                Event::CliParse(_) | Event::InitMirrorlist(..) => unreachable!("we already have the mirrorlist"),
            }
        }
    })
}
