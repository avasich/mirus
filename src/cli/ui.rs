use std::{collections::HashMap, sync::Arc};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
    event::{Event, LoadMirrorlistEvent, MeasureEvent, SaveMirrorlistEvent},
    mirror::{MirrorId, Mirrorlist},
};


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
                use crate::error::NetworkError;
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


pub fn start_ui(mut rx: UnboundedReceiver<Event>) {
    tokio::spawn({
        async move {
            while let Some(event) = rx.recv().await {
                match event {
                    Event::CliParsingError => todo!(),
                    Event::LoadMirrorlist(event) => match event {
                        LoadMirrorlistEvent::Success(mirrorlist) => todo!(),
                        LoadMirrorlistEvent::CacheExpired => todo!(),
                        LoadMirrorlistEvent::CacheReadFailure => todo!(),
                        LoadMirrorlistEvent::Fetching => todo!(),
                        LoadMirrorlistEvent::FetchFailure => todo!(),
                    },
                    Event::SaveMirrorlist(event) => match event {
                        SaveMirrorlistEvent::CacheSaveSuccess => todo!(),
                        SaveMirrorlistEvent::CacheSaveFailure => todo!(),
                        SaveMirrorlistEvent::MirrorlistSaveSuccess => todo!(),
                        SaveMirrorlistEvent::MirrorlistSaveFailure => todo!(),
                    },
                    Event::Measure { id, event } => todo!(),
                    Event::NoMirrorsFound => todo!(),
                }
            }
        }
    });
}
