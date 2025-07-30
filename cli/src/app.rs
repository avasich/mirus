use std::sync::Arc;

use mirus::mirror::Mirrorlist;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::event::{CliParseEvent, Event, InitMirrorlistEvent, SaveMirrorlistEvent};


pub fn create_app() -> (impl Future<Output = Result<(), crate::error::Error>>, UnboundedReceiver<Event>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (run(tx), rx)
}


async fn run(tx: UnboundedSender<Event>) -> Result<(), crate::error::Error> {
    let (config, pipeline) = crate::parser::parse().inspect_err(|_| {
        let _ = tx.send(Event::CliParse(CliParseEvent::Error));
    })?;

    let _ = tx.send(Event::CliParse(CliParseEvent::Parsed { verbose: config.verbose }));

    let mirrorlist = config.cache_file.as_ref().filter(|_| !config.cache_ttl.is_zero()).and_then(|cache| {
        let _ = tx.send(Event::InitMirrorlist(InitMirrorlistEvent::CheckingCache { path: cache.clone() }));

        let metadata = cache.metadata().ok()?;
        let modified = metadata.modified().ok()?;
        let elapsed = modified.elapsed().ok();

        if elapsed.is_none_or(|elapsed| elapsed >= config.cache_ttl) {
            let _ = tx.send(Event::InitMirrorlist(InitMirrorlistEvent::CacheExpired));
            return None;
        }

        Mirrorlist::from_file(cache)
            .inspect_err(|_| {
                let _ = tx.send(Event::InitMirrorlist(InitMirrorlistEvent::CacheReadFailure));
            })
            .ok()
            .filter(|mirrorlist| mirrorlist.url.as_ref() == Some(&config.mirrorlist_url))
    });

    let mirrorlist = if let Some(mirrorlist) = mirrorlist {
        mirrorlist
    } else {
        let url = config.mirrorlist_url.clone();
        let _ = tx.send(Event::InitMirrorlist(InitMirrorlistEvent::Fetching { url: url.clone() }));
        let mirrorlist = Mirrorlist::fetch(url.clone()).await.inspect_err(|_| {
            let _ = tx.send(Event::InitMirrorlist(InitMirrorlistEvent::FetchFailure));
        })?;

        if let Some(cache) = config.cache_file {
            let event = match mirrorlist.to_file(&cache) {
                Ok(()) => InitMirrorlistEvent::CacheSaveSuccess { path: cache },
                Err(e) => InitMirrorlistEvent::CacheSaveFailure {
                    path: cache,
                    error: e.to_string(),
                },
            };
            let _ = tx.send(Event::InitMirrorlist(event));
        }

        mirrorlist
    };

    let mirrorlist = Arc::new(mirrorlist);
    let _ = tx.send(Event::InitMirrorlist(InitMirrorlistEvent::Success {
        mirrorlist: Arc::clone(&mirrorlist),
    }));

    let mirrors = mirrorlist
        .apply_pipeline(pipeline, |id, event| {
            let _ = tx.send(Event::Measure { id, event });
        })
        .await;

    if mirrors.is_empty() {
        let _ = tx.send(Event::SaveMirrorlist(SaveMirrorlistEvent::NoMirrorsFound));
        return Ok(());
    }

    let _ = tx.send(Event::SaveMirrorlist(SaveMirrorlistEvent::MirrorsFound(
        mirrors.iter().map(|mirror| mirror.id).collect(),
    )));

    if let Some(path) = config.save_file {
        let event = match Mirrorlist::save_formatted(&path, mirrors) {
            Ok(()) => SaveMirrorlistEvent::MirrorlistSaveSuccess { path },
            Err(e) => SaveMirrorlistEvent::MirrorlistSaveFailure { path, error: e.to_string() },
        };
        let _ = tx.send(Event::SaveMirrorlist(event));
    }

    Ok(())
}
