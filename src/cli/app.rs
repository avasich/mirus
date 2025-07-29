use std::{path::PathBuf, sync::Arc};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    event::{Event, LoadMirrorlistEvent, SaveMirrorlistEvent},
    mirror::{Mirror, Mirrorlist},
};


pub fn create_app() -> (impl Future<Output = Result<(), crate::Error>>, UnboundedReceiver<Event>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (run(tx), rx)
}


async fn run(tx: UnboundedSender<Event>) -> Result<(), crate::Error> {
    let (config, pipeline) = super::parser::parse().inspect_err(|_| {
        let _ = tx.send(Event::CliParsingError);
    })?;

    let cache = std::env::var("XDG_CACHE_HOME")
        .ok()
        .map(PathBuf::from)
        .or_else(|| std::env::home_dir().map(|home| home.join(".cache")))
        .map(|cache_dir| cache_dir.join("mirus.json"));

    let mirrorlist = cache.as_ref().and_then(|cache| {
        let metadata = cache.metadata().ok()?;
        let modified = metadata.modified().ok()?;
        let elapsed = modified.elapsed().ok()?;

        if elapsed < config.cache_ttl {
            Mirrorlist::from_file(cache)
                .inspect_err(|_| {
                    let _ = tx.send(Event::LoadMirrorlist(LoadMirrorlistEvent::CacheReadFailure));
                })
                .ok()
        } else {
            let _ = tx.send(Event::LoadMirrorlist(LoadMirrorlistEvent::CacheExpired));
            None
        }
    });

    let mirrorlist = if let Some(mirrorlist) = mirrorlist {
        mirrorlist
    } else {
        let _ = tx.send(Event::LoadMirrorlist(LoadMirrorlistEvent::Fetching));
        let mirrorlist = Mirrorlist::fetch(config.mirrorlist_url).await.inspect_err(|_| {
            let _ = tx.send(Event::LoadMirrorlist(LoadMirrorlistEvent::FetchFailure));
        })?;

        if let Some(cache) = cache {
            let event = match mirrorlist.to_file(&cache) {
                Ok(()) => SaveMirrorlistEvent::CacheSaveSuccess,
                Err(_) => SaveMirrorlistEvent::CacheSaveFailure,
            };
            let _ = tx.send(Event::SaveMirrorlist(event));
        }

        mirrorlist
    };

    let mirrorlist = Arc::new(mirrorlist);
    let _ = tx.send(Event::LoadMirrorlist(LoadMirrorlistEvent::Success(Arc::clone(&mirrorlist))));

    let mirrors = mirrorlist
        .apply_pipeline(pipeline, |event| {
            let _ = tx.send(event);
        })
        .await;

    if mirrors.is_empty() {
        let _ = tx.send(Event::NoMirrorsFound);
    }

    if let Some(path) = config.save_file {
        let event = match std::fs::File::create(path).and_then(|mut file| format_mirrors(&mut file, &mirrors)) {
            Ok(()) => SaveMirrorlistEvent::MirrorlistSaveSuccess,
            Err(_) => SaveMirrorlistEvent::MirrorlistSaveFailure,
        };
        let _ = tx.send(Event::SaveMirrorlist(event));
    }

    Ok(())
}

fn format_mirrors(f: &mut impl std::io::Write, mirrors: &[&Mirror]) -> Result<(), std::io::Error> {
    mirrors.iter().try_for_each(|mirror| writeln!(f, "Server = {}/$repo/os/$arch", mirror.url))
}
