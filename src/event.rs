use std::{sync::Arc, time::Duration};

use crate::{
    error::NetworkError,
    mirror::{MirrorId, Mirrorlist, Rate},
};

pub trait Callback: FnMut(Event) + Send + Clone {}
impl<F> Callback for F where F: FnMut(Event) + Send + Clone {}


#[derive(Debug, Clone)]
pub enum Event {
    CliParsingError,
    LoadMirrorlist(LoadMirrorlistEvent),
    SaveMirrorlist(SaveMirrorlistEvent),
    Measure { id: MirrorId, event: MeasureEvent },
    NoMirrorsFound,
}


#[derive(Debug, Clone)]
pub enum LoadMirrorlistEvent {
    Success(Arc<Mirrorlist>),
    CacheExpired,
    CacheReadFailure,
    Fetching,
    FetchFailure,
}


#[derive(Debug, Clone)]
pub enum SaveMirrorlistEvent {
    CacheSaveSuccess,
    CacheSaveFailure,
    MirrorlistSaveSuccess,
    MirrorlistSaveFailure,
}


#[derive(Debug, Clone)]
pub enum MeasureEvent {
    Connecting,
    Connected { connection_time: Duration, file_size: Option<u64> },
    BytesReceived { rate: Rate },
    Finished { rate: Rate },
    Failed(NetworkError),
}
