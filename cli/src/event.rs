use std::{path::PathBuf, sync::Arc};

use mirus::{
    measure::MeasureEvent,
    mirror::{MirrorId, Mirrorlist},
};
use url::Url;


#[derive(Debug, Clone)]
pub enum Event {
    CliParse(CliParseEvent),
    InitMirrorlist(InitMirrorlistEvent),
    Measure { id: MirrorId, event: MeasureEvent },
    SaveMirrorlist(SaveMirrorlistEvent),
}


#[derive(Debug, Clone)]
pub enum CliParseEvent {
    Error,
    Parsed { verbose: bool },
}

#[derive(Debug, Clone)]
pub enum InitMirrorlistEvent {
    Success { mirrorlist: Arc<Mirrorlist> },
    CheckingCache { path: PathBuf },
    CacheExpired,
    CacheReadFailure,
    Fetching { url: Url },
    FetchFailure,
    CacheSaveSuccess { path: PathBuf },
    CacheSaveFailure { path: PathBuf, error: String },
}


#[derive(Debug, Clone)]
pub enum SaveMirrorlistEvent {
    NoMirrorsFound,
    MirrorsFound(Vec<MirrorId>),
    MirrorlistSaveSuccess { path: PathBuf },
    MirrorlistSaveFailure { path: PathBuf, error: String },
}
