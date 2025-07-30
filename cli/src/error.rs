#[derive(Debug)]
pub enum Error {
    CliError(clap::Error),
    MirusError(mirus::Error),
    JoinError(tokio::task::JoinError),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CliError(e) => write!(f, "cli error: {e}"),
            Self::MirusError(e) => write!(f, "error: {e}"),
            Self::JoinError(e) => write!(f, "tokio error: {e}"),
        }
    }
}


impl From<mirus::Error> for Error {
    fn from(error: mirus::Error) -> Self {
        Self::MirusError(error)
    }
}


impl From<clap::Error> for Error {
    fn from(error: clap::Error) -> Self {
        Self::CliError(error)
    }
}


impl From<tokio::task::JoinError> for Error {
    fn from(error: tokio::task::JoinError) -> Self {
        Self::JoinError(error)
    }
}
