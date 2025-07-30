use reqwest::StatusCode;

#[derive(Debug)]
pub enum Error {
    InvalidMirror(String),
    Io(std::io::Error),
    Network(NetworkError),
    Serde(serde_json::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMirror(msg) => write!(f, "invalid mirror: {msg}"),
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Network(e) => write!(f, "network error: {e:?}"),
            Self::Serde(e) => write!(f, "serialization error: {e:?}"),
        }
    }
}

impl std::error::Error for Error {}


#[derive(Debug, Clone)]
pub enum NetworkError {
    Connect,
    Decode,
    Io,
    Status(StatusCode),
    Timeout,
    Other(String),
}

impl std::fmt::Display for NetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect => write!(f, "connection error"),
            Self::Decode => write!(f, "error decoding response body"),
            Self::Io => write!(f, "network io error"),
            Self::Status(status) => write!(f, "status {status}"),
            Self::Timeout => write!(f, "connection timeout"),
            Self::Other(message) => write!(f, "network error: {message}"),
        }
    }
}

impl std::error::Error for NetworkError {}

impl From<reqwest::Error> for NetworkError {
    fn from(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout
        } else if let Some(status) = error.status() {
            Self::Status(status)
        } else if error.is_connect() {
            Self::Connect
        } else if error.is_decode() {
            Self::Decode
        } else {
            Self::Other(error.to_string())
        }
    }
}

impl From<NetworkError> for Error {
    fn from(error: NetworkError) -> Self {
        Self::Network(error)
    }
}

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Self::Network(error.into())
    }
}


impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::Serde(error)
    }
}
