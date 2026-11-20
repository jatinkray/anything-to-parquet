use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("arrow: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("csv: {0}")]
    Csv(#[from] csv::Error),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("invalid timestamp in column `{column}` near record {record}: {value:?}")]
    BadTimestamp {
        column: String,
        record: u64,
        value: String,
    },
    #[error("schema conflict in column `{column}` at record {record}: {detail}")]
    SchemaConflict {
        column: String,
        record: u64,
        detail: String,
    },
    #[error("source error: {0}")]
    Source(String),
    #[error("verification failed: {0}")]
    Verify(String),
    #[error("cancelled")]
    Cancelled,
    #[error("{0}")]
    Msg(String),
    #[error("cannot {action} `{path}`: {reason}")]
    Path {
        action: &'static str,
        path: PathBuf,
        reason: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn msg(s: impl Into<String>) -> Self {
        Error::Msg(s.into())
    }

    pub fn source_err(s: impl Into<String>) -> Self {
        Error::Source(s.into())
    }

    /// Process exit code that matches the documented CLI contract.
    pub fn exit_code(&self) -> i32 {
        match self {
            Error::SchemaConflict { .. } | Error::BadTimestamp { .. } => 3,
            Error::Verify(_) => 4,
            Error::Http(_) | Error::Source(_) => 5,
            Error::Cancelled => 130,
            _ => 1,
        }
    }
}
