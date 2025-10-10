use thiserror::Error;

use pufferfish::error::Error as PufferfishError;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Not in tabix format")]
    TabixFormat,
    #[error("Not in BED format")]
    BedFormat,
    #[error("No index found")]
    NoIndex,
    #[error("Unable to read BGZ block")]
    BGZBlock,
    #[error(transparent)]
    Pufferfish(#[from] PufferfishError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
