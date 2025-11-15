use thiserror::Error;

use pufferfish::error::Error as PufferfishError;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum Error
{
	#[error("Unable to read a region from uncompressed BED {0}")]
	PlainBedRegion(String),
	#[error("{0} not in tabix format")]
	TabixFormat(String),
	#[error("Unable to parse line - {0}")]
	Parse(String),
	#[error("Unable to auto detect bed format from data")]
	AutoDetect,
	#[error("{0} not in BED format")]
	BedFormat(String),
	#[error("No index for BED {0} found")]
	NoIndex(String),
	#[error("Associated Tabix file for BED {0} not open")]
	TabixNotOpen(String),
	#[error(transparent)]
	Pufferfish(#[from] PufferfishError),
	#[error("IO error: {0}")]
	Io(#[from] std::io::Error),
}
