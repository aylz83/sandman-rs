use thiserror::Error;

use pufferfish::error::Error as PufferfishError;
use lexical_core::Error as LexicalCoreError;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum Error
{
	#[error("Invalid ScoreField value {0}")]
	InvalidScoreField(String),
	#[error("Unexpected end of file")]
	UnexpectedEof,
	#[error("Tid {0} not found with region {1} when looking for modification {2} in the mappings")]
	TidRegionNotFound(String, u64, String),
	#[error("Not currently implemented")]
	NotImplemented,
	#[error("FieldValue variant mismatch")]
	VariantMismatch,
	#[error("Unable to find tid {0} in index")]
	TidNotFound(String),
	#[error("Base mapping {1} not found when looking for  mappings {0} with expected base {2}")]
	BaseLookupFailed(String, String, String),
	#[error("Invalid tid region: start is {0}, end is {1} and the tid size is {2}")]
	InvalidTidRegion(u64, u64, u64),
	#[error("Invalid Char length {0} in AutoSQL field")]
	InvalidCharLength(String),
	#[error("Missing type in AutoSQL field")]
	MissingAutoSQLType,
	#[error("Missing field name in AutoSQL field")]
	MissingAutoSQLField,
	#[error("File loaded is not in bigBed format")]
	NotBigBed,
	#[error("Reading lines in the {0} format not supported")]
	ReadLineNotSupported(String),
	#[error("Memory error")]
	Memory,
	#[error(
		"Supplied scratch bed record ({0}) must match the format of the loaded bed file ({1})"
	)]
	BedFormatMismatch(String, String),
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
	#[error("Unable to read file as not in {0} format")]
	BedMismatch(String),
	#[error("No index for BED {0} found")]
	NoIndex(String),
	#[error("Associated Tabix file for BED {0} not open")]
	TabixNotOpen(String),
	#[error(transparent)]
	Pufferfish(#[from] PufferfishError),
	#[error(transparent)]
	LexicalCore(#[from] LexicalCoreError),
	#[error("IO error: {0}")]
	Io(#[from] std::io::Error),
}
