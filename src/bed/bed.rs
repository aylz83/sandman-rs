use std::fmt::{Debug, Display};
use std::collections::HashMap;
use std::fmt;

pub use crate::bed::record::*;
#[cfg(feature = "bigbed")]
pub use crate::bed::bigbedrecord::*;
pub use crate::bed::extra::*;
pub use crate::bed::parser::*;
use crate::store::TidResolver;

use crate::error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BedKind
{
	Bed3,
	Bed4,
	Bed5,
	Bed6,
	Bed12,
	BedMethyl,
	#[cfg(feature = "bigbed")]
	BigBed,
}

impl fmt::Display for BedKind
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
	{
		let s = match self
		{
			BedKind::Bed3 => "BED3",
			BedKind::Bed4 => "BED4",
			BedKind::Bed5 => "BED5",
			BedKind::Bed6 => "BED6",
			BedKind::Bed12 => "BED12",
			BedKind::BedMethyl => "BEDMethyl",
			#[cfg(feature = "bigbed")]
			BedKind::BigBed => "BigBed",
		};
		f.write_str(s)
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct BedFormat
{
	pub kind: BedKind,
	pub has_tracks: Option<bool>,
	pub has_browsers: Option<bool>,
}

impl TryFrom<&Vec<String>> for BedFormat
{
	type Error = error::Error;

	fn try_from(bed_lines: &Vec<String>) -> error::Result<Self>
	{
		let mut has_tracks = false;
		let mut has_browsers = false;

		for line in bed_lines
		{
			let trimmed = line.trim();

			if trimmed.is_empty()
			{
				continue;
			}
			else if trimmed.starts_with("track")
			{
				has_tracks = true;
				continue;
			}
			else if trimmed.starts_with("browser")
			{
				has_browsers = true;
				continue;
			}

			let count = trimmed.split_whitespace().count();
			let kind = match count
			{
				3 => BedKind::Bed3,
				4 => BedKind::Bed4,
				5 => BedKind::Bed5,
				6 => BedKind::Bed6,
				12 => BedKind::Bed12,
				18 => BedKind::BedMethyl,
				_ => return Err(error::Error::Parse(trimmed.to_string())),
			};

			return Ok(BedFormat {
				kind,
				has_tracks: Some(has_tracks),
				has_browsers: Some(has_browsers),
			});
		}

		Err(error::Error::AutoDetect)
	}
}

#[cfg_attr(feature = "bincode", derive(bincode::Encode, bincode::Decode))]
#[derive(PartialOrd, Ord, Eq, Hash, PartialEq, Debug, Clone, Copy, Default)]
pub enum Strand
{
	Plus,
	Minus,
	#[default]
	Both,
}

impl From<&str> for Strand
{
	fn from(strand_str: &str) -> Self
	{
		match strand_str
		{
			"+" => Strand::Plus,
			"-" => Strand::Minus,
			_ => Strand::Both,
		}
	}
}

impl Display for Strand
{
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error>
	{
		match self
		{
			Strand::Plus => write!(f, "+"),
			Strand::Minus => write!(f, "-"),
			Strand::Both => write!(f, "."),
		}
	}
}

#[cfg(feature = "bincode")]
mod bincode_utils
{
	use super::Strand;

	pub fn serialize(data: &Strand) -> Vec<u8>
	{
		bincode::encode_to_vec(data, bincode::config::standard()).unwrap()
	}

	pub fn deserialize(bytes: &[u8]) -> Strand
	{
		bincode::decode_from_slice(bytes, bincode::config::standard())
			.unwrap()
			.0
	}
}

#[derive(Debug, Clone, Default)]
pub struct Track
{
	pub name: Option<String>,
	pub description: Option<String>,
	pub visibility: Option<u8>,
	pub item_rgb: Option<String>,
	pub color: Option<String>,
	pub use_score: Option<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct BrowserMeta
{
	pub attrs: HashMap<String, String>,
}

impl BrowserMeta
{
	pub fn get(&self, key: &str) -> Option<&str>
	{
		self.attrs.get(key).map(|s| s.as_str())
	}
}

#[derive(Clone, Debug)]
pub enum AnyBedRecord<T>
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	Bed3(BedRecord<T, T::Tid, Bed3Fields>),
	Bed4(BedRecord<T, T::Tid, Bed4Extra>),
	Bed5(BedRecord<T, T::Tid, Bed5Extra>),
	Bed6(BedRecord<T, T::Tid, Bed6Extra>),
	Bed12(BedRecord<T, T::Tid, Bed12Extra>),
	BedMethyl(BedRecord<T, T::Tid, BedMethylExtra>),
	#[cfg(feature = "bigbed")]
	BigBed(BedRecord<T, T::Tid, BigBedExtra>),
}

pub trait IntoAnyBedRecord<T>
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn into_any(self) -> AnyBedRecord<T>;
}
