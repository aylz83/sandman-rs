use std::fmt::{Debug, Display};
use std::collections::HashMap;
use std::fmt;
// use std::sync::Arc;
// use tokio::sync::Mutex;

pub use crate::bed::record::*;
pub use crate::bed::extra::*;
pub use crate::bed::parser::*;
use crate::store::TidResolver;

use crate::error;

use async_trait::async_trait;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BedKind
{
	Bed3,
	Bed4,
	Bed5,
	Bed6,
	Bed12,
	BedMethyl,
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
		};
		f.write_str(s)
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct BedFormat
{
	pub kind: BedKind,
	pub has_tracks: bool,
	pub has_browsers: bool,
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
				has_tracks,
				has_browsers,
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
	use bincode::{Encode, Decode};

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
}

pub trait IntoAnyBedRecord<T>
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn into_any(self) -> AnyBedRecord<T>;
}

#[async_trait]
pub trait AutoBedRecord<T>: Debug + Clone + Send + Sync
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn tid(&self) -> &T::Tid;
	async fn pretty_tid(&self) -> Option<String>;

	fn start(&self) -> u64;
	fn end(&self) -> u64;
	fn name(&self) -> Option<&str>
	{
		None
	}
	fn score(&self) -> Option<u32>
	{
		None
	}
	fn strand(&self) -> Option<&Strand>
	{
		None
	}

	// BED12 extras
	fn thick_start(&self) -> Option<u64>
	{
		None
	}
	fn thick_end(&self) -> Option<u64>
	{
		None
	}
	fn item_rgb(&self) -> &Option<String>
	{
		&None
	}
	fn block_count(&self) -> Option<u32>
	{
		None
	}
	fn block_sizes(&self) -> Option<&Vec<u32>>
	{
		None
	}
	fn block_starts(&self) -> Option<&Vec<u32>>
	{
		None
	}

	// BEDMethyl extras
	fn n_valid_cov(&self) -> Option<u32>
	{
		None
	}
	fn frac_mod(&self) -> Option<f32>
	{
		None
	}
	fn n_mod(&self) -> Option<u32>
	{
		None
	}
	fn n_canonical(&self) -> Option<u32>
	{
		None
	}
	fn n_other_mod(&self) -> Option<u32>
	{
		None
	}
	fn n_delete(&self) -> Option<u32>
	{
		None
	}
	fn n_fail(&self) -> Option<u32>
	{
		None
	}
	fn n_diff(&self) -> Option<u32>
	{
		None
	}
	fn n_nocall(&self) -> Option<u32>
	{
		None
	}

	fn get_score(&self, column: &str) -> Option<f32>
	{
		match column
		{
			"score" => self.score().map(|score| score as f32),
			"n_valid_cov" => self.n_valid_cov().map(|n_valid_cov| n_valid_cov as f32),
			"frac_mod" => self.frac_mod(),
			"n_mod" => self.n_mod().map(|n_mod| n_mod as f32),
			"n_canonical" => self.n_canonical().map(|n_canonical| n_canonical as f32),
			"n_other_mod" => self.n_other_mod().map(|n_other_mod| n_other_mod as f32),
			"n_delete" => self.n_delete().map(|n_delete| n_delete as f32),
			"n_fail" => self.n_fail().map(|n_fail| n_fail as f32),
			"n_diff" => self.n_diff().map(|n_diff| n_diff as f32),
			"n_nocall" => self.n_nocall().map(|n_nocall| n_nocall as f32),
			_ => None,
		}
	}
}

#[async_trait]
impl<T> AutoBedRecord<T> for AnyBedRecord<T>
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn tid(&self) -> &T::Tid
	{
		match self
		{
			Self::Bed3(r) => &r.tid,
			Self::Bed4(r) => &r.tid,
			Self::Bed5(r) => &r.tid,
			Self::Bed6(r) => &r.tid,
			Self::Bed12(r) => &r.tid,
			Self::BedMethyl(r) => &r.tid,
		}
	}
	async fn pretty_tid(&self) -> Option<String>
	{
		let mut r = match self
		{
			Self::Bed3(r) => r.resolver.lock().await,
			Self::Bed4(r) => r.resolver.lock().await,
			Self::Bed5(r) => r.resolver.lock().await,
			Self::Bed6(r) => r.resolver.lock().await,
			Self::Bed12(r) => r.resolver.lock().await,
			Self::BedMethyl(r) => r.resolver.lock().await,
		};

		r.from_symbol_id(&self.tid()).map(|s| s.to_string())
	}
	fn start(&self) -> u64
	{
		match self
		{
			Self::Bed3(r) => r.start,
			Self::Bed4(r) => r.start,
			Self::Bed5(r) => r.start,
			Self::Bed6(r) => r.start,
			Self::Bed12(r) => r.start,
			Self::BedMethyl(r) => r.start,
		}
	}
	fn end(&self) -> u64
	{
		match self
		{
			Self::Bed3(r) => r.end,
			Self::Bed4(r) => r.end,
			Self::Bed5(r) => r.end,
			Self::Bed6(r) => r.end,
			Self::Bed12(r) => r.end,
			Self::BedMethyl(r) => r.end,
		}
	}

	fn name(&self) -> Option<&str>
	{
		match self
		{
			Self::Bed4(r) => Some(&r.fields.name),
			Self::Bed5(r) => Some(&r.fields.name),
			Self::Bed6(r) => Some(&r.fields.name),
			Self::Bed12(r) => Some(&r.fields.name),
			Self::BedMethyl(r) => Some(&r.fields.name),
			_ => None,
		}
	}

	fn score(&self) -> Option<u32>
	{
		match self
		{
			Self::Bed5(r) => r.fields.score,
			Self::Bed6(r) => r.fields.score,
			Self::Bed12(r) => r.fields.score,
			Self::BedMethyl(r) => r.fields.score,
			_ => None,
		}
	}

	fn strand(&self) -> Option<&Strand>
	{
		match self
		{
			Self::Bed6(r) => Some(&r.fields.strand),
			Self::Bed12(r) => Some(&r.fields.strand),
			Self::BedMethyl(r) => Some(&r.fields.strand),
			_ => None,
		}
	}

	fn thick_start(&self) -> Option<u64>
	{
		match self
		{
			Self::Bed12(r) => Some(r.fields.thick_start),
			_ => None,
		}
	}
	fn thick_end(&self) -> Option<u64>
	{
		match self
		{
			Self::Bed12(r) => Some(r.fields.thick_end),
			_ => None,
		}
	}
	fn item_rgb(&self) -> &Option<String>
	{
		match self
		{
			Self::Bed12(r) => &r.fields.item_rgb,
			_ => &None,
		}
	}
	fn block_count(&self) -> Option<u32>
	{
		match self
		{
			Self::Bed12(r) => Some(r.fields.block_count),
			_ => None,
		}
	}
	fn block_sizes(&self) -> Option<&Vec<u32>>
	{
		match self
		{
			Self::Bed12(r) => Some(&r.fields.block_sizes),
			_ => None,
		}
	}
	fn block_starts(&self) -> Option<&Vec<u32>>
	{
		match self
		{
			Self::Bed12(r) => Some(&r.fields.block_starts),
			_ => None,
		}
	}

	fn n_valid_cov(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_valid_cov),
			_ => None,
		}
	}
	fn frac_mod(&self) -> Option<f32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.frac_mod),
			_ => None,
		}
	}
	fn n_mod(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_mod),
			_ => None,
		}
	}
	fn n_canonical(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_canonical),
			_ => None,
		}
	}
	fn n_other_mod(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_other_mod),
			_ => None,
		}
	}
	fn n_delete(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_delete),
			_ => None,
		}
	}
	fn n_fail(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_fail),
			_ => None,
		}
	}
	fn n_diff(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_diff),
			_ => None,
		}
	}
	fn n_nocall(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_nocall),
			_ => None,
		}
	}
}
