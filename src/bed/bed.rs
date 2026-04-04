use std::fmt::{Debug, Display};
// use std::collections::HashMap;
use std::fmt;

use std::sync::atomic::AtomicUsize;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncSeek, AsyncSeekExt, AsyncRead, SeekFrom};

pub use crate::bed::record::*;
// #[cfg(feature = "bigbed")]
// pub use crate::bed::bigbedrecord::*;
pub use crate::bed::extra::*;
// pub use crate::bed::parser::*;
// use crate::store::TidResolver;

use crate::error;

pub(crate) async fn detect_format_from_reader<
	B: AsyncRead + AsyncSeek + Send + Unpin + AsyncBufRead,
>(
	name: String,
	reader: &mut B,
	max_lines: usize,
) -> error::Result<BedKind>
{
	let mut accumulated = Vec::new();
	let mut line = String::new();

	for _ in 0..max_lines
	{
		line.clear();
		let bytes_read = reader
			.read_line(&mut line)
			.await
			.map_err(|_| error::Error::BedFormat(name.clone()))?;
		if bytes_read == 0
		{
			break; // EOF
		}

		accumulated.push(line.clone());

		if let Ok(format) = BedKind::try_from(&accumulated)
		{
			reader.seek(SeekFrom::Start(0)).await?;
			return Ok(format);
		}
	}

	Err(error::Error::BedFormat(name))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct SourceId(pub usize);

impl From<usize> for SourceId
{
	fn from(id: usize) -> Self
	{
		SourceId(id)
	}
}

impl From<SourceId> for usize
{
	fn from(source_id: SourceId) -> Self
	{
		source_id.0
	}
}

impl std::fmt::Display for SourceId
{
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
	{
		write!(f, "{}", self.0)
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ReaderId(pub usize);

impl From<usize> for ReaderId
{
	fn from(id: usize) -> Self
	{
		ReaderId(id)
	}
}

impl From<ReaderId> for usize
{
	fn from(reader_id: ReaderId) -> Self
	{
		reader_id.0
	}
}

impl From<&ReaderId> for usize
{
	fn from(reader_id: &ReaderId) -> Self
	{
		reader_id.0
	}
}

impl std::fmt::Display for ReaderId
{
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
	{
		write!(f, "{}", self.0)
	}
}

pub(crate) static NEXT_READER_ID: AtomicUsize = AtomicUsize::new(0);

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

impl TryFrom<&Vec<String>> for BedKind
{
	type Error = error::Error;

	fn try_from(bed_lines: &Vec<String>) -> error::Result<Self>
	{
		for line in bed_lines
		{
			let trimmed = line.trim();

			if trimmed.is_empty()
			{
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

			return Ok(kind);
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

impl From<u8> for Strand
{
	fn from(strand_byte: u8) -> Self
	{
		match strand_byte
		{
			b'+' => Strand::Plus,
			b'-' => Strand::Minus,
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
