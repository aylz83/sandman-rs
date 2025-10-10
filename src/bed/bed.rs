use nom::bytes::complete::is_not;
use nom::character::complete::{digit1, line_ending, multispace1};
use nom::IResult;
use nom::Parser;
use nom::combinator::map_res;

use std::fmt::Display;

pub struct Bed3Fields;

#[derive(Eq, Hash, PartialEq, Debug, Clone)]
pub enum Strand
{
	Plus,
	Minus,
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

#[derive(Debug, Clone)]
pub struct BedRecord<F>
{
	pub chrom: String,
	pub start: u32,
	pub end: u32,
	pub fields: F,
}

#[derive(Debug, Clone)]
pub struct Bed4Extra
{
	pub name: String,
}

#[derive(Debug, Clone)]
pub struct Bed5Extra
{
	pub name: String,
	pub score: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct Bed6Extra
{
	pub name: String,
	pub score: Option<u32>,
	pub strand: Strand,
}

#[derive(Debug, Clone)]
pub struct Bed12Extra
{
	pub name: String,
	pub score: Option<u32>,
	pub strand: Strand,
	pub thick_start: u32,
	pub thick_end: u32,
	pub item_rgb: Option<String>,
	pub block_count: u32,
	pub block_sizes: Vec<u32>,
	pub block_starts: Vec<u32>,
}

impl BedRecord<Bed3Fields>
{
	pub fn new(chrom: impl Into<String>, start: u32, end: u32) -> Self
	{
		Self {
			chrom: chrom.into(),
			start,
			end,
			fields: Bed3Fields,
		}
	}

	pub fn with_name(self, name: impl Into<String>) -> BedRecord<Bed4Extra>
	{
		BedRecord {
			chrom: self.chrom,
			start: self.start,
			end: self.end,
			fields: Bed4Extra { name: name.into() },
		}
	}
}

impl BedRecord<Bed4Extra>
{
	pub fn with_score(self, score: Option<u32>) -> BedRecord<Bed5Extra>
	{
		BedRecord {
			chrom: self.chrom,
			start: self.start,
			end: self.end,
			fields: Bed5Extra {
				name: self.fields.name,
				score,
			},
		}
	}
}

impl BedRecord<Bed5Extra>
{
	pub fn with_strand(self, strand: Strand) -> BedRecord<Bed6Extra>
	{
		BedRecord {
			chrom: self.chrom,
			start: self.start,
			end: self.end,
			fields: Bed6Extra {
				name: self.fields.name,
				score: self.fields.score,
				strand,
			},
		}
	}
}

impl BedRecord<Bed6Extra>
{
	pub fn with_bed12(
		self,
		thick_start: u32,
		thick_end: u32,
		item_rgb: Option<String>,
		block_count: u32,
		block_sizes: Vec<u32>,
		block_starts: Vec<u32>,
	) -> BedRecord<Bed12Extra>
	{
		BedRecord {
			chrom: self.chrom,
			start: self.start,
			end: self.end,
			fields: Bed12Extra {
				name: self.fields.name,
				score: self.fields.score,
				strand: self.fields.strand,
				thick_start,
				thick_end,
				item_rgb,
				block_count,
				block_sizes,
				block_starts,
			},
		}
	}
}

pub trait BedLine
{
	fn chrom(&self) -> &str;
	fn start(&self) -> u32;
	fn end(&self) -> u32;
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
	fn thick_start(&self) -> Option<u32>
	{
		None
	}
	fn thick_end(&self) -> Option<u32>
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
}

impl BedLine for BedRecord<Bed3Fields>
{
	fn chrom(&self) -> &str
	{
		&self.chrom
	}
	fn start(&self) -> u32
	{
		self.start
	}
	fn end(&self) -> u32
	{
		self.end
	}
}

impl BedLine for BedRecord<Bed4Extra>
{
	fn chrom(&self) -> &str
	{
		&self.chrom
	}
	fn start(&self) -> u32
	{
		self.start
	}
	fn end(&self) -> u32
	{
		self.end
	}
	fn name(&self) -> Option<&str>
	{
		Some(&self.fields.name)
	}
}

impl BedLine for BedRecord<Bed5Extra>
{
	fn chrom(&self) -> &str
	{
		&self.chrom
	}
	fn start(&self) -> u32
	{
		self.start
	}
	fn end(&self) -> u32
	{
		self.end
	}
	fn name(&self) -> Option<&str>
	{
		Some(&self.fields.name)
	}
	fn score(&self) -> Option<u32>
	{
		self.fields.score
	}
}

impl BedLine for BedRecord<Bed6Extra>
{
	fn chrom(&self) -> &str
	{
		&self.chrom
	}
	fn start(&self) -> u32
	{
		self.start
	}
	fn end(&self) -> u32
	{
		self.end
	}
	fn name(&self) -> Option<&str>
	{
		Some(&self.fields.name)
	}
	fn score(&self) -> Option<u32>
	{
		self.fields.score
	}
	fn strand(&self) -> Option<&Strand>
	{
		Some(&self.fields.strand)
	}
}

impl BedLine for BedRecord<Bed12Extra>
{
	fn chrom(&self) -> &str
	{
		&self.chrom
	}
	fn start(&self) -> u32
	{
		self.start
	}
	fn end(&self) -> u32
	{
		self.end
	}
	fn name(&self) -> Option<&str>
	{
		Some(&self.fields.name)
	}
	fn score(&self) -> Option<u32>
	{
		self.fields.score
	}
	fn strand(&self) -> Option<&Strand>
	{
		Some(&self.fields.strand)
	}
	fn thick_start(&self) -> Option<u32>
	{
		Some(self.fields.thick_start)
	}
	fn thick_end(&self) -> Option<u32>
	{
		Some(self.fields.thick_end)
	}
	fn item_rgb(&self) -> &Option<String>
	{
		&self.fields.item_rgb
	}
	fn block_count(&self) -> Option<u32>
	{
		Some(self.fields.block_count)
	}
	fn block_sizes(&self) -> Option<&Vec<u32>>
	{
		Some(&self.fields.block_sizes)
	}
	fn block_starts(&self) -> Option<&Vec<u32>>
	{
		Some(&self.fields.block_starts)
	}
}

fn parse_string(input: &[u8]) -> IResult<&[u8], String>
{
	map_res(is_not(" \t\r\n"), |s: &[u8]| {
		std::str::from_utf8(s).map(|s| s.to_string())
	})
	.parse(input)
}

fn parse_u32(input: &[u8]) -> IResult<&[u8], u32>
{
	map_res(digit1, |s: &[u8]| {
		std::str::from_utf8(s).unwrap_or("0").parse::<u32>()
	})
	.parse(input)
}

fn parse_strand(input: &[u8]) -> IResult<&[u8], Strand>
{
	map_res(is_not(" \t\r\n"), |s: &[u8]| {
		std::str::from_utf8(s).map(Strand::from)
	})
	.parse(input)
}

pub(crate) fn parse_bed3_record(input: &[u8]) -> IResult<&[u8], BedRecord<Bed3Fields>>
{
	let (input, (chrom, _, start, _, end, _)) = (
		parse_string,
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		line_ending,
	)
		.parse(input)?;
	Ok((input, BedRecord::new(chrom, start, end)))
}

pub(crate) fn parse_bed4_record(input: &[u8]) -> IResult<&[u8], BedRecord<Bed4Extra>>
{
	let (input, (chrom, _, start, _, end, _, name, _)) = (
		parse_string,
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		multispace1,
		parse_string,
		line_ending,
	)
		.parse(input)?;
	Ok((input, BedRecord::new(chrom, start, end).with_name(name)))
}

pub(crate) fn parse_bed5_record(input: &[u8]) -> IResult<&[u8], BedRecord<Bed5Extra>>
{
	let (input, (chrom, _, start, _, end, _, name, _, score, _)) = (
		parse_string,
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		multispace1,
		parse_string,
		multispace1,
		parse_u32,
		line_ending,
	)
		.parse(input)?;
	Ok((
		input,
		BedRecord::new(chrom, start, end)
			.with_name(name)
			.with_score(Some(score)),
	))
}

pub(crate) fn parse_bed6_record(input: &[u8]) -> IResult<&[u8], BedRecord<Bed6Extra>>
{
	let (input, (chrom, _, start, _, end, _, name, _, score, _, strand, _)) = (
		parse_string,
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		multispace1,
		parse_string,
		multispace1,
		parse_u32,
		multispace1,
		parse_strand,
		line_ending,
	)
		.parse(input)?;
	Ok((
		input,
		BedRecord::new(chrom, start, end)
			.with_name(name)
			.with_score(Some(score))
			.with_strand(strand),
	))
}

pub(crate) fn parse_bed12_record(input: &[u8]) -> IResult<&[u8], BedRecord<Bed12Extra>>
{
	let (input, (chrom, _, start, _, end, _, name, _, score, _, strand)) = (
		parse_string,
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		multispace1,
		parse_string,
		multispace1,
		parse_u32,
		multispace1,
		parse_strand,
	)
		.parse(input)?;

	let (
		input,
		(
			_,
			thick_start,
			_,
			thick_end,
			_,
			item_rgb,
			_,
			block_count,
			_,
			block_sizes_str,
			_,
			block_starts_str,
			_,
		),
	) = (
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		multispace1,
		parse_string,
		multispace1,
		parse_u32,
		multispace1,
		parse_string,
		multispace1,
		parse_string,
		line_ending,
	)
		.parse(input)?;

	let block_sizes = block_sizes_str
		.split(',')
		.filter(|s| !s.is_empty())
		.map(|s| s.parse::<u32>().unwrap_or(0))
		.collect::<Vec<_>>();
	let block_starts = block_starts_str
		.split(',')
		.filter(|s| !s.is_empty())
		.map(|s| s.parse::<u32>().unwrap_or(0))
		.collect::<Vec<_>>();

	Ok((
		input,
		BedRecord::new(chrom, start, end)
			.with_name(name)
			.with_score(Some(score))
			.with_strand(strand)
			.with_bed12(
				thick_start,
				thick_end,
				if item_rgb.is_empty()
				{
					None
				}
				else
				{
					Some(item_rgb)
				},
				block_count,
				block_sizes,
				block_starts,
			),
	))
}
