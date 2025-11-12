use nom::character::complete::{space1, line_ending, multispace1};
use nom::{Parser, IResult};
use nom::number::complete::float;
use nom::sequence::delimited;
use nom::character::char;
use nom::bytes::complete::{is_not, take_while1, take_till1, tag};
use nom::combinator::{map_res, opt};
use nom::multi::many0;

use tokio::sync::Mutex;

use std::sync::Arc;
use std::fmt::Debug;
use std::collections::HashMap;

use crate::bed::BedLine;
use crate::bed::BrowserMeta;
use crate::bed::Track;
use crate::bed::Strand;
use crate::bed::BedRecord;
use crate::bed::Bed3Fields;
use crate::bed::Bed4Extra;
use crate::bed::Bed5Extra;
use crate::bed::Bed6Extra;
use crate::bed::Bed12Extra;
use crate::bed::BedMethylExtra;
use crate::store::TidResolver;
use crate::error;

fn is_key_char(c: char) -> bool
{
	c.is_alphanumeric() || c == '_' || c == '-'
}

fn parse_key(input: &str) -> IResult<&str, &str>
{
	take_while1(is_key_char).parse(input)
}

fn parse_value(input: &str) -> IResult<&str, &str>
{
	// value can be quoted or not
	let quoted = delimited(char('"'), is_not("\""), char('"'));
	let unquoted = take_till1(|c: char| c.is_whitespace());
	nom::branch::alt((quoted, unquoted)).parse(input)
}

fn parse_key_value_pair(input: &str) -> IResult<&str, (&str, &str)>
{
	let (input, (k, _, v)) = ((parse_key, tag("="), parse_value)).parse(input)?;
	Ok((input, (k, v)))
}

fn parse_browser_pair(input: &str) -> IResult<&str, (String, String)>
{
	let (input, key) = parse_key(input)?;
	let (input, _) = space1(input)?;

	// try key=value first
	if let Ok((rest, value)) = parse_value(input)
	{
		// check if original input has '=' between key and value
		if input.starts_with('=')
		{
			let value = value.trim_start_matches('=');
			return Ok((rest, (key.to_string(), value.to_string())));
		}
		else
		{
			return Ok((rest, (key.to_string(), value.to_string())));
		}
	}

	// fallback: key with empty value
	Ok((input, (key.to_string(), "".to_string())))
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
	map_res(take_while1(|c: u8| c.is_ascii_digit()), |bytes: &[u8]| {
		let mut val: u32 = 0;
		for &b in bytes
		{
			val = val
				.checked_mul(10)
				.and_then(|v| v.checked_add((b - b'0') as u32))
				.ok_or("overflow")?;
		}
		Ok::<u32, &str>(val)
	})
	.parse(input)
}

// fn parse_u32(input: &[u8]) -> IResult<&[u8], u32>
// {
// 	map_res(digit1, |s: &[u8]| {
// 		std::str::from_utf8(s).unwrap_or("0").parse::<u32>()
// 	})
// 	.parse(input)
// }

fn parse_f32(input: &[u8]) -> IResult<&[u8], f32>
{
	float(input).map(|(next, val)| (next, val as f32))
}

fn parse_strand(input: &[u8]) -> IResult<&[u8], Strand>
{
	map_res(is_not(" \t\r\n"), |s: &[u8]| {
		std::str::from_utf8(s).map(Strand::from)
	})
	.parse(input)
}

pub(crate) async fn parse_all_records<'a, Parser, Fut, Record, Tid, Resolver>(
	bytes: &'a [u8],
	resolver: impl Fn() -> Resolver,
	parser: Parser,
) -> error::Result<Vec<Box<dyn BedLine<Tid>>>>
where
	Parser: Fn(Resolver, &'a [u8]) -> Fut,
	Fut: Future<Output = IResult<&'a [u8], Record>>,
	Record: BedLine<Tid> + 'static,
	Tid: Debug + Clone + Send + Sync + PartialEq + 'static,
{
	let mut rest = bytes;
	let mut results = Vec::new();

	while !rest.is_empty()
	{
		let resolver = resolver();
		let (new_rest, record) = parser(resolver, rest)
			.await
			.map_err(|_| error::Error::BedFormat)?;
		rest = new_rest;
		results.push(Box::new(record) as Box<dyn BedLine<Tid>>);
	}

	Ok(results)
}

pub(crate) async fn parse_bed3_record<'a, R: TidResolver + std::fmt::Debug + std::clone::Clone>(
	resolver: Arc<Mutex<R>>,
	input: &'a [u8],
) -> IResult<&'a [u8], BedRecord<R, R::Tid, Bed3Fields>>
{
	let (input, (tid, _, start, _, end, _)) = (
		parse_string,
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		line_ending,
	)
		.parse(input)?;

	let tid_id = resolver.lock().await.to_symbol_id(&tid);

	Ok((
		input,
		BedRecord::new(Arc::clone(&resolver), tid_id, start, end),
	))
}

pub(crate) async fn parse_bed4_record<'a, R: TidResolver + std::fmt::Debug + std::clone::Clone>(
	resolver: Arc<Mutex<R>>,
	input: &'a [u8],
) -> IResult<&'a [u8], BedRecord<R, R::Tid, Bed4Extra>>
{
	let (input, (tid, _, start, _, end, _, name, _)) = (
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

	let tid_id = resolver.lock().await.to_symbol_id(&tid);

	Ok((
		input,
		BedRecord::new(Arc::clone(&resolver), tid_id, start, end)
			.clone()
			.with_name(name),
	))
}

pub(crate) async fn parse_bed5_record<'a, R: TidResolver + std::fmt::Debug + std::clone::Clone>(
	resolver: Arc<Mutex<R>>,
	input: &'a [u8],
) -> IResult<&'a [u8], BedRecord<R, R::Tid, Bed5Extra>>
{
	let (input, (tid, _, start, _, end, _, name, _, score, _)) = (
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

	let tid_id = resolver.lock().await.to_symbol_id(&tid);

	Ok((
		input,
		BedRecord::new(Arc::clone(&resolver), tid_id, start, end)
			.with_name(name)
			.with_score(Some(score)),
	))
}

pub(crate) async fn parse_bed6_record<'a, R: TidResolver + std::fmt::Debug + std::clone::Clone>(
	resolver: Arc<Mutex<R>>,
	input: &'a [u8],
) -> IResult<&'a [u8], BedRecord<R, R::Tid, Bed6Extra>>
{
	let (input, (tid, _, start, _, end, _, name, _, score, _, strand, _)) = (
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

	let tid_id = resolver.lock().await.to_symbol_id(&tid);

	Ok((
		input,
		BedRecord::new(Arc::clone(&resolver), tid_id, start, end)
			.with_name(name)
			.with_score(Some(score))
			.with_strand(strand),
	))
}

pub(crate) async fn parse_bed12_record<'a, R: TidResolver + std::fmt::Debug + std::clone::Clone>(
	resolver: Arc<Mutex<R>>,
	input: &'a [u8],
) -> IResult<&'a [u8], BedRecord<R, R::Tid, Bed12Extra>>
{
	let (input, (tid, _, start, _, end, _, name, _, score, _, strand)) = (
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

	let tid_id = resolver.lock().await.to_symbol_id(&tid);

	Ok((
		input,
		BedRecord::new(Arc::clone(&resolver), tid_id, start, end)
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

pub(crate) async fn parse_bedmethyl_record<'a, R: TidResolver>(
	resolver: Arc<Mutex<R>>,
	input: &'a [u8],
) -> IResult<&'a [u8], BedRecord<R, R::Tid, BedMethylExtra>>
{
	let (input, (tid, _, start, _, end, _, name, _, score, _, strand)) = (
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

	let (input, (_, thick_start, _, thick_end, _, item_rgb)) = (
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		multispace1,
		parse_string,
	)
		.parse(input)?;

	let (
		input,
		(
			_,
			n_valid_cov,
			_,
			frac_mod,
			_,
			n_mod,
			_,
			n_canonical,
			_,
			n_other_mod,
			_,
			n_delete,
			_,
			n_fail,
			_,
			n_diff,
			_,
			n_nocall,
			_,
		),
	) = (
		multispace1,
		parse_u32,
		multispace1,
		parse_f32,
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		multispace1,
		parse_u32,
		line_ending,
	)
		.parse(input)?;

	let tid_id = resolver.lock().await.to_symbol_id(&tid);

	Ok((
		input,
		BedRecord::new(Arc::clone(&resolver), tid_id, start, end)
			.with_name(name)
			.with_score(Some(score))
			.with_strand(strand)
			.with_bedmethyl(
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
				n_valid_cov,
				frac_mod,
				n_mod,
				n_canonical,
				n_other_mod,
				n_delete,
				n_fail,
				n_diff,
				n_nocall,
			),
	))
}

pub(crate) fn parse_track_line(input: &str) -> IResult<&str, Track>
{
	let mut track = Track::default();

	// must start with "track"
	let (input, _) = ((tag("track"), space1)).parse(input)?;

	let (input, pairs) = many0((parse_key_value_pair, opt(space1))).parse(input)?;

	for ((key, value), _) in pairs
	{
		match key
		{
			"name" => track.name = Some(value.to_string()),
			"description" => track.description = Some(value.to_string()),
			"visibility" => track.visibility = value.parse::<u8>().ok(),
			"itemRgb" => track.item_rgb = Some(value.to_string()),
			"color" => track.color = Some(value.to_string()),
			"useScore" => track.use_score = value.parse::<u8>().ok(),
			_ =>
			{}
		}
	}

	Ok((input, track))
}

pub(crate) fn parse_browser_line(input: &str) -> IResult<&str, BrowserMeta>
{
	let (mut input, _) = ((tag("browser"), space1)).parse(input)?;
	let mut attrs = HashMap::new();

	while !input.trim().is_empty()
	{
		if let Ok((rest, (k, v))) = parse_browser_pair(input)
		{
			attrs.insert(k.to_string(), v.to_string());
			input = rest.trim_start();
		}
		else
		{
			break;
		}
	}

	Ok((input, BrowserMeta { attrs }))
}
