use std::fmt::Debug;

use crate::error;
use crate::bed::{Strand, BedKind, BedSinkValue, Bed3Fields};
use crate::bed::{Bed4Extra, Bed5Extra, Bed6Extra, Bed12Extra, BedMethylExtra};
use crate::filtering::ReadFilterContext;

mod bed3_fields
{
	pub const TID: usize = 0;
	pub const START: usize = 1;
	pub const END: usize = 2;
	pub const N_FIELDS: usize = 3;
}
mod bed4_fields
{
	pub const NAME: usize = 3;
	pub const N_FIELDS: usize = 4;
}
mod bed5_fields
{
	pub const SCORE: usize = 4;
	pub const N_FIELDS: usize = 5;
}
mod bed6_fields
{
	pub const STRAND: usize = 5;
	pub const N_FIELDS: usize = 6;
}
mod bed12_fields
{
	pub const N_FIELDS: usize = 12;
}
mod bedmethyl_fields
{
	pub const N_VALID_COV: usize = 9;
	pub const FRAC_MOD: usize = 10;
	pub const N_MOD: usize = 11;
	pub const N_CANONICAL: usize = 12;
	pub const N_OTHER_MOD: usize = 13;
	pub const N_DELETE: usize = 14;
	pub const N_FAIL: usize = 15;
	pub const N_DIFF: usize = 16;
	pub const N_NOCALL: usize = 17;

	pub const N_FIELDS: usize = 18;
}

// #[async_trait::async_trait]
pub trait BedFieldsSink<Tid>: Send + Sync
where
	Tid: Debug + Clone + Send + Sync + PartialEq,
{
	const KIND: BedKind;

	fn parse_sink<'a>(
		input: &'a [u8],
		// _ctx: Option<ParseContext<'b>>,
		filter_ctx: Option<&ReadFilterContext>,
	) -> impl std::future::Future<
		Output = error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>,
	> + Send
	where
		Self: Sized;
}

// #[async_trait::async_trait]
impl<Tid> BedFieldsSink<Tid> for Bed3Fields
where
	Tid: Debug + Clone + Send + Sync + PartialEq,
{
	const KIND: BedKind = BedKind::Bed3;

	async fn parse_sink<'a>(
		input: &'a [u8],
		// _ctx: Option<ParseContext<'b>>,
		filter_ctx: Option<&ReadFilterContext>,
	) -> error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>
	{
		let (rest, parsed) = parse_bed3_sink_simd(input, filter_ctx).await?;

		Ok((rest, parsed))
	}
}

// #[async_trait::async_trait]
impl<Tid> BedFieldsSink<Tid> for Bed4Extra
where
	Tid: Debug + Clone + Send + Sync + PartialEq,
{
	const KIND: BedKind = BedKind::Bed4;

	async fn parse_sink<'a>(
		input: &'a [u8],
		// _ctx: Option<ParseContext<'b>>,
		filter_ctx: Option<&ReadFilterContext>,
	) -> error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>
	{
		let (rest, parsed) = parse_bed4_sink_simd(input, filter_ctx).await?;

		Ok((rest, parsed))
	}
}

// #[async_trait::async_trait]
impl<Tid> BedFieldsSink<Tid> for Bed5Extra
where
	Tid: Debug + Clone + Send + Sync + PartialEq,
{
	const KIND: BedKind = BedKind::Bed5;

	async fn parse_sink<'a>(
		input: &'a [u8],
		// _ctx: Option<ParseContext<'b>>,
		filter_ctx: Option<&ReadFilterContext>,
	) -> error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>
	{
		let (rest, parsed) = parse_bed5_sink_simd(input, filter_ctx).await?;

		Ok((rest, parsed))
	}
}

// #[async_trait::async_trait]
impl<Tid> BedFieldsSink<Tid> for Bed6Extra
where
	Tid: Debug + Clone + Send + Sync + PartialEq,
{
	const KIND: BedKind = BedKind::Bed6;

	async fn parse_sink<'a>(
		input: &'a [u8],
		// _ctx: Option<ParseContext<'b>>,
		filter_ctx: Option<&ReadFilterContext>,
	) -> error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>
	{
		let (rest, parsed) = parse_bed6_sink_simd(input, filter_ctx).await?;

		Ok((rest, parsed))
	}
}

// #[async_trait::async_trait]
impl<Tid> BedFieldsSink<Tid> for Bed12Extra
where
	Tid: Debug + Clone + Send + Sync + PartialEq,
{
	const KIND: BedKind = BedKind::Bed12;

	async fn parse_sink<'a>(
		input: &'a [u8],
		// _ctx: Option<ParseContext<'b>>,
		filter_ctx: Option<&ReadFilterContext>,
	) -> error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>
	{
		let (rest, parsed) = parse_bed12_sink_simd(input, filter_ctx).await?;

		Ok((rest, parsed))
	}
}

// #[async_trait::async_trait]
impl<Tid> BedFieldsSink<Tid> for BedMethylExtra
where
	Tid: Debug + Clone + Send + Sync + PartialEq,
{
	const KIND: BedKind = BedKind::BedMethyl;

	async fn parse_sink<'a>(
		input: &'a [u8],
		// _ctx: Option<ParseContext<'b>>,
		filter_ctx: Option<&ReadFilterContext>,
	) -> error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>
	{
		let (rest, parsed) = parse_bedmethyl_sink_simd(input, filter_ctx).await?;

		Ok((rest, parsed))
	}
}

pub async fn parse_bed3_sink_simd<'a>(
	input: &'a [u8],
	_filter_ctx: Option<&ReadFilterContext>,
) -> error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>
{
	if input.is_empty() || input[0] == b'\n'
	{
		let rest = memchr::memchr(b'\n', input)
			.map(|p| p + 1)
			.unwrap_or(input.len());
		return Ok((&input[rest..], None));
	}

	let line_end = memchr::memchr(b'\n', input).unwrap_or(input.len());
	let line = &input[..line_end];

	let mut fields = [0usize; 32];
	let mut n = 0;
	let mut start_idx = 0;

	for (i, &b) in line.iter().enumerate()
	{
		if b == b' ' || b == b'\t'
		{
			fields[n] = start_idx;
			n += 1;
			start_idx = i + 1;
		}
	}

	if (n + 1) != bed3_fields::N_FIELDS
	{
		return Err(error::Error::BedMismatch("BED3".into()));
	}

	fields[n] = start_idx;

	let tid = unsafe {
		std::str::from_utf8_unchecked(
			&line[fields[bed3_fields::TID]..fields[bed3_fields::START] - 1],
		)
	};
	let start_val = lexical_core::parse::<u64>(
		&line[fields[bed3_fields::START]..fields[bed3_fields::END] - 1],
	)?;
	let end_val = lexical_core::parse::<u64>(&line[fields[bed3_fields::END]..fields[line.len()]])?;

	let rest = if line_end < input.len()
	{
		&input[line_end + 1..]
	}
	else
	{
		&input[line_end..]
	};

	Ok((
		&rest,
		Some((
			tid,
			Strand::Both,
			start_val,
			end_val,
			BedSinkValue {
				name: None,
				score: None,
				n_valid_cov: None,
				frac_mod: None,
				n_mod: None,
				n_canonical: None,
				n_other_mod: None,
				n_delete: None,
				n_fail: None,
				n_diff: None,
				n_nocall: None,
			},
		)),
	))
}

pub async fn parse_bed4_sink_simd<'a>(
	input: &'a [u8],
	filter_ctx: Option<&ReadFilterContext>,
) -> error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>
{
	if input.is_empty() || input[0] == b'\n'
	{
		let rest = memchr::memchr(b'\n', input)
			.map(|p| p + 1)
			.unwrap_or(input.len());
		return Ok((&input[rest..], None));
	}

	let line_end = memchr::memchr(b'\n', input).unwrap_or(input.len());
	let line = &input[..line_end];

	let mut fields = [0usize; 32];
	let mut n = 0;
	let mut start_idx = 0;

	for (i, &b) in line.iter().enumerate()
	{
		if b == b' ' || b == b'\t'
		{
			fields[n] = start_idx;
			n += 1;
			start_idx = i + 1;
		}
	}

	if (n + 1) != bed4_fields::N_FIELDS
	{
		return Err(error::Error::BedMismatch("BED4".into()));
	}

	fields[n] = start_idx;

	let tid = unsafe {
		std::str::from_utf8_unchecked(
			&line[fields[bed3_fields::TID]..fields[bed3_fields::START] - 1],
		)
	};
	let start_val = lexical_core::parse::<u64>(
		&line[fields[bed3_fields::START]..fields[bed3_fields::END] - 1],
	)?;
	let end_val =
		lexical_core::parse::<u64>(&line[fields[bed3_fields::END]..fields[bed4_fields::NAME] - 1])?;
	let name = &line[fields[bed4_fields::NAME]..fields[line.len()]];

	let rest = if line_end < input.len()
	{
		&input[line_end + 1..]
	}
	else
	{
		&input[line_end..]
	};

	if let Some(ctx) = filter_ctx
	{
		if !ctx
			.passes(tid, start_val, end_val, Strand::Both, Some(&name), None)
			.await
		{
			return Ok((&rest, None));
		}
	}

	let name = unsafe { std::str::from_utf8_unchecked(name) }.to_owned();

	Ok((
		&rest,
		Some((
			tid,
			Strand::Both,
			start_val,
			end_val,
			BedSinkValue {
				name: Some(name),
				score: None,
				n_valid_cov: None,
				frac_mod: None,
				n_mod: None,
				n_canonical: None,
				n_other_mod: None,
				n_delete: None,
				n_fail: None,
				n_diff: None,
				n_nocall: None,
			},
		)),
	))
}

pub async fn parse_bed5_sink_simd<'a>(
	input: &'a [u8],
	filter_ctx: Option<&ReadFilterContext>,
) -> error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>
{
	if input.is_empty() || input[0] == b'\n'
	{
		let rest = memchr::memchr(b'\n', input)
			.map(|p| p + 1)
			.unwrap_or(input.len());
		return Ok((&input[rest..], None));
	}

	let line_end = memchr::memchr(b'\n', input).unwrap_or(input.len());
	let line = &input[..line_end];

	let mut fields = [0usize; 32];
	let mut n = 0;
	let mut start_idx = 0;

	for (i, &b) in line.iter().enumerate()
	{
		if b == b' ' || b == b'\t'
		{
			fields[n] = start_idx;
			n += 1;
			start_idx = i + 1;
		}
	}

	if (n + 1) != bed5_fields::N_FIELDS
	{
		return Err(error::Error::BedMismatch("BED5".into()));
	}

	fields[n] = start_idx;

	let tid = unsafe {
		std::str::from_utf8_unchecked(
			&line[fields[bed3_fields::TID]..fields[bed3_fields::START] - 1],
		)
	};
	let start_val = lexical_core::parse::<u64>(
		&line[fields[bed3_fields::START]..fields[bed3_fields::END] - 1],
	)?;
	let end_val =
		lexical_core::parse::<u64>(&line[fields[bed3_fields::END]..fields[bed4_fields::NAME] - 1])?;
	let name = &line[fields[bed4_fields::NAME]..fields[bed5_fields::SCORE] - 1];
	let score = lexical_core::parse::<u32>(&line[fields[bed5_fields::SCORE]..fields[line.len()]])?;

	let rest = if line_end < input.len()
	{
		&input[line_end + 1..]
	}
	else
	{
		&input[line_end..]
	};

	if let Some(ctx) = filter_ctx
	{
		if !ctx
			.passes(
				tid,
				start_val,
				end_val,
				Strand::Both,
				Some(&name),
				Some(&[score as f32]),
			)
			.await
		{
			return Ok((&rest, None));
		}
	}

	let name = unsafe { std::str::from_utf8_unchecked(name) }.to_owned();

	Ok((
		&rest,
		Some((
			tid,
			Strand::Both,
			start_val,
			end_val,
			BedSinkValue {
				name: Some(name),
				score: Some(score),
				n_valid_cov: None,
				frac_mod: None,
				n_mod: None,
				n_canonical: None,
				n_other_mod: None,
				n_delete: None,
				n_fail: None,
				n_diff: None,
				n_nocall: None,
			},
		)),
	))
}

pub async fn parse_bed6_sink_simd<'a>(
	input: &'a [u8],
	filter_ctx: Option<&ReadFilterContext>,
) -> error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>
{
	if input.is_empty() || input[0] == b'\n'
	{
		let rest = memchr::memchr(b'\n', input)
			.map(|p| p + 1)
			.unwrap_or(input.len());
		return Ok((&input[rest..], None));
	}

	let line_end = memchr::memchr(b'\n', input).unwrap_or(input.len());
	let line = &input[..line_end];

	let mut fields = [0usize; 32];
	let mut n = 0;
	let mut start_idx = 0;

	for (i, &b) in line.iter().enumerate()
	{
		if b == b' ' || b == b'\t'
		{
			fields[n] = start_idx;
			n += 1;
			start_idx = i + 1;
		}
	}

	if (n + 1) != bed6_fields::N_FIELDS
	{
		return Err(error::Error::BedMismatch("BED6".into()));
	}

	fields[n] = start_idx;

	let tid = unsafe {
		std::str::from_utf8_unchecked(
			&line[fields[bed3_fields::TID]..fields[bed3_fields::START] - 1],
		)
	};
	let start_val = lexical_core::parse::<u64>(
		&line[fields[bed3_fields::START]..fields[bed3_fields::END] - 1],
	)?;
	let end_val =
		lexical_core::parse::<u64>(&line[fields[bed3_fields::END]..fields[bed4_fields::NAME] - 1])?;
	let name = &line[fields[bed4_fields::NAME]..fields[bed5_fields::SCORE] - 1];
	let score = lexical_core::parse::<u32>(
		&line[fields[bed5_fields::SCORE]..fields[bed6_fields::STRAND] - 1],
	)?;
	let strand = Strand::from(line[fields[bed6_fields::STRAND]]);

	let rest = if line_end < input.len()
	{
		&input[line_end + 1..]
	}
	else
	{
		&input[line_end..]
	};

	if let Some(ctx) = filter_ctx
	{
		if !ctx
			.passes(
				tid,
				start_val,
				end_val,
				strand,
				Some(&name),
				Some(&[score as f32]),
			)
			.await
		{
			return Ok((&rest, None));
		}
	}

	let name = unsafe { std::str::from_utf8_unchecked(name) }.to_owned();

	Ok((
		&rest,
		Some((
			tid,
			strand,
			start_val,
			end_val,
			BedSinkValue {
				name: Some(name),
				score: Some(score),
				n_valid_cov: None,
				frac_mod: None,
				n_mod: None,
				n_canonical: None,
				n_other_mod: None,
				n_delete: None,
				n_fail: None,
				n_diff: None,
				n_nocall: None,
			},
		)),
	))
}

pub async fn parse_bed12_sink_simd<'a>(
	input: &'a [u8],
	filter_ctx: Option<&ReadFilterContext>,
) -> error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>
{
	if input.is_empty() || input[0] == b'\n'
	{
		let rest = memchr::memchr(b'\n', input)
			.map(|p| p + 1)
			.unwrap_or(input.len());
		return Ok((&input[rest..], None));
	}

	let line_end = memchr::memchr(b'\n', input).unwrap_or(input.len());
	let line = &input[..line_end];

	let mut fields = [0usize; 32];
	let mut n = 0;
	let mut start_idx = 0;

	for (i, &b) in line.iter().enumerate()
	{
		if b == b' ' || b == b'\t'
		{
			fields[n] = start_idx;
			n += 1;
			start_idx = i + 1;
		}
	}

	if (n + 1) != bed12_fields::N_FIELDS
	{
		return Err(error::Error::BedMismatch("BED12".into()));
	}

	fields[n] = start_idx;

	let tid = unsafe {
		std::str::from_utf8_unchecked(
			&line[fields[bed3_fields::TID]..fields[bed3_fields::START] - 1],
		)
	};
	let start_val = lexical_core::parse::<u64>(
		&line[fields[bed3_fields::START]..fields[bed3_fields::END] - 1],
	)?;
	let end_val =
		lexical_core::parse::<u64>(&line[fields[bed3_fields::END]..fields[bed4_fields::NAME] - 1])?;
	let name = &line[fields[bed4_fields::NAME]..fields[bed5_fields::SCORE] - 1];
	let score = lexical_core::parse::<u32>(
		&line[fields[bed5_fields::SCORE]..fields[bed6_fields::STRAND] - 1],
	)?;
	let strand = Strand::from(line[fields[bed6_fields::STRAND]]);

	let rest = if line_end < input.len()
	{
		&input[line_end + 1..]
	}
	else
	{
		&input[line_end..]
	};

	if let Some(ctx) = filter_ctx
	{
		if !ctx
			.passes(
				tid,
				start_val,
				end_val,
				strand,
				Some(&name),
				Some(&[score as f32]),
			)
			.await
		{
			return Ok((&rest, None));
		}
	}

	let name = unsafe { std::str::from_utf8_unchecked(name) }.to_owned();

	Ok((
		&rest,
		Some((
			tid,
			strand,
			start_val,
			end_val,
			BedSinkValue {
				name: Some(name),
				score: Some(score),
				n_valid_cov: None,
				frac_mod: None,
				n_mod: None,
				n_canonical: None,
				n_other_mod: None,
				n_delete: None,
				n_fail: None,
				n_diff: None,
				n_nocall: None,
			},
		)),
	))
}

pub async fn parse_bedmethyl_sink_simd<'a>(
	input: &'a [u8],
	filter_ctx: Option<&ReadFilterContext>,
) -> error::Result<(&'a [u8], Option<(&'a str, Strand, u64, u64, BedSinkValue)>)>
{
	if input.is_empty() || input[0] == b'\n'
	{
		let rest = memchr::memchr(b'\n', input)
			.map(|p| p + 1)
			.unwrap_or(input.len());
		return Ok((&input[rest..], None));
	}

	let line_end = memchr::memchr(b'\n', input).unwrap_or(input.len());
	let mut line = &input[..line_end];

	if line.ends_with(b"\r")
	{
		line = &line[..line.len() - 1];
	}

	let mut fields = [0usize; 32];
	let mut n = 0;
	let mut start_idx = 0;

	for (i, &b) in line.iter().enumerate()
	{
		if b == b' ' || b == b'\t' || b == b'\r'
		{
			fields[n] = start_idx;
			n += 1;
			start_idx = i + 1;
		}
	}

	if (n + 1) != bedmethyl_fields::N_FIELDS
	{
		return Err(error::Error::BedMismatch("BEDMethyl".into()));
	}

	fields[n] = start_idx;

	let tid = unsafe {
		std::str::from_utf8_unchecked(
			&line[fields[bed3_fields::TID]..fields[bed3_fields::START] - 1],
		)
	};
	let start_val = lexical_core::parse::<u64>(
		&line[fields[bed3_fields::START]..fields[bed3_fields::END] - 1],
	)?;
	let end_val =
		lexical_core::parse::<u64>(&line[fields[bed3_fields::END]..fields[bed4_fields::NAME] - 1])?;
	let name = &line[fields[bed4_fields::NAME]..fields[bed5_fields::SCORE] - 1];
	let score = lexical_core::parse::<u32>(
		&line[fields[bed5_fields::SCORE]..fields[bed6_fields::STRAND] - 1],
	)?;
	let strand = Strand::from(line[fields[bed6_fields::STRAND]]);

	let n_valid_cov = lexical_core::parse::<u32>(
		&line[fields[bedmethyl_fields::N_VALID_COV]..fields[bedmethyl_fields::FRAC_MOD] - 1],
	)?;
	let frac_mod = lexical_core::parse::<f32>(
		&line[fields[bedmethyl_fields::FRAC_MOD]..fields[bedmethyl_fields::N_MOD] - 1],
	)?;
	let n_mod = lexical_core::parse::<u32>(
		&line[fields[bedmethyl_fields::N_MOD]..fields[bedmethyl_fields::N_CANONICAL] - 1],
	)?;
	let n_canonical = lexical_core::parse::<u32>(
		&line[fields[bedmethyl_fields::N_CANONICAL]..fields[bedmethyl_fields::N_OTHER_MOD] - 1],
	)?;
	let n_other_mod = lexical_core::parse::<u32>(
		&line[fields[bedmethyl_fields::N_OTHER_MOD]..fields[bedmethyl_fields::N_DELETE] - 1],
	)?;
	let n_delete = lexical_core::parse::<u32>(
		&line[fields[bedmethyl_fields::N_DELETE]..fields[bedmethyl_fields::N_FAIL] - 1],
	)?;
	let n_fail = lexical_core::parse::<u32>(
		&line[fields[bedmethyl_fields::N_FAIL]..fields[bedmethyl_fields::N_DIFF] - 1],
	)?;
	let n_diff = lexical_core::parse::<u32>(
		&line[fields[bedmethyl_fields::N_DIFF]..fields[bedmethyl_fields::N_NOCALL] - 1],
	)?;
	let n_nocall =
		lexical_core::parse::<u32>(&line[fields[bedmethyl_fields::N_NOCALL]..line.len()])?;

	let rest = if line_end < input.len()
	{
		&input[line_end + 1..]
	}
	else
	{
		&input[line_end..]
	};

	if let Some(ctx) = filter_ctx
	{
		if !ctx
			.passes(
				tid,
				start_val,
				end_val,
				strand,
				Some(name),
				Some(&[
					score as f32,
					n_valid_cov as f32,
					frac_mod,
					n_mod as f32,
					n_canonical as f32,
					n_other_mod as f32,
					n_delete as f32,
					n_fail as f32,
					n_diff as f32,
					n_nocall as f32,
				]),
			)
			.await
		{
			return Ok((&rest, None));
		}
	}

	let name = unsafe { std::str::from_utf8_unchecked(name) }.to_owned();

	Ok((
		&rest,
		Some((
			tid,
			strand,
			start_val,
			end_val,
			BedSinkValue {
				name: Some(name),
				score: Some(score),
				n_valid_cov: Some(n_valid_cov),
				frac_mod: Some(frac_mod),
				n_mod: Some(n_mod),
				n_canonical: Some(n_canonical),
				n_other_mod: Some(n_other_mod),
				n_delete: Some(n_delete),
				n_fail: Some(n_fail),
				n_diff: Some(n_diff),
				n_nocall: Some(n_nocall),
			},
		)),
	))
}
