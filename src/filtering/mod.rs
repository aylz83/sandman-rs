pub mod basechecker;

use faisync::Contigs;

use std::borrow::Cow;
use std::collections::HashMap;

use crate::bed::Strand;
use crate::filtering::basechecker::BaseChecker;
use crate::bed::ScoreField;

#[derive(Default)]
pub struct ReadFilterContext
{
	minimum_scores: Option<Vec<(ScoreField, f32)>>,
	basechecker: Option<BaseChecker>,
}

impl ReadFilterContext
{
	pub fn add_minimum_score(&mut self, ix: ScoreField, score: f32)
	{
		self.minimum_scores
			.get_or_insert_with(Vec::new)
			.push((ix, score));
	}

	pub fn set_basechecker(&mut self, contigs: Contigs, checker_map: HashMap<Cow<'static, str>, u8>)
	{
		self.basechecker = Some(BaseChecker(contigs, checker_map));
	}

	// pub(crate) async fn passes_scores(&self, scores: &[f32]) -> bool
	// {
	// 	if let Some(minimum_scores) = &self.minimum_scores
	// 	{
	// 		for (ix, min) in minimum_scores
	// 		{
	// 			let v = scores[ix.as_usize()];
	// 			if v < *min
	// 			{
	// 				return false;
	// 			}
	// 		}
	// 	}

	// 	true
	// }

	// pub(crate) async fn passes_name(
	// 	&self,
	// 	tid: &str,
	// 	start: u64,
	// 	end: u64,
	// 	strand: Strand,
	// 	name: &[u8],
	// ) -> bool
	// {
	// 	if let Some(basechecker) = &self.basechecker
	// 	{
	// 		let name = unsafe { std::str::from_utf8_unchecked(name) };
	// 		if basechecker
	// 			.check_base(&tid, start, end, &strand, name)
	// 			.await
	// 			.is_err()
	// 		{
	// 			return false;
	// 		}
	// 	}

	// 	true
	// }

	pub(crate) async fn passes(
		&self,
		tid: &str,
		start: u64,
		end: u64,
		strand: Strand,
		name: Option<&[u8]>,
		scores: Option<&[f32]>,
	) -> bool
	{
		if let Some(minimum_scores) = &self.minimum_scores
		{
			let Some(scores) = scores
			else
			{
				return false; // scores required but missing
			};

			for (ix, min) in minimum_scores
			{
				let v = scores[ix.as_usize()];
				if v < *min
				{
					return false;
				}
			}
		}

		if let Some(basechecker) = &self.basechecker
		{
			let Some(name) = name
			else
			{
				return false; // base checker requires name but missing
			};

			let name = unsafe { std::str::from_utf8_unchecked(name) };

			if basechecker
				.check_base(&tid, start, end, &strand, name)
				.await
				.is_err()
			{
				return false;
			}
		}

		true
	}
}
