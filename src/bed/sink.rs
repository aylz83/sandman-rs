use std::fmt::Debug;

use crate::bed::ScoreField;
use crate::bed::ReaderId;
use crate::bed::SourceId;
use crate::bed::Strand;

#[derive(Debug, Clone)]
pub struct BedSinkValue
{
	// core BED fields
	pub(crate) name: Option<String>,
	pub(crate) score: Option<u32>,

	// methyl-specific (None for non-methyl)
	pub(crate) n_valid_cov: Option<u32>,
	pub(crate) frac_mod: Option<f32>,
	pub(crate) n_mod: Option<u32>,
	pub(crate) n_canonical: Option<u32>,
	pub(crate) n_other_mod: Option<u32>,
	pub(crate) n_delete: Option<u32>,
	pub(crate) n_fail: Option<u32>,
	pub(crate) n_diff: Option<u32>,
	pub(crate) n_nocall: Option<u32>,
}

impl BedSinkValue
{
	pub fn get_u32(&self, field: ScoreField) -> Option<u32>
	{
		match field
		{
			ScoreField::Score => self.score,
			ScoreField::NValidCov => self.n_valid_cov,
			ScoreField::FracMod => self.frac_mod.map(|f| f as u32),
			ScoreField::NMod => self.n_mod,
			ScoreField::NCanonical => self.n_canonical,
			ScoreField::NOtherMod => self.n_other_mod,
			ScoreField::NDelete => self.n_delete,
			ScoreField::NFail => self.n_fail,
			ScoreField::NDiff => self.n_diff,
			ScoreField::NNoCall => self.n_nocall,
		}
	}

	pub fn get_f32(&self, field: ScoreField) -> Option<f32>
	{
		match field
		{
			ScoreField::Score => self.score.map(|u| u as f32),
			ScoreField::NValidCov => self.n_valid_cov.map(|u| u as f32),
			ScoreField::FracMod => self.frac_mod,
			ScoreField::NMod => self.n_mod.map(|u| u as f32),
			ScoreField::NCanonical => self.n_canonical.map(|u| u as f32),
			ScoreField::NOtherMod => self.n_other_mod.map(|u| u as f32),
			ScoreField::NDelete => self.n_delete.map(|u| u as f32),
			ScoreField::NFail => self.n_fail.map(|u| u as f32),
			ScoreField::NDiff => self.n_diff.map(|u| u as f32),
			ScoreField::NNoCall => self.n_nocall.map(|u| u as f32),
		}
	}

	pub fn get_name(&self) -> Option<&str>
	{
		self.name.as_deref()
	}
}

pub trait BedSink<Tid>: Send + Sync
{
	fn begin_tid(&mut self, tid: &Tid, strand: &Strand);
	fn end_tid(&mut self, tid: &Tid, strand: &Strand);

	fn begin_position(&mut self, start: u64);
	fn end_position(&mut self, end: u64);

	fn push_value(
		&mut self,
		source_id: &Option<SourceId>,
		reader_id: &ReaderId,
		value: BedSinkValue,
	);
}
