use crate::bed::Strand;
use crate::bed::BedRecord;

use std::fmt::Debug;

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

#[derive(Debug, Clone)]
pub struct BedMethylExtra
{
	pub name: String,
	pub score: Option<u32>,
	pub strand: Strand,
	pub thick_start: u32,
	pub thick_end: u32,
	pub item_rgb: Option<String>,
	pub n_valid_cov: u32,
	pub frac_mod: f32,
	pub n_mod: u32,
	pub n_canonical: u32,
	pub n_other_mod: u32,
	pub n_delete: u32,
	pub n_fail: u32,
	pub n_diff: u32,
	pub n_nocall: u32,
}

impl<Tid> BedRecord<Tid, Bed4Extra>
where
	Tid: Clone + Debug + Send + Sync,
{
	pub fn with_score(self, score: Option<u32>) -> BedRecord<Tid, Bed5Extra>
	{
		BedRecord {
			tid: self.tid,
			start: self.start,
			end: self.end,
			fields: Bed5Extra {
				name: self.fields.name,
				score,
			},
		}
	}
}

impl<Tid> BedRecord<Tid, Bed5Extra>
where
	Tid: Clone + Debug + Send + Sync,
{
	pub fn with_strand(self, strand: Strand) -> BedRecord<Tid, Bed6Extra>
	{
		BedRecord {
			tid: self.tid,
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

impl<Tid> BedRecord<Tid, Bed6Extra>
where
	Tid: Clone + Debug + Send + Sync,
{
	pub fn with_bed12(
		self,
		thick_start: u32,
		thick_end: u32,
		item_rgb: Option<String>,
		block_count: u32,
		block_sizes: Vec<u32>,
		block_starts: Vec<u32>,
	) -> BedRecord<Tid, Bed12Extra>
	{
		BedRecord {
			tid: self.tid,
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

	pub fn with_bedmethyl(
		self,
		thick_start: u32,
		thick_end: u32,
		item_rgb: Option<String>,
		n_valid_cov: u32,
		frac_mod: f32,
		n_mod: u32,
		n_canonical: u32,
		n_other_mod: u32,
		n_delete: u32,
		n_fail: u32,
		n_diff: u32,
		n_nocall: u32,
	) -> BedRecord<Tid, BedMethylExtra>
	{
		BedRecord {
			tid: self.tid,
			start: self.start,
			end: self.end,
			fields: BedMethylExtra {
				name: self.fields.name,
				score: self.fields.score,
				strand: self.fields.strand,
				thick_start,
				thick_end,
				item_rgb,
				n_valid_cov,
				frac_mod,
				n_mod,
				n_canonical,
				n_other_mod,
				n_delete,
				n_fail,
				n_diff,
				n_nocall,
			},
		}
	}
}
