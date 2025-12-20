use crate::bed::Strand;
use crate::bed::BedRecord;
use crate::store::TidResolver;

use crate::bed::IntoAnyBedRecord;
use crate::bed::AnyBedRecord;

use std::fmt::Debug;

#[derive(Debug, Clone, Default)]
pub struct Bed4Extra
{
	pub name: String,
}

#[derive(Debug, Clone, Default)]
pub struct Bed5Extra
{
	pub name: String,
	pub score: Option<u32>,
}

#[derive(Debug, Clone, Default)]
pub struct Bed6Extra
{
	pub name: String,
	pub score: Option<u32>,
	pub strand: Strand,
}

#[derive(Debug, Clone, Default)]
pub struct Bed12Extra
{
	pub name: String,
	pub score: Option<u32>,
	pub strand: Strand,
	pub thick_start: u64,
	pub thick_end: u64,
	pub item_rgb: Option<String>,
	pub block_count: u32,
	pub block_sizes: Vec<u32>,
	pub block_starts: Vec<u32>,
}

#[derive(Debug, Clone, Default)]
pub struct BedMethylExtra
{
	pub name: String,
	pub score: Option<u32>,
	pub strand: Strand,
	pub thick_start: u64,
	pub thick_end: u64,
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

impl<T> IntoAnyBedRecord<T> for BedRecord<T, T::Tid, Bed4Extra>
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn into_any(self) -> AnyBedRecord<T>
	{
		AnyBedRecord::Bed4(self)
	}
}

impl<T> IntoAnyBedRecord<T> for BedRecord<T, T::Tid, Bed5Extra>
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn into_any(self) -> AnyBedRecord<T>
	{
		AnyBedRecord::Bed5(self)
	}
}

impl<T> IntoAnyBedRecord<T> for BedRecord<T, T::Tid, Bed6Extra>
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn into_any(self) -> AnyBedRecord<T>
	{
		AnyBedRecord::Bed6(self)
	}
}

impl<T> IntoAnyBedRecord<T> for BedRecord<T, T::Tid, Bed12Extra>
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn into_any(self) -> AnyBedRecord<T>
	{
		AnyBedRecord::Bed12(self)
	}
}

impl<T> IntoAnyBedRecord<T> for BedRecord<T, T::Tid, BedMethylExtra>
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn into_any(self) -> AnyBedRecord<T>
	{
		AnyBedRecord::BedMethyl(self)
	}
}
