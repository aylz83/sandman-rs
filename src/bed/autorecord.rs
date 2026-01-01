use async_trait::async_trait;

use std::fmt::Debug;

use crate::error;
use crate::bed::{AnyBedRecord, Strand};
use crate::store::TidResolver;

#[derive(Debug, Clone)]
pub enum FieldValue
{
	Int(i32),
	UInt(u32),
	Float(f32),
	Double(f64),
	String(String),
	Char(String),
	IntArray(Vec<i32>),
	UIntArray(Vec<u32>),
	FloatArray(Vec<f32>),
	StringArray(Vec<String>),
}

impl TryFrom<FieldValue> for i32
{
	type Error = error::Error;
	fn try_from(value: FieldValue) -> error::Result<Self>
	{
		match value
		{
			FieldValue::Int(i) => Ok(i),
			_ => Err(error::Error::VariantMismatch),
		}
	}
}

impl TryFrom<FieldValue> for u32
{
	type Error = error::Error;
	fn try_from(value: FieldValue) -> error::Result<Self>
	{
		match value
		{
			FieldValue::UInt(u) => Ok(u),
			_ => Err(error::Error::VariantMismatch),
		}
	}
}

impl TryFrom<FieldValue> for f32
{
	type Error = error::Error;
	fn try_from(value: FieldValue) -> error::Result<Self>
	{
		match value
		{
			FieldValue::Float(f) => Ok(f),
			_ => Err(error::Error::VariantMismatch),
		}
	}
}

impl TryFrom<FieldValue> for f64
{
	type Error = error::Error;
	fn try_from(value: FieldValue) -> error::Result<Self>
	{
		match value
		{
			FieldValue::Double(d) => Ok(d),
			_ => Err(error::Error::VariantMismatch),
		}
	}
}

impl TryFrom<FieldValue> for String
{
	type Error = error::Error;
	fn try_from(value: FieldValue) -> error::Result<Self>
	{
		match value
		{
			FieldValue::String(s) | FieldValue::Char(s) => Ok(s),
			_ => Err(error::Error::VariantMismatch),
		}
	}
}

impl TryFrom<FieldValue> for Vec<i32>
{
	type Error = error::Error;
	fn try_from(value: FieldValue) -> error::Result<Self>
	{
		match value
		{
			FieldValue::IntArray(v) => Ok(v),
			_ => Err(error::Error::VariantMismatch),
		}
	}
}

impl TryFrom<FieldValue> for Vec<u32>
{
	type Error = error::Error;
	fn try_from(value: FieldValue) -> error::Result<Self>
	{
		match value
		{
			FieldValue::UIntArray(v) => Ok(v),
			_ => Err(error::Error::VariantMismatch),
		}
	}
}

impl TryFrom<FieldValue> for Vec<f32>
{
	type Error = error::Error;
	fn try_from(value: FieldValue) -> error::Result<Self>
	{
		match value
		{
			FieldValue::FloatArray(v) => Ok(v),
			_ => Err(error::Error::VariantMismatch),
		}
	}
}

impl TryFrom<FieldValue> for Vec<String>
{
	type Error = error::Error;
	fn try_from(value: FieldValue) -> error::Result<Self>
	{
		match value
		{
			FieldValue::StringArray(v) => Ok(v),
			_ => Err(error::Error::VariantMismatch),
		}
	}
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
	fn name(&self) -> Option<&str>;
	fn score(&self) -> Option<u32>;
	fn strand(&self) -> Option<Strand>;

	// BED12 extras
	fn thick_start(&self) -> Option<u64>;
	fn thick_end(&self) -> Option<u64>;
	fn item_rgb(&self) -> &Option<String>;
	fn block_count(&self) -> Option<u32>;
	fn block_sizes(&self) -> Option<&Vec<u32>>;
	fn block_starts(&self) -> Option<&Vec<u32>>;

	// BEDMethyl extras
	fn n_valid_cov(&self) -> Option<u32>;
	fn frac_mod(&self) -> Option<f32>;
	fn n_mod(&self) -> Option<u32>;
	fn n_canonical(&self) -> Option<u32>;
	fn n_other_mod(&self) -> Option<u32>;
	fn n_delete(&self) -> Option<u32>;
	fn n_fail(&self) -> Option<u32>;
	fn n_diff(&self) -> Option<u32>;
	fn n_nocall(&self) -> Option<u32>;

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

	fn get_custom_value(&self, column: &str) -> Option<FieldValue>;
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
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) => &r.tid,
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
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) => r.resolver.lock().await,
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
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) => r.start,
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
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) => r.end,
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
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find_map(|(fname, value)| match (fname.as_str(), value)
					{
						("name", FieldValue::String(s)) => Some(s.as_str()),
						_ => None,
					})
			}
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
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find(|(name, _)| name == "score")
					.and_then(|(_, value)| match value
					{
						FieldValue::UInt(score) => Some(*score),
						_ => None,
					})
			}
			_ => None,
		}
	}

	fn strand(&self) -> Option<Strand>
	{
		match self
		{
			Self::Bed6(r) => Some(r.fields.strand),
			Self::Bed12(r) => Some(r.fields.strand),
			Self::BedMethyl(r) => Some(r.fields.strand),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find(|(name, _)| name == "strand")
					.and_then(|(_, value)| match value
					{
						FieldValue::String(s) => Some(Strand::from(s.as_str())),
						_ => None,
					})
			}
			_ => None,
		}
	}

	fn thick_start(&self) -> Option<u64>
	{
		match self
		{
			Self::Bed12(r) => Some(r.fields.thick_start),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find(|(name, _)| name == "thick_start")
					.and_then(|(_, value)| match value
					{
						FieldValue::UInt(value) => Some(*value as u64),
						_ => None,
					})
			}
			_ => None,
		}
	}
	fn thick_end(&self) -> Option<u64>
	{
		match self
		{
			Self::Bed12(r) => Some(r.fields.thick_end),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find(|(name, _)| name == "thick_end")
					.and_then(|(_, value)| match value
					{
						FieldValue::UInt(value) => Some(*value as u64),
						_ => None,
					})
			}
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
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find(|(name, _)| name == "block_count")
					.and_then(|(_, value)| match value
					{
						FieldValue::UInt(value) => Some(*value),
						_ => None,
					})
			}
			_ => None,
		}
	}
	fn block_sizes(&self) -> Option<&Vec<u32>>
	{
		match self
		{
			Self::Bed12(r) => Some(&r.fields.block_sizes),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find_map(|(fname, value)| match (fname.as_str(), value)
					{
						("block_sizes", FieldValue::UIntArray(vec)) => Some(vec),
						_ => None,
					})
			}
			_ => None,
		}
	}
	fn block_starts(&self) -> Option<&Vec<u32>>
	{
		match self
		{
			Self::Bed12(r) => Some(&r.fields.block_starts),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find_map(|(fname, value)| match (fname.as_str(), value)
					{
						("block_starts", FieldValue::UIntArray(vec)) => Some(vec),
						_ => None,
					})
			}
			_ => None,
		}
	}

	fn n_valid_cov(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_valid_cov),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find(|(name, _)| name == "n_valid_cov")
					.and_then(|(_, value)| match value
					{
						FieldValue::UInt(value) => Some(*value),
						_ => None,
					})
			}
			_ => None,
		}
	}
	fn frac_mod(&self) -> Option<f32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.frac_mod),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find(|(name, _)| name == "n_frac_mod")
					.and_then(|(_, value)| match value
					{
						FieldValue::Float(value) => Some(*value),
						_ => None,
					})
			}
			_ => None,
		}
	}
	fn n_mod(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_mod),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find(|(name, _)| name == "n_mod")
					.and_then(|(_, value)| match value
					{
						FieldValue::UInt(value) => Some(*value),
						_ => None,
					})
			}
			_ => None,
		}
	}
	fn n_canonical(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_canonical),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find(|(name, _)| name == "n_canonical")
					.and_then(|(_, value)| match value
					{
						FieldValue::UInt(value) => Some(*value),
						_ => None,
					})
			}
			_ => None,
		}
	}
	fn n_other_mod(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_other_mod),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find(|(name, _)| name == "n_other_mod")
					.and_then(|(_, value)| match value
					{
						FieldValue::UInt(value) => Some(*value),
						_ => None,
					})
			}
			_ => None,
		}
	}
	fn n_delete(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_delete),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields.iter().find(|(name, _)| name == "n_delete").and_then(
					|(_, value)| match value
					{
						FieldValue::UInt(value) => Some(*value),
						_ => None,
					},
				)
			}
			_ => None,
		}
	}
	fn n_fail(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_fail),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find(|(name, _)| name == "n_fail")
					.and_then(|(_, value)| match value
					{
						FieldValue::UInt(value) => Some(*value),
						_ => None,
					})
			}
			_ => None,
		}
	}
	fn n_diff(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_diff),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields
					.iter()
					.find(|(name, _)| name == "n_diff")
					.and_then(|(_, value)| match value
					{
						FieldValue::UInt(value) => Some(*value),
						_ => None,
					})
			}
			_ => None,
		}
	}
	fn n_nocall(&self) -> Option<u32>
	{
		match self
		{
			Self::BedMethyl(r) => Some(r.fields.n_nocall),
			#[cfg(feature = "bigbed")]
			Self::BigBed(r) =>
			{
				let fields = r.fields.fields.as_ref()?;
				fields.iter().find(|(name, _)| name == "n_nocall").and_then(
					|(_, value)| match value
					{
						FieldValue::UInt(value) => Some(*value),
						_ => None,
					},
				)
			}
			_ => None,
		}
	}

	fn get_custom_value(&self, column: &str) -> Option<FieldValue>
	{
		match column
		{
			"start" => Some(FieldValue::UInt(self.start() as u32)),
			"end" => Some(FieldValue::UInt(self.end() as u32)),
			"name" => self.name().map(|s| FieldValue::String(s.to_string())),
			"score" => self.score().map(FieldValue::UInt),
			"strand" => self.strand().map(|s| FieldValue::String(s.to_string())),
			"thick_start" => self.thick_start().map(|v| FieldValue::UInt(v as u32)),
			"thick_end" => self.thick_end().map(|v| FieldValue::UInt(v as u32)),
			"item_rgb" => self.item_rgb().clone().map(FieldValue::String),
			"block_count" => self.block_count().map(FieldValue::UInt),
			"block_sizes" => self.block_sizes().map(|v| FieldValue::UIntArray(v.clone())),
			"block_starts" => self
				.block_starts()
				.map(|v| FieldValue::UIntArray(v.clone())),
			"n_valid_cov" => self.n_valid_cov().map(FieldValue::UInt),
			"frac_mod" => self.frac_mod().map(FieldValue::Float),
			"n_mod" => self.n_mod().map(FieldValue::UInt),
			"n_canonical" => self.n_canonical().map(FieldValue::UInt),
			"n_other_mod" => self.n_other_mod().map(FieldValue::UInt),
			"n_delete" => self.n_delete().map(FieldValue::UInt),
			"n_fail" => self.n_fail().map(FieldValue::UInt),
			"n_diff" => self.n_diff().map(FieldValue::UInt),
			"n_nocall" => self.n_nocall().map(FieldValue::UInt),
			#[cfg(feature = "bigbed")]
			_ =>
			{
				// fallback for custom fields (BigBed)
				if let Self::BigBed(r) = self
				{
					r.fields
						.fields
						.as_ref()?
						.iter()
						.find(|(name, _)| name == column)
						.map(|(_, value)| value.clone())
				}
				else
				{
					None
				}
			}
			#[cfg(not(feature = "bigbed"))]
			_ => None,
		}
	}
}
