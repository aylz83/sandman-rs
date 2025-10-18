use std::fmt::{Debug, Display};
use std::collections::HashMap;

pub use crate::bed::record::*;
pub use crate::bed::extra::*;

#[macro_export]
macro_rules! record {
    // BED3
    ($chr:expr, $start:expr, $end:expr) => {{
        Box::new(
            $crate::bed::BedRecord::new($chr, $start, $end)
        ) as Box<dyn $crate::bed::BedLine>
    }};

    // BED4
    ($chr:expr, $start:expr, $end:expr, $name:expr) => {{
        Box::new(
            $crate::bed::BedRecord::new($chr, $start, $end)
                .with_name($name)
        ) as Box<dyn $crate::bed::BedLine>
    }};

    // BED5
    ($chr:expr, $start:expr, $end:expr, $name:expr, $score:expr) => {{
        Box::new(
            $crate::bed::BedRecord::new($chr, $start, $end)
                .with_name($name)
                .with_score(Some($score))
        ) as Box<dyn $crate::bed::BedLine>
    }};

    // BED6
    ($chr:expr, $start:expr, $end:expr, $name:expr, $score:expr, $strand:expr) => {{
        Box::new(
            $crate::bed::BedRecord::new($chr, $start, $end)
                .with_name($name)
                .with_score(Some($score))
                .with_strand($strand.into())
        ) as Box<dyn $crate::bed::BedLine>
    }};

    // BED12
    ($chr:expr, $start:expr, $end:expr, $name:expr, $score:expr, $strand:expr,
        $thick_start:expr, $thick_end:expr, $item_rgb:expr, $block_count:expr,
        $block_sizes:expr, $block_starts:expr) => {{
        Box::new(
            $crate::bed::BedRecord::new($chr, $start, $end)
                .with_name($name)
                .with_score(Some($score))
                .with_strand($strand.into())
                .with_bed12(
                    $thick_start,
                    $thick_end,
                    Some($item_rgb),
                    $block_count,
                    $block_sizes,
                    $block_starts,
                )
        ) as Box<dyn $crate::bed::BedLine>
    }};

    // BEDMethyl
    ($chr:expr, $start:expr, $end:expr, $name:expr, $score:expr, $strand:expr,
        $thick_start:expr, $thick_end:expr, $item_rgb:expr, $n_valid_cov:expr,
        $frac_mod:expr, $n_mod:expr, $n_canonical:expr, $n_other_mod:expr,
        $n_delete:expr, $n_fail:expr, $n_diff:expr, $n_nocall:expr) => {{
        Box::new(
            $crate::bed::BedRecord::new($chr, $start, $end)
                .with_name($name)
                .with_score(Some($score))
                .with_strand($strand.into())
                .with_bedmethyl(
                    $thick_start,
                    $thick_end,
                    Some($item_rgb),
                    $n_valid_cov,
                    $frac_mod,
                    $n_mod
                    $n_canonical,
                    $n_other_mod,
                    $n_delete,
                    $n_fail,
                    $n_diff,
                    $n_nocall,
                )
        ) as Box<dyn $crate::bed::BedLine>
    }};
}

#[cfg_attr(feature = "bincode", derive(bincode::Encode, bincode::Decode))]
#[derive(PartialOrd, Ord, Eq, Hash, PartialEq, Debug, Clone)]
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

pub trait BedLine: Debug + BedLineClone
{
	fn tid(&self) -> &str;
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
			// add more as needed
			_ => None,
		}
	}
}

pub trait BedLineClone
{
	fn clone_box(&self) -> Box<dyn BedLine>;
}

impl<T> BedLineClone for T
where
	T: 'static + BedLine + Clone,
{
	fn clone_box(&self) -> Box<dyn BedLine>
	{
		Box::new(self.clone())
	}
}

impl Clone for Box<dyn BedLine>
{
	fn clone(&self) -> Box<dyn BedLine>
	{
		self.clone_box()
	}
}

impl BedLine for BedRecord<Bed3Fields>
{
	fn tid(&self) -> &str
	{
		&self.tid
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
	fn tid(&self) -> &str
	{
		&self.tid
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
	fn tid(&self) -> &str
	{
		&self.tid
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
	fn tid(&self) -> &str
	{
		&self.tid
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
	fn tid(&self) -> &str
	{
		&self.tid
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

impl BedLine for BedRecord<BedMethylExtra>
{
	fn tid(&self) -> &str
	{
		&self.tid
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

	fn n_valid_cov(&self) -> Option<u32>
	{
		Some(self.fields.n_valid_cov)
	}
	fn frac_mod(&self) -> Option<f32>
	{
		Some(self.fields.frac_mod)
	}
	fn n_mod(&self) -> Option<u32>
	{
		Some(self.fields.n_mod)
	}
	fn n_canonical(&self) -> Option<u32>
	{
		Some(self.fields.n_canonical)
	}
	fn n_other_mod(&self) -> Option<u32>
	{
		Some(self.fields.n_other_mod)
	}
	fn n_delete(&self) -> Option<u32>
	{
		Some(self.fields.n_delete)
	}
	fn n_fail(&self) -> Option<u32>
	{
		Some(self.fields.n_fail)
	}
	fn n_diff(&self) -> Option<u32>
	{
		Some(self.fields.n_diff)
	}
	fn n_nocall(&self) -> Option<u32>
	{
		Some(self.fields.n_nocall)
	}
}

pub type Record = Box<dyn BedLine>;
