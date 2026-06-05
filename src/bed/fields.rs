use crate::error;

use std::path::Path;
use std::str::FromStr;

use serde::{Serialize, Deserialize};

use crate::bed::detect_format;
use crate::bed::BedKind;

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ScoreField
{
	Score = 0,
	NValidCov,
	FracMod,
	NMod,
	NCanonical,
	NOtherMod,
	NDelete,
	NFail,
	NDiff,
	NNoCall,
}

impl ScoreField
{
	#[inline]
	pub fn as_usize(self) -> usize
	{
		self as usize
	}

	pub async fn best_from_file(file: &Path) -> Self
	{
		let bed_format = detect_format(file).await;
		match bed_format
		{
			Ok(BedKind::BedMethyl) => ScoreField::NMod,
			_ => ScoreField::Score,
		}
	}
}

impl From<ScoreField> for u8
{
	fn from(field: ScoreField) -> Self
	{
		field as u8
	}
}

impl TryFrom<u8> for ScoreField
{
	type Error = error::Error;

	fn try_from(value: u8) -> Result<Self, Self::Error>
	{
		match value
		{
			0 => Ok(ScoreField::Score),
			1 => Ok(ScoreField::NValidCov),
			2 => Ok(ScoreField::FracMod),
			3 => Ok(ScoreField::NMod),
			4 => Ok(ScoreField::NCanonical),
			5 => Ok(ScoreField::NOtherMod),
			6 => Ok(ScoreField::NDelete),
			7 => Ok(ScoreField::NFail),
			8 => Ok(ScoreField::NDiff),
			9 => Ok(ScoreField::NNoCall),
			_ => Err(error::Error::InvalidScoreField((value as char).to_string())),
		}
	}
}

impl TryFrom<&str> for ScoreField
{
	type Error = error::Error;

	fn try_from(value: &str) -> error::Result<Self>
	{
		match value
		{
			"score" | "Score" => Ok(ScoreField::Score),
			"n_valid_cov" | "NValidCov" => Ok(ScoreField::NValidCov),
			"frac_mod" | "FracMod" => Ok(ScoreField::FracMod),
			"n_mod" | "NMod" => Ok(ScoreField::NMod),
			"n_canonical" | "NCanonical" => Ok(ScoreField::NCanonical),
			"n_other_mod" | "NOtherMod" => Ok(ScoreField::NOtherMod),
			"n_delete" | "NDelete" => Ok(ScoreField::NDelete),
			"n_fail" | "NFail" => Ok(ScoreField::NFail),
			"n_diff" | "NDiff" => Ok(ScoreField::NDiff),
			"n_no_call" | "NNoCall" => Ok(ScoreField::NNoCall),
			_ => Err(error::Error::InvalidScoreField(value.to_string())),
		}
	}
}

impl std::fmt::Display for ScoreField
{
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
	{
		let s = match self
		{
			ScoreField::Score => "score",
			ScoreField::NValidCov => "n_valid_cov",
			ScoreField::FracMod => "frac_mod",
			ScoreField::NMod => "n_mod",
			ScoreField::NCanonical => "n_canonical",
			ScoreField::NOtherMod => "n_other_mod",
			ScoreField::NDelete => "n_delete",
			ScoreField::NFail => "n_fail",
			ScoreField::NDiff => "n_diff",
			ScoreField::NNoCall => "n_no_call",
		};
		write!(f, "{}", s)
	}
}

impl FromStr for ScoreField
{
	type Err = error::Error;

	fn from_str(s: &str) -> error::Result<Self>
	{
		match s
		{
			"score" | "Score" => Ok(ScoreField::Score),
			"n_valid_cov" | "NValidCov" => Ok(ScoreField::NValidCov),
			"frac_mod" | "FracMod" => Ok(ScoreField::FracMod),
			"n_mod" | "NMod" => Ok(ScoreField::NMod),
			"n_canonical" | "NCanonical" => Ok(ScoreField::NCanonical),
			"n_other_mod" | "NOtherMod" => Ok(ScoreField::NOtherMod),
			"n_delete" | "NDelete" => Ok(ScoreField::NDelete),
			"n_fail" | "NFail" => Ok(ScoreField::NFail),
			"n_diff" | "NDiff" => Ok(ScoreField::NDiff),
			"n_no_call" | "NNoCall" => Ok(ScoreField::NNoCall),
			_ => Err(error::Error::InvalidScoreField(s.to_string())),
		}
	}
}

impl From<ScoreField> for String
{
	fn from(field: ScoreField) -> Self
	{
		field.to_string()
	}
}
