use faisync::Contigs;

use std::borrow::Cow;
use std::collections::HashMap;

use crate::bed::Strand;
use crate::error;

pub struct PrettyMap<'a>(pub &'a HashMap<Cow<'a, str>, u8>);

impl<'a> std::fmt::Display for PrettyMap<'a>
{
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
	{
		let mut entries: Vec<_> = self.0.iter().collect();
		entries.sort_by(|(k1, _), (k2, _)| k1.as_ref().cmp(k2.as_ref()));

		for (i, (k, v)) in entries.iter().enumerate()
		{
			if i > 0
			{
				writeln!(f)?;
			}
			write!(f, "{} = {}", k, v)?;
		}

		Ok(())
	}
}

pub struct BaseChecker(pub Contigs, pub HashMap<Cow<'static, str>, u8>);

impl BaseChecker
{
	pub async fn check_base(
		&self,
		tid: &str,
		start: u64,
		end: u64,
		strand: &Strand,
		base: &str,
	) -> error::Result<bool>
	{
		// Determine the position to check
		let pos = match strand
		{
			Strand::Plus | Strand::Both => start,
			Strand::Minus => end.saturating_sub(1),
		};

		let contig = self
			.0
			.get(tid)
			.ok_or(error::Error::TidNotFound(tid.to_string()))?;

		// Read the base from the fasta
		let mut ref_base =
			contig
				.base_at(pos.into())
				.await
				.ok_or(error::Error::TidRegionNotFound(
					tid.to_string(),
					pos,
					base.to_string(),
				))?;

		// let mut ref_base = ref_base.clone();
		// ref_base.make_ascii_uppercase();

		// Reverse complement if strand is minus
		if let Strand::Minus = strand
		{
			ref_base = faisync::reverse_complement(ref_base);
		}

		// Lookup the expected base for the modification
		let expected_base = self.1.get(base).ok_or(error::Error::BaseLookupFailed(
			PrettyMap(&self.1).to_string(),
			base.to_string(),
			(ref_base as char).to_string(),
		))?;

		Ok(ref_base == *expected_base)
	}
}
