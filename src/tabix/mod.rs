use std::io::Cursor;
use std::path::Path;
use std::collections::HashMap;
use std::str::FromStr;
use std::ops::Range;

use tokio::fs::File as TokioFile;
use tokio::io::{AsyncRead, AsyncSeek, BufReader as TokioBufReader};

use byteorder::{LittleEndian, ReadBytesExt};

use pufferfish::prelude::*;

use crate::error;

#[derive(Debug)]
pub struct Header
{
	pub n_ref: i32,
	pub col_seq: i32,
	pub col_beg: i32,
	pub col_end: i32,
	pub meta: i32,
	pub skip: i32,
}

#[derive(Debug)]
pub struct Region
{
	pub chunks: Vec<Range<u64>>,
}

#[derive(Debug)]
pub struct Reference
{
	pub bins: HashMap<u64, Region>,
}

#[derive(Debug)]
pub struct Reader
{
	pub header: Header,

	pub seqnames: Vec<String>,
	pub ref_indices: Vec<Reference>,
}

impl Reader
{
	pub async fn from_path<P>(path: P) -> error::Result<Self>
	where
		P: AsRef<Path> + std::marker::Copy,
	{
		let tabix_file = TokioFile::open(path).await?;
		Self::from_reader(tabix_file).await
	}

	pub async fn from_reader<R>(reader: R) -> error::Result<Self>
	where
		R: AsyncRead + AsyncSeek + std::marker::Send + std::marker::Unpin,
	{
		let mut async_reader = TokioBufReader::new(reader);

		let (header, seqnames, ref_indices) = Self::read_tabix(&mut async_reader).await?;

		Ok(Reader {
			header,
			seqnames,
			ref_indices,
		})
	}

	pub fn offsets_for_tid(&self, tid: &str) -> error::Result<Option<Vec<Range<u64>>>>
	{
		let Some(idx) = self.seqnames.iter().position(|s| s == tid)
		else
		{
			return Ok(None); // chromosome missing
		};

		let index = &self.ref_indices[idx];

		let mut chunks = Vec::new();
		for bin_entry in index.bins.values()
		{
			chunks.extend_from_slice(&bin_entry.chunks);
		}

		Ok(Some(chunks))
	}

	pub fn offsets_for_tid_region(
		&self,
		tid: &str,
		start: u64,
		end: u64,
	) -> error::Result<Option<Vec<Range<u64>>>>
	{
		let Some(idx) = self.seqnames.iter().position(|s| s == tid)
		else
		{
			return Ok(None); // chromosome missing
		};

		let index = &self.ref_indices[idx];

		let mut chunks = Vec::new();

		for bin in Self::region_bins(start, end)
		{
			if let Some(region) = index.bins.get(&bin)
			{
				chunks.extend_from_slice(&region.chunks);
			}
		}

		Ok(Some(chunks))
	}

	fn region_bins(start: u64, end: u64) -> Vec<u64>
	{
		const MAX_POS: u64 = 1 << 29; // maximum coordinate (512 Mb)
		const BIN_OFFSETS: [u64; 6] = [0, 1, 9, 73, 585, 4681];

		let mut bins = Vec::new();

		if start >= MAX_POS
		{
			return bins;
		}

		// Tabix defines bins as 0-based, half-open intervals [start, end)
		let mut end = if end > 0 { end - 1 } else { 0 };
		if end >= MAX_POS
		{
			end = MAX_POS - 1;
		}

		bins.push(0); // root bin

		// Level 1 (512 Mb / 8)
		for k in (BIN_OFFSETS[1] + (start >> 26))..=(BIN_OFFSETS[1] + (end >> 26))
		{
			bins.push(k);
		}

		// Level 2 (64 Mb)
		for k in (BIN_OFFSETS[2] + (start >> 23))..=(BIN_OFFSETS[2] + (end >> 23))
		{
			bins.push(k);
		}

		// Level 3 (8 Mb)
		for k in (BIN_OFFSETS[3] + (start >> 20))..=(BIN_OFFSETS[3] + (end >> 20))
		{
			bins.push(k);
		}

		// Level 4 (1 Mb)
		for k in (BIN_OFFSETS[4] + (start >> 17))..=(BIN_OFFSETS[4] + (end >> 17))
		{
			bins.push(k);
		}

		// Level 5 (128 kb)
		for k in (BIN_OFFSETS[5] + (start >> 14))..=(BIN_OFFSETS[5] + (end >> 14))
		{
			bins.push(k);
		}

		bins
	}

	async fn read_tabix<R>(
		reader: &mut TokioBufReader<R>,
	) -> error::Result<(Header, Vec<String>, Vec<Reference>)>
	where
		R: AsyncRead + AsyncSeek + std::marker::Send + std::marker::Unpin,
	{
		let mut bytes = Vec::new();
		loop
		{
			match reader.read_bgzf_block(Some(is_bgzf_eof)).await?
			{
				Some(block) =>
				{
					bytes.extend_from_slice(&block);
				}
				None => break,
			};
		}

		let mut cursor = Cursor::new(bytes);

		let mut magic = [0u8; 4];
		std::io::Read::read_exact(&mut cursor, &mut magic)?;

		//if magic != r"TBI\1"
		// {
		// 	bail!("Not a tabix file");
		// }

		let n_ref = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let _ = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let col_seq = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let col_beg = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let col_end = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let meta = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let skip = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let l_nm = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;

		let mut seqnames = vec![0u8; l_nm as usize];
		std::io::Read::read_exact(&mut cursor, &mut seqnames)?;

		let seqnames = unsafe { std::str::from_utf8_unchecked(&seqnames).to_string() };
		let seqnames = seqnames
			.split("\0")
			.filter(|seqname| seqname != &"")
			.map(|seqname| String::from_str(seqname).unwrap())
			.collect::<Vec<_>>();

		let mut ref_indices = Vec::with_capacity(n_ref as usize);

		for _ in 0..n_ref
		{
			let n_bin = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;

			let mut bins_map = HashMap::with_capacity(n_bin as usize);

			for _ in 0..n_bin
			{
				let bin = ReadBytesExt::read_u32::<LittleEndian>(&mut cursor)? as u64;
				let n_chunk = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;

				let mut chunks = Vec::with_capacity(n_chunk as usize);

				for _ in 0..n_chunk
				{
					let cnk_beg = ReadBytesExt::read_u64::<LittleEndian>(&mut cursor)?;
					let cnk_end = ReadBytesExt::read_u64::<LittleEndian>(&mut cursor)?;

					chunks.push(Range {
						start: cnk_beg,
						end: cnk_end,
					});
				}

				bins_map.insert(bin, Region { chunks });
			}

			ref_indices.push(Reference { bins: bins_map });

			let n_intv = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;

			for _ in 0..n_intv
			{
				let _ioff = ReadBytesExt::read_u64::<LittleEndian>(&mut cursor)?;
			}
		}

		// for (bin, region) in &ref_indices[0].bins
		// {
		// }

		Ok((
			Header {
				n_ref,
				col_seq,
				col_beg,
				col_end,
				meta,
				skip,
			},
			seqnames,
			ref_indices,
		))
	}
}
