use std::io::Cursor;
use std::path::Path;
use std::collections::HashMap;
use std::str::FromStr;
use std::ops::Range;

use tokio::fs::File as TokioFile;
use tokio::io::{BufReader as TokioBufReader};

use indexset::BTreeSet;
use log::debug;

use byteorder::{LittleEndian, ReadBytesExt};

use pufferfish::BGZ;

use crate::AsyncReadSeek;
use crate::error;

pub struct Header
{
	pub n_ref: i32,
	pub col_seq: i32,
	pub col_beg: i32,
	pub col_end: i32,
	pub meta: i32,
	pub skip: i32,
}

pub struct Region
{
	pub chunks: Vec<Range<u64>>,
}

pub struct Reference
{
	pub bins: HashMap<u32, Region>,
}

pub struct Reader
{
	pub header: Header,

	pub seqnames: BTreeSet<String>,
	pub ref_indices: Vec<Reference>,
}

impl Reader
{
	pub async fn from_path<P>(path: P) -> error::Result<Self>
	where
		P: AsRef<Path> + std::marker::Copy,
	{
		debug!("{:?}", path.as_ref().file_name().unwrap());

		let tabix_file = TokioFile::open(path).await?;
		Self::from_reader(tabix_file).await
	}

	pub async fn from_reader<R>(reader: R) -> error::Result<Self>
	where
		R: AsyncReadSeek + std::marker::Send + std::marker::Unpin,
	{
		let mut async_reader = TokioBufReader::new(reader);

		let (header, seqnames, ref_indices) = Self::read_tabix(&mut async_reader).await?;

		Ok(Reader {
			header,
			seqnames,
			ref_indices,
		})
	}

	pub fn offsets_for_seqname(&self, seqname: &str) -> error::Result<Vec<Range<u64>>>
	{
		let idx = self.seqnames.iter().position(|s| s == seqname).unwrap();

		let index = &self.ref_indices[idx];

		let mut chunks = Vec::new();
		for bin_entry in index.bins.values()
		{
			chunks.extend_from_slice(&bin_entry.chunks);
		}

		Ok(chunks)
	}

	async fn read_tabix<R>(
		reader: &mut TokioBufReader<R>,
	) -> error::Result<(Header, BTreeSet<String>, Vec<Reference>)>
	where
		R: AsyncReadSeek + std::marker::Send + std::marker::Unpin,
	{
		let mut bytes = Vec::new();
		loop
		{
			match reader
				.read_bgzf_block(Some(pufferfish::is_bgzf_eof))
				.await?
			{
				Some(block) =>
				{
					debug!("Reading BGZF block");
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
		let format = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let col_seq = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let col_beg = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let col_end = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let meta = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let skip = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;
		let l_nm = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;

		debug!("magic string = {:?}", unsafe {
			std::str::from_utf8_unchecked(&magic).to_string()
		});

		debug!("n_ref = {}", n_ref);
		debug!("format = {}", format);
		debug!("col_seq = {}", col_seq);
		debug!("col_beg = {}", col_beg);
		debug!("col_end = {}", col_end);
		debug!("meta = {}", meta);
		debug!("skip = {}", skip);
		debug!("l_nm = {}", l_nm);

		let mut seqnames = vec![0u8; l_nm as usize];
		std::io::Read::read_exact(&mut cursor, &mut seqnames)?;

		let seqnames = unsafe { std::str::from_utf8_unchecked(&seqnames).to_string() };
		let seqnames = seqnames
			.split("\0")
			.filter(|seqname| seqname != &"")
			.map(|seqname| String::from_str(seqname).unwrap())
			.collect::<BTreeSet<_>>();

		debug!("names = {:?}", seqnames);

		let mut ref_indices = Vec::with_capacity(n_ref as usize);

		for _ in 0..n_ref
		{
			//debug!("current_reference = {:?}", current_reference);

			let n_bin = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;

			// debug!("n_bin = {}", n_bin);

			let mut bins_map = HashMap::with_capacity(n_bin as usize);

			for _ in 0..n_bin
			{
				// debug!("current_bin = {}", current_bin);

				let bin = ReadBytesExt::read_u32::<LittleEndian>(&mut cursor)?;
				let n_chunk = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;

				let mut chunks = Vec::with_capacity(n_chunk as usize);

				// debug!("bin = {}", bin);
				// debug!("n_chunk = {:?}", n_chunk);

				for _ in 0..n_chunk
				{
					// debug!("current_chunk = {:?}", current_chunk);

					let cnk_beg = ReadBytesExt::read_u64::<LittleEndian>(&mut cursor)?;
					let cnk_end = ReadBytesExt::read_u64::<LittleEndian>(&mut cursor)?;

					chunks.push(Range {
						start: cnk_beg,
						end: cnk_end,
					});

					// debug!("cnk_beg = {:?}", cnk_beg);
					// debug!("cnk_end = {:?}", cnk_end);
				}

				bins_map.insert(bin, Region { chunks });
			}

			ref_indices.push(Reference { bins: bins_map });

			let n_intv = ReadBytesExt::read_i32::<LittleEndian>(&mut cursor)?;

			// debug!("n_intv = {:?}", n_intv);

			for _ in 0..n_intv
			{
				// debug!("current_interval = {:?}", current_interval);

				let _ioff = ReadBytesExt::read_u64::<LittleEndian>(&mut cursor)?;

				// debug!("ioff = {:?}", ioff);
			}
		}

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
