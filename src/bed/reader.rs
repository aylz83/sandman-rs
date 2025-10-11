use std::path::Path;
use std::ops::Range;
use std::io::{SeekFrom, Cursor, BufReader, Seek, Read};
use std::path::PathBuf;

use nom::{Parser, Finish, multi::many0};

use tokio::fs::File as TokioFile;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncSeekExt, BufReader as TokioBufReader};
use log::debug;

use pufferfish::BGZ;

use crate::error;
use crate::AsyncReadSeek;
use crate::tabix;
use crate::bed::*;

enum FileKind<R>
where
	R: AsyncReadSeek + std::marker::Send + std::marker::Unpin,
{
	Plain(TokioBufReader<R>),
	BGZF(TokioBufReader<R>),
}

#[derive(Debug, Clone)]
pub enum BedFormat
{
	BED3
	{
		has_tracks: bool,
		has_browsers: bool,
	},
	BED4
	{
		has_tracks: bool,
		has_browsers: bool,
	},
	BED5
	{
		has_tracks: bool,
		has_browsers: bool,
	},
	BED6
	{
		has_tracks: bool,
		has_browsers: bool,
	},
	BED12
	{
		has_tracks: bool,
		has_browsers: bool,
	},
	BEDMethyl
	{
		has_tracks: bool,
		has_browsers: bool,
	},
}

impl TryFrom<&Vec<String>> for BedFormat
{
	type Error = error::Error;

	fn try_from(bed_lines: &Vec<String>) -> error::Result<BedFormat>
	{
		let mut has_tracks = false;
		let mut has_browsers = false;

		for line in bed_lines
		{
			let trimmed = line.trim();

			if trimmed.is_empty()
			{
				continue;
			}
			else if trimmed.starts_with("track")
			{
				has_tracks = true;
				continue;
			}
			else if trimmed.starts_with("browser")
			{
				has_browsers = true;
				continue;
			}

			// first non-comment, non-header line determines BED kind
			let count = trimmed.split_whitespace().count();
			return match count
			{
				3 => Ok(BedFormat::BED3 {
					has_tracks,
					has_browsers,
				}),
				4 => Ok(BedFormat::BED4 {
					has_tracks,
					has_browsers,
				}),
				5 => Ok(BedFormat::BED5 {
					has_tracks,
					has_browsers,
				}),
				6 => Ok(BedFormat::BED6 {
					has_tracks,
					has_browsers,
				}),
				12 => Ok(BedFormat::BED12 {
					has_tracks,
					has_browsers,
				}),
				18 => Ok(BedFormat::BEDMethyl {
					has_tracks,
					has_browsers,
				}),
				_ => Err(error::Error::BedFormat),
			};
		}

		// If no valid BED line was found
		Err(error::Error::BedFormat)
	}
}

pub struct Reader<R>
where
	R: AsyncReadSeek + std::marker::Send + std::marker::Unpin,
{
	reader: FileKind<R>,
	pub format: BedFormat,

	tbi_reader: Option<tabix::Reader>,
	bgz_buffer: String,
}

impl Reader<TokioFile>
{
	pub async fn from_path<P>(path: P, tabix_path: Option<P>) -> error::Result<Self>
	where
		P: AsRef<Path> + std::marker::Copy,
	{
		let path = path.as_ref();

		let gzip_file = TokioFile::open(path).await?;

		let tabix_file = if let Some(tabix_path) = tabix_path
		{
			Some(TokioFile::open(tabix_path).await?)
		}
		else if path.extension().and_then(|ext| ext.to_str()) == Some("gz")
		{
			let mut tbi_path = PathBuf::from(path);
			tbi_path.set_extension("gz.tbi");

			if tokio::fs::metadata(&tbi_path).await.is_ok()
			{
				debug!("Found tabix file {:?}, opening.", tbi_path);
				Some(TokioFile::open(tbi_path).await?)
			}
			else
			{
				None
			}
		}
		else
		{
			None
		};

		Self::from_reader(gzip_file, tabix_file).await
	}
}

impl<R> Reader<R>
where
	R: AsyncReadSeek + std::marker::Send + std::marker::Unpin,
{
	pub async fn from_reader(reader: R, tbi_reader: Option<R>) -> error::Result<Self>
	{
		let mut reader = TokioBufReader::new(reader);

		let is_bgzf = reader.is_bgz().await?;
		reader.seek(SeekFrom::Start(0)).await?;

		let format = if is_bgzf
		{
			let block = reader
				.read_bgzf_block(Some(pufferfish::is_bgzf_eof))
				.await
				.map_err(|_| error::Error::BedFormat)?
				.ok_or(error::Error::BedFormat)?;

			let mut buffer = TokioBufReader::new(Cursor::new(&block));
			Self::detect_bed_format(&mut buffer, 10).await?
		}
		else
		{
			Self::detect_bed_format(&mut reader, 10).await?
		};

		debug!("Detected bed(.gz) format as {:?}", format);

		reader.seek(SeekFrom::Start(0)).await?;

		let reader = if is_bgzf
		{
			FileKind::BGZF(reader)
		}
		else
		{
			FileKind::Plain(reader)
		};

		let tbi_reader = match tbi_reader
		{
			Some(reader) => Some(tabix::Reader::from_reader(reader).await?),
			None => None,
		};

		Ok(Reader {
			reader,
			format,
			tbi_reader,
			bgz_buffer: String::new(),
		})
	}

	async fn detect_bed_format<B: AsyncBufRead + Unpin>(
		reader: &mut B,
		max_lines: usize,
	) -> error::Result<BedFormat>
	{
		let mut accumulated = Vec::new();
		let mut line = String::new();

		for _ in 0..max_lines
		{
			line.clear();
			let bytes_read = reader
				.read_line(&mut line)
				.await
				.map_err(|_| error::Error::BedFormat)?;
			if bytes_read == 0
			{
				break; // EOF
			}

			accumulated.push(line.clone());

			if let Ok(format) = BedFormat::try_from(&accumulated)
			{
				return Ok(format);
			}
		}

		Err(error::Error::BedFormat)
	}

	fn extract_line_from_buffer(&mut self) -> Option<(String, &str)>
	{
		if let Some(idx) = self.bgz_buffer.find('\n')
		{
			let (line, rest) = self.bgz_buffer.split_at(idx + 1);
			Some((line.to_string(), rest))
		}
		else
		{
			None
		}
	}

	async fn read_bgzf_line(&mut self) -> error::Result<Option<String>>
	{
		// keep reading until we get a complete line
		loop
		{
			// Try to extract a line from existing buffer first
			if let Some((line, rest)) = self.extract_line_from_buffer()
			{
				self.bgz_buffer = rest.to_string();

				if line.trim().is_empty()
				{
					continue;
				}

				return Ok(Some(line));
			}

			// If buffer is empty, read next BGZF block
			let Some(block) = (match &mut self.reader
			{
				FileKind::BGZF(reader) =>
				{
					reader
						.read_bgzf_block(Some(pufferfish::is_bgzf_eof))
						.await?
				}
				_ => None,
			})
			else
			{
				// No more blocks and buffer empty → EOF
				if self.bgz_buffer.is_empty()
				{
					return Ok(None);
				}
				else
				{
					// last line might be missing newline
					let line = std::mem::take(&mut self.bgz_buffer);

					if line.trim().is_empty()
					{
						continue;
					}

					return Ok(Some(line));
				}
			};

			// Append decompressed block to buffer
			self.bgz_buffer
				.push_str(std::str::from_utf8(&block).map_err(|_| error::Error::BedFormat)?);
		}
	}

	pub async fn read_line(&mut self) -> error::Result<Option<Box<dyn BedLine>>>
	{
		loop
		{
			let mut line = String::new();

			match &mut self.reader
			{
				FileKind::Plain(reader) =>
				{
					let bytes = reader.read_line(&mut line).await?;
					if bytes == 0
					{
						return Ok(None);
					}
				}
				FileKind::BGZF(_reader) =>
				{
					if let Some(next_line) = self.read_bgzf_line().await?
					{
						line = next_line;
					}
					else
					{
						return Ok(None);
					}
				}
			}

			if line.trim().is_empty()
			{
				continue; // skip blank lines
			}

			let line_bytes = line.as_bytes();

			// Parse one record depending on detected format
			let bed: Box<dyn BedLine> = match self.format
			{
				BedFormat::BED3 {
					has_tracks: _,
					has_browsers: _,
				} =>
				{
					let (_, record) = parse_bed3_record(line_bytes)
						.finish()
						.map_err(|_| error::Error::BedFormat)?;
					Box::new(record)
				}
				BedFormat::BED4 {
					has_tracks: _,
					has_browsers: _,
				} =>
				{
					let (_, record) = parse_bed4_record(line_bytes)
						.finish()
						.map_err(|_| error::Error::BedFormat)?;
					Box::new(record)
				}
				BedFormat::BED5 {
					has_tracks: _,
					has_browsers: _,
				} =>
				{
					let (_, record) = parse_bed5_record(line_bytes)
						.finish()
						.map_err(|_| error::Error::BedFormat)?;
					Box::new(record)
				}
				BedFormat::BED6 {
					has_tracks: _,
					has_browsers: _,
				} =>
				{
					let (_, record) = parse_bed6_record(line_bytes)
						.finish()
						.map_err(|_| error::Error::BedFormat)?;
					Box::new(record)
				}
				BedFormat::BED12 {
					has_tracks: _,
					has_browsers: _,
				} =>
				{
					let (_, record) = parse_bed12_record(line_bytes)
						.finish()
						.map_err(|_| error::Error::BedFormat)?;
					Box::new(record)
				}
				BedFormat::BEDMethyl {
					has_tracks: _,
					has_browsers: _,
				} =>
				{
					let (_, record) = parse_bedmethyl_record(line_bytes)
						.finish()
						.map_err(|_| error::Error::BedFormat)?;
					Box::new(record)
				}
			};

			return Ok(Some(bed));
		}
	}

	async fn read_bytes_for_region(&mut self, region: &[Range<u64>]) -> error::Result<Vec<u8>>
	{
		let mut buffer = Vec::new();

		match &mut self.reader
		{
			FileKind::Plain(_) =>
			{
				return Err(error::Error::PlainBedRegion);
			}
			FileKind::BGZF(reader) =>
			{
				for offset in region
				{
					let (block_start, uncompressed_start) =
						(offset.start >> 16, offset.start & 0xFFFF);
					let (block_end, uncompressed_end) = (offset.end >> 16, offset.end & 0xFFFF);

					debug!("block_start = {}, block_end = {}", block_start, block_end);

					if offset.start == offset.end
					{
						break;
					}

					reader.seek(SeekFrom::Start(block_start)).await?;

					if block_start == block_end
					{
						Self::read_subblock(
							reader,
							uncompressed_start,
							uncompressed_end,
							&mut buffer,
						)
						.await?;
					}
					else
					{
						let mut current_position = block_start;
						while current_position <= block_end
						{
							let start_offset = if current_position == block_start
							{
								uncompressed_start
							}
							else
							{
								0
							};
							let end_offset = if current_position == block_end
							{
								uncompressed_end
							}
							else
							{
								u64::MAX
							};
							Self::read_subblock(reader, start_offset, end_offset, &mut buffer)
								.await?;
							current_position = reader.seek(SeekFrom::Current(0)).await?;
						}
					}
				}
			}
		}

		Ok(buffer)
	}

	pub async fn read_lines_in_tid(
		&mut self,
		tid: &str,
	) -> error::Result<Option<Vec<Box<dyn BedLine>>>>
	{
		if matches!(self.reader, FileKind::Plain(_))
		{
			return Err(error::Error::PlainBedRegion);
		}

		match &self.tbi_reader
		{
			Some(reader) =>
			{
				let Some(ranges) = reader.offsets_for_tid(tid)?
				else
				{
					return Ok(None);
				};

				self.read_lines_in_range(ranges).await
			}
			None => return Err(error::Error::TabixNotOpen),
		}
	}

	pub async fn read_lines_in_tid_region(
		&mut self,
		tid: &str,
		start: u32,
		end: u32,
	) -> error::Result<Option<Vec<Box<dyn BedLine>>>>
	{
		if matches!(self.reader, FileKind::Plain(_))
		{
			return Err(error::Error::PlainBedRegion);
		}

		match &self.tbi_reader
		{
			Some(reader) =>
			{
				let Some(ranges) = reader.offsets_for_tid_region(tid, start, end)?
				else
				{
					return Ok(None);
				};

				match self.read_lines_in_range(ranges).await?
				{
					Some(mut records) =>
					{
						records.retain(|rec| {
							rec.tid() == tid && rec.start() < end && rec.end() > start
						});
						Ok(Some(records))
					}
					None => Ok(None),
				}
			}
			None => return Err(error::Error::TabixNotOpen),
		}
	}

	pub async fn read_lines_in_range(
		&mut self,
		region: Vec<Range<u64>>,
	) -> error::Result<Option<Vec<Box<dyn BedLine>>>>
	{
		let bytes = self.read_bytes_for_region(&region).await?;
		debug!("read bytes = {:?}", bytes);

		let records: Vec<Box<dyn BedLine>> = match self.format
		{
			BedFormat::BED3 {
				has_tracks: _,
				has_browsers: _,
			} => many0(parse_bed3_record)
				.parse(&bytes)
				.map_err(|_| error::Error::BedFormat)?
				.1
				.into_iter()
				.map(|r| Box::new(r) as Box<dyn BedLine>)
				.collect(),
			BedFormat::BED4 {
				has_tracks: _,
				has_browsers: _,
			} => many0(parse_bed4_record)
				.parse(&bytes)
				.map_err(|_| error::Error::BedFormat)?
				.1
				.into_iter()
				.map(|r| Box::new(r) as Box<dyn BedLine>)
				.collect(),
			BedFormat::BED5 {
				has_tracks: _,
				has_browsers: _,
			} => many0(parse_bed5_record)
				.parse(&bytes)
				.map_err(|_| error::Error::BedFormat)?
				.1
				.into_iter()
				.map(|r| Box::new(r) as Box<dyn BedLine>)
				.collect(),
			BedFormat::BED6 {
				has_tracks: _,
				has_browsers: _,
			} => many0(parse_bed6_record)
				.parse(&bytes)
				.map_err(|_| error::Error::BedFormat)?
				.1
				.into_iter()
				.map(|r| Box::new(r) as Box<dyn BedLine>)
				.collect(),
			BedFormat::BED12 {
				has_tracks: _,
				has_browsers: _,
			} => many0(parse_bed12_record)
				.parse(&bytes)
				.map_err(|_| error::Error::BedFormat)?
				.1
				.into_iter()
				.map(|r| Box::new(r) as Box<dyn BedLine>)
				.collect(),
			BedFormat::BEDMethyl {
				has_tracks: _,
				has_browsers: _,
			} => many0(parse_bedmethyl_record)
				.parse(&bytes)
				.map_err(|_| error::Error::BedFormat)?
				.1
				.into_iter()
				.map(|r| Box::new(r) as Box<dyn BedLine>)
				.collect(),
		};

		debug!("records.len = {}", &records.len());
		Ok(Some(records))
	}

	async fn read_subblock(
		reader: &mut TokioBufReader<R>,
		start: u64,
		end: u64,
		bgzf_block: &mut Vec<u8>,
	) -> error::Result<()>
	{
		if let Ok(Some(block)) = reader.read_bgzf_block(Some(pufferfish::is_bgzf_eof)).await
		{
			let block_len = block.len() as u64;
			let start_offset = start.min(block_len);
			let end_offset = end.min(block_len);

			if start_offset < end_offset
			{
				let mut bytes = vec![0u8; (end_offset - start_offset) as usize];
				let cursor = Cursor::new(&block);
				let mut buffer = BufReader::new(cursor);
				buffer.seek(SeekFrom::Start(start_offset))?;
				buffer.read_exact(&mut bytes)?;
				bgzf_block.extend_from_slice(&bytes);
			}
		}

		Ok(())
	}
}
