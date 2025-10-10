use std::path::Path;
use std::ops::Range;
use std::io::{SeekFrom, Cursor, BufReader, BufRead, Seek, Read};

use nom::{Parser, Finish, multi::many0};

use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncSeekExt, BufReader as TokioBufReader};
use log::{debug};

use pufferfish::BGZ;

use crate::error;
use crate::AsyncReadSeek;

use crate::bed::*;

enum FileKind<R>
where
	R: AsyncReadSeek + std::marker::Send + std::marker::Unpin,
{
	Plain(TokioBufReader<R>),
	BGZF(TokioBufReader<R>),
}

#[derive(Debug)]
enum BedFormat
{
	BED3,
	BED4,
	BED5,
	BED6,
	BED12,
}

impl TryFrom<String> for BedFormat
{
	type Error = error::Error;

	fn try_from(bed_line: String) -> error::Result<BedFormat>
	{
		let input_format = match bed_line.split_whitespace().count()
		{
			3 => BedFormat::BED3,
			4 => BedFormat::BED4,
			5 => BedFormat::BED5,
			6 => BedFormat::BED6,
			12 => BedFormat::BED12,
			_ => return Err(error::Error::BedFormat),
		};

		Ok(input_format)
	}
}

pub struct Reader<R>
where
	R: AsyncReadSeek + std::marker::Send + std::marker::Unpin,
{
	reader: FileKind<R>,
	format: BedFormat,
}

impl Reader<TokioFile>
{
	pub async fn from_path<P>(path: P) -> error::Result<Self>
	where
		P: AsRef<Path> + std::marker::Copy,
	{
		debug!("{:?}", path.as_ref().file_name().unwrap());

		let gzip_file = TokioFile::open(path).await?;
		Self::from_reader(gzip_file).await
	}
}

impl<R> Reader<R>
where
	R: AsyncReadSeek + std::marker::Send + std::marker::Unpin,
{
	pub async fn from_reader(reader: R) -> error::Result<Self>
	{
		let mut reader = TokioBufReader::new(reader);

		let is_bgzf = reader.is_bgz().await?;

		let format = if is_bgzf
		{
			let block = reader
				.read_bgzf_block(Some(pufferfish::is_bgzf_eof))
				.await
				.map_err(|_| error::Error::BedFormat)?
				.ok_or(error::Error::BedFormat)?;

			let mut line = String::new();
			let mut buf = BufReader::new(Cursor::new(&block));
			buf.read_line(&mut line)?;
			BedFormat::try_from(line)?
		}
		else
		{
			let mut line = String::new();
			reader.read_line(&mut line).await?;
			BedFormat::try_from(line)?
		};

		debug!("Detected bed.gz format as {:?}", format);

		reader.seek(SeekFrom::Start(0)).await?;

		let reader = if is_bgzf
		{
			FileKind::BGZF(reader)
		}
		else
		{
			FileKind::Plain(reader)
		};

		Ok(Reader { reader, format })
	}

	pub async fn read_line(&mut self) -> error::Result<Option<Box<dyn BedLine>>>
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
			FileKind::BGZF(reader) =>
			{
				let Some(block) = reader
					.read_bgzf_block(Some(pufferfish::is_bgzf_eof))
					.await
					.map_err(|_| error::Error::BedFormat)?
				else
				{
					return Ok(None);
				};

				let mut buf = BufReader::new(Cursor::new(&block));
				let bytes = buf.read_line(&mut line)?;
				if bytes == 0
				{
					return Ok(None);
				}
			}
		}

		let line_bytes = line.as_bytes();

		// Parse one record depending on detected format
		let bed: Box<dyn BedLine> = match self.format
		{
			BedFormat::BED3 =>
			{
				let (_, record) = parse_bed3_record(line_bytes)
					.finish()
					.map_err(|_| error::Error::BedFormat)?;
				Box::new(record)
			}
			BedFormat::BED4 =>
			{
				let (_, record) = parse_bed4_record(line_bytes)
					.finish()
					.map_err(|_| error::Error::BedFormat)?;
				Box::new(record)
			}
			BedFormat::BED5 =>
			{
				let (_, record) = parse_bed5_record(line_bytes)
					.finish()
					.map_err(|_| error::Error::BedFormat)?;
				Box::new(record)
			}
			BedFormat::BED6 =>
			{
				let (_, record) = parse_bed6_record(line_bytes)
					.finish()
					.map_err(|_| error::Error::BedFormat)?;
				Box::new(record)
			}
			BedFormat::BED12 =>
			{
				let (_, record) = parse_bed12_record(line_bytes)
					.finish()
					.map_err(|_| error::Error::BedFormat)?;
				Box::new(record)
			}
		};

		Ok(Some(bed))
	}

	async fn read_bytes_for_region(&mut self, region: &[Range<u64>]) -> error::Result<Vec<u8>>
	{
		let mut buffer = Vec::new();

		match &mut self.reader
		{
			FileKind::Plain(reader) =>
			{
				for offset in region
				{
					reader.seek(SeekFrom::Start(offset.start)).await?;
					let mut tmp = vec![0u8; (offset.end - offset.start) as usize];
					reader.read_exact(&mut tmp).await?;
					buffer.extend_from_slice(&tmp);
				}
			}
			FileKind::BGZF(reader) =>
			{
				for offset in region
				{
					let (block_start, uncompressed_start) =
						(offset.start >> 16, offset.start & 0xFFFF);
					let (block_end, uncompressed_end) = (offset.end >> 16, offset.end & 0xFFFF);

					if block_start == 0 && block_end == 0
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

	pub async fn read_region(
		&mut self,
		region: Vec<Range<u64>>,
	) -> error::Result<Option<Vec<Box<dyn BedLine>>>>
	{
		let bytes = self.read_bytes_for_region(&region).await?;

		let records: Vec<Box<dyn BedLine>> = match self.format
		{
			BedFormat::BED3 => many0(parse_bed3_record)
				.parse(&bytes)
				.map_err(|_| error::Error::BedFormat)?
				.1
				.into_iter()
				.map(|r| Box::new(r) as Box<dyn BedLine>)
				.collect(),
			BedFormat::BED4 => many0(parse_bed4_record)
				.parse(&bytes)
				.map_err(|_| error::Error::BedFormat)?
				.1
				.into_iter()
				.map(|r| Box::new(r) as Box<dyn BedLine>)
				.collect(),
			BedFormat::BED5 => many0(parse_bed5_record)
				.parse(&bytes)
				.map_err(|_| error::Error::BedFormat)?
				.1
				.into_iter()
				.map(|r| Box::new(r) as Box<dyn BedLine>)
				.collect(),
			BedFormat::BED6 => many0(parse_bed6_record)
				.parse(&bytes)
				.map_err(|_| error::Error::BedFormat)?
				.1
				.into_iter()
				.map(|r| Box::new(r) as Box<dyn BedLine>)
				.collect(),
			BedFormat::BED12 => many0(parse_bed12_record)
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
