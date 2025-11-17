use std::path::Path;
use std::ops::Range;
use std::io::{SeekFrom, Cursor, BufReader, Seek, Read};
use std::path::PathBuf;
use std::borrow::BorrowMut;

use nom::Finish;

use tokio::fs::File as TokioFile;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncSeekExt, BufReader as TokioBufReader};
use std::sync::Arc;
use tokio::sync::Mutex;

use pufferfish::BGZ;

use crate::error;
use crate::AsyncReadSeek;
use crate::tabix;
use crate::bed::*;

use crate::bed::parser::parse_all_records;
use crate::bed::parser::parse_browser_line;
use crate::bed::parser::parse_track_line;
use crate::bed::parser::parse_bed3_record;
use crate::bed::parser::parse_bed4_record;
use crate::bed::parser::parse_bed5_record;
use crate::bed::parser::parse_bed6_record;
use crate::bed::parser::parse_bed12_record;
use crate::bed::parser::parse_bedmethyl_record;
use crate::store::TidResolver;

#[cfg(feature = "interning")]
use crate::store::TidStore;

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
				_ => Err(error::Error::Parse(trimmed.to_string())),
			};
		}

		// If no valid BED line was found
		Err(error::Error::AutoDetect)
	}
}

pub struct Reader<R, T>
where
	R: AsyncReadSeek + std::marker::Send + std::marker::Unpin,
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync,
{
	name: String,
	reader: FileKind<R>,
	pub format: BedFormat,

	tbi_reader: Option<tabix::Reader>,
	bgz_buffer: String,
	track_line: Option<Track>,
	last_browser: Option<BrowserMeta>,
	reset_browser: bool,
	resolver: Arc<Mutex<T>>,
}

#[cfg(not(feature = "interning"))]
impl Reader<TokioFile, ()>
{
	pub async fn from_path<P>(path: P, tabix_path: Option<P>) -> error::Result<Self>
	where
		P: AsRef<Path> + std::marker::Copy,
	{
		let name = path
			.as_ref()
			.file_name()
			.and_then(|s| s.to_str())
			.unwrap_or("unknown")
			.to_string();

		let (gzip_file, tabix_file) = Self::open_bed_files(path, tabix_path).await?;
		let reader = Self::from_reader(name, gzip_file, tabix_file).await?;

		Ok(reader)
	}
}

#[cfg(feature = "interning")]
impl Reader<TokioFile, TidStore>
{
	pub async fn from_path<P>(path: P, tabix_path: Option<P>) -> error::Result<Self>
	where
		P: AsRef<Path> + std::marker::Copy,
	{
		let name = path
			.as_ref()
			.file_name()
			.and_then(|s| s.to_str())
			.unwrap_or("unknown")
			.to_string();

		let (gzip_file, tabix_file) = Self::open_bed_files(path, tabix_path).await?;
		let reader = Self::from_reader(name, gzip_file, tabix_file).await?;

		Ok(reader)
	}

	pub async fn from_path_with_resolver<P>(
		path: P,
		tabix_path: Option<P>,
		resolver: Arc<Mutex<TidStore>>,
	) -> error::Result<Self>
	where
		P: AsRef<Path> + std::marker::Copy,
	{
		let name = path
			.as_ref()
			.file_name()
			.and_then(|s| s.to_str())
			.unwrap_or("unknown")
			.to_string();

		let (gzip_file, tabix_file) = Self::open_bed_files(path, tabix_path).await?;
		let reader = Self::from_reader_with_resolver(name, gzip_file, tabix_file, resolver).await?;

		Ok(reader)
	}
}

#[cfg(not(feature = "interning"))]
impl<R> Reader<R, ()>
where
	R: AsyncReadSeek + std::marker::Send + std::marker::Unpin,
{
	pub async fn from_reader(name: String, reader: R, tbi_reader: Option<R>)
		-> error::Result<Self>
	{
		let (format, reader, tbi_reader) =
			Self::open_bed_readers(&name, reader, tbi_reader).await?;

		Ok(Reader {
			name,
			reader,
			format,
			tbi_reader,
			bgz_buffer: String::new(),
			track_line: None,
			last_browser: None,
			reset_browser: true,
			resolver: Arc::new(Mutex::new(())),
		})
	}
}

#[cfg(feature = "interning")]
impl<R> Reader<R, TidStore>
where
	R: AsyncReadSeek + std::marker::Send + std::marker::Unpin,
{
	pub async fn from_reader(name: String, reader: R, tbi_reader: Option<R>)
		-> error::Result<Self>
	{
		let (format, reader, tbi_reader) =
			Self::open_bed_readers(&name, reader, tbi_reader).await?;

		let resolver = Arc::new(Mutex::new(TidStore::default()));
		if let Some(tbi) = &tbi_reader
		{
			for tid in &tbi.seqnames
			{
				resolver.lock().await.to_symbol_id(&tid);
			}
		}

		Ok(Reader {
			name,
			reader,
			format,
			tbi_reader,
			bgz_buffer: String::new(),
			track_line: None,
			last_browser: None,
			reset_browser: true,
			resolver,
		})
	}

	pub async fn from_reader_with_resolver(
		name: String,
		reader: R,
		tbi_reader: Option<R>,
		resolver: Arc<Mutex<TidStore>>,
	) -> error::Result<Self>
	{
		let (format, reader, tbi_reader) =
			Self::open_bed_readers(&name, reader, tbi_reader).await?;

		if let Some(tbi) = &tbi_reader
		{
			for tid in &tbi.seqnames
			{
				resolver.lock().await.to_symbol_id(&tid);
			}
		}

		Ok(Reader {
			name,
			reader,
			format,
			tbi_reader,
			bgz_buffer: String::new(),
			track_line: None,
			last_browser: None,
			reset_browser: true,
			resolver,
		})
	}
}

impl<R, T> Reader<R, T>
where
	R: AsyncReadSeek + std::marker::Send + std::marker::Unpin,
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	async fn open_bed_readers(
		name: &String,
		reader: R,
		tbi_reader: Option<R>,
	) -> error::Result<(BedFormat, FileKind<R>, Option<tabix::Reader>)>
	{
		let mut reader = TokioBufReader::new(reader);

		let is_bgzf = reader.is_bgz().await?;
		reader.seek(SeekFrom::Start(0)).await?;

		let format = if is_bgzf
		{
			let block = reader
				.read_bgzf_block(Some(pufferfish::is_bgzf_eof))
				.await
				.map_err(|_| error::Error::BedFormat(name.clone()))?
				.ok_or(error::Error::BedFormat(name.clone()))?;

			let mut buffer = TokioBufReader::new(Cursor::new(&block));
			Self::detect_bed_format(name.clone(), &mut buffer, 10).await?
		}
		else
		{
			Self::detect_bed_format(name.clone(), &mut reader, 10).await?
		};

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

		Ok((format, reader, tbi_reader))
	}

	async fn open_bed_files<P>(
		path: P,
		tabix_path: Option<P>,
	) -> error::Result<(TokioFile, Option<TokioFile>)>
	where
		P: AsRef<Path> + Copy,
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
				Some(TokioFile::open(&tbi_path).await?)
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

		Ok((gzip_file, tabix_file))
	}

	async fn detect_bed_format<B: AsyncBufRead + Unpin>(
		name: String,
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
				.map_err(|_| error::Error::BedFormat(name.clone()))?;
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

		Err(error::Error::BedFormat(name))
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
			self.bgz_buffer.push_str(
				std::str::from_utf8(&block)
					.map_err(|_| error::Error::BedFormat(self.name.clone()))?,
			);
		}
	}

	pub async fn store(&mut self) -> Arc<Mutex<T>>
	{
		self.resolver.clone()
	}

	pub async fn seek_to_start(&mut self) -> error::Result<()>
	{
		match &mut self.reader
		{
			FileKind::Plain(reader) =>
			{
				reader.seek(SeekFrom::Start(0)).await?;
			}
			FileKind::BGZF(reader) =>
			{
				reader.seek(SeekFrom::Start(0)).await?;
			}
		}

		Ok(())
	}

	pub async fn read_line(&mut self) -> error::Result<Option<(&Option<Track>, Record<T::Tid>)>>
	{
		while let Some(line) = self.read_line_io().await?
		{
			let line_bytes = line.as_bytes();

			if line.starts_with("track")
			{
				let (_, track) = parse_track_line(&line)
					.finish()
					.map_err(|_| error::Error::BedFormat(self.name.clone()))?;
				self.track_line = Some(track);
				continue;
			}

			if line.starts_with("browser")
			{
				// Skip browser lines for this simple reader
				continue;
			}

			let record = self.parse_record(line_bytes).await?;
			return Ok(Some((&self.track_line, record)));
		}

		Ok(None)
	}

	pub async fn read_line_with_meta(
		&mut self,
		browser_meta: &mut Option<BrowserMeta>,
	) -> error::Result<Option<(&Option<Track>, Record<T::Tid>)>>
	{
		while let Some(line) = self.read_line_io().await?
		{
			let line_bytes = line.as_bytes();

			if line.starts_with("track")
			{
				let (_, track) = parse_track_line(&line)
					.finish()
					.map_err(|_| error::Error::BedFormat(self.name.clone()))?;
				self.track_line = Some(track);
				continue;
			}

			if line.starts_with("browser")
			{
				let (_, parsed) = parse_browser_line(&line)
					.finish()
					.map_err(|_| error::Error::BedFormat(self.name.clone()))?;

				if self.reset_browser
				{
					// Previous BED rows are done. Start a new browser block.
					self.last_browser = Some(parsed);
				}
				else
				{
					// Consecutive browser lines → merge
					if let Some(existing) = self.last_browser.as_mut()
					{
						existing.attrs.extend(parsed.attrs);
					}
					else
					{
						self.last_browser = Some(parsed);
					}
				}

				self.reset_browser = false;

				continue;
			}

			self.reset_browser = true;

			let record = self.parse_record(line_bytes).await?;
			*browser_meta = self.last_browser.clone();

			return Ok(Some((&self.track_line, record)));
		}

		Ok(None)
	}

	async fn read_line_io(&mut self) -> error::Result<Option<String>>
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
				FileKind::BGZF(_) =>
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
				continue;
			}

			return Ok(Some(line));
		}
	}

	async fn parse_record(&mut self, line_bytes: &[u8]) -> error::Result<Record<T::Tid>>
	{
		match self.format
		{
			BedFormat::BED3 { .. } =>
			{
				let (_, r) = parse_bed3_record(self.resolver.clone(), &line_bytes)
					.await
					.finish()
					.map_err(|_| error::Error::BedFormat(self.name.clone()))?;
				Ok(Box::new(r))
			}
			BedFormat::BED4 { .. } =>
			{
				let (_, r) = parse_bed4_record(self.resolver.clone(), &line_bytes)
					.await
					.finish()
					.map_err(|_| error::Error::BedFormat(self.name.clone()))?;
				Ok(Box::new(r))
			}
			BedFormat::BED5 { .. } =>
			{
				let (_, r) = parse_bed5_record(self.resolver.clone(), &line_bytes)
					.await
					.finish()
					.map_err(|_| error::Error::BedFormat(self.name.clone()))?;
				Ok(Box::new(r))
			}
			BedFormat::BED6 { .. } =>
			{
				let (_, r) = parse_bed6_record(self.resolver.clone(), &line_bytes)
					.await
					.finish()
					.map_err(|_| error::Error::BedFormat(self.name.clone()))?;
				Ok(Box::new(r))
			}
			BedFormat::BED12 { .. } =>
			{
				let (_, r) = parse_bed12_record(self.resolver.clone(), &line_bytes)
					.await
					.finish()
					.map_err(|_| error::Error::BedFormat(self.name.clone()))?;
				Ok(Box::new(r))
			}
			BedFormat::BEDMethyl { .. } =>
			{
				let (_, r) = parse_bedmethyl_record(self.resolver.clone(), &line_bytes)
					.await
					.finish()
					.map_err(|_| error::Error::BedFormat(self.name.clone()))?;
				Ok(Box::new(r))
			}
		}
	}

	async fn read_bytes_for_region(&mut self, region: &[Range<u64>]) -> error::Result<Vec<u8>>
	{
		let mut buffer = Vec::new();

		match &mut self.reader
		{
			FileKind::Plain(_) =>
			{
				return Err(error::Error::PlainBedRegion(self.name.clone()));
			}
			FileKind::BGZF(reader) =>
			{
				for offset in region
				{
					let (block_start, uncompressed_start) =
						(offset.start >> 16, offset.start & 0xFFFF);
					let (block_end, uncompressed_end) = (offset.end >> 16, offset.end & 0xFFFF);

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
	) -> error::Result<Option<Vec<Record<T::Tid>>>>
	{
		if matches!(self.reader, FileKind::Plain(_))
		{
			return Err(error::Error::PlainBedRegion(self.name.clone()));
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
			None => return Err(error::Error::TabixNotOpen(self.name.clone())),
		}
	}

	pub async fn read_lines_in_tid_region(
		&mut self,
		tid: &str,
		start: u64,
		end: u64,
	) -> error::Result<Option<Vec<Record<T::Tid>>>>
	{
		if matches!(self.reader, FileKind::Plain(_))
		{
			return Err(error::Error::PlainBedRegion(self.name.clone()));
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

				let lines = self.read_lines_in_range(ranges).await?;
				let resolver = &mut self.resolver.borrow_mut();
				match lines
				{
					Some(mut records) =>
					{
						let tid_id: T::Tid = resolver
							.lock()
							.await
							.find(tid)
							.ok_or(error::Error::BedFormat(self.name.clone()))?;

						records.retain(|rec| {
							*rec.tid() == tid_id && rec.start() < end && rec.end() > start
						});
						Ok(Some(records))
					}
					None => Ok(None),
				}
			}
			None => return Err(error::Error::TabixNotOpen(self.name.clone())),
		}
	}

	pub async fn read_lines_in_range(
		&mut self,
		region: Vec<Range<u64>>,
	) -> error::Result<Option<Vec<Record<T::Tid>>>>
	{
		let bytes = self.read_bytes_for_region(&region).await?;

		let records: Vec<Box<dyn BedLine<T::Tid>>> = match self.format
		{
			BedFormat::BED3 { .. } =>
			{
				parse_all_records(&bytes, || self.resolver.clone(), parse_bed3_record).await?
			}
			BedFormat::BED4 { .. } =>
			{
				parse_all_records(&bytes, || self.resolver.clone(), parse_bed4_record).await?
			}
			BedFormat::BED5 { .. } =>
			{
				parse_all_records(&bytes, || self.resolver.clone(), parse_bed5_record).await?
			}
			BedFormat::BED6 { .. } =>
			{
				parse_all_records(&bytes, || self.resolver.clone(), parse_bed6_record).await?
			}
			BedFormat::BED12 { .. } =>
			{
				parse_all_records(&bytes, || self.resolver.clone(), parse_bed12_record).await?
			}
			BedFormat::BEDMethyl { .. } =>
			{
				parse_all_records(&bytes, || self.resolver.clone(), parse_bedmethyl_record).await?
			}
		};

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
