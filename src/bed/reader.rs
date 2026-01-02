use std::path::Path;
use std::ops::Range;
use std::path::PathBuf;
// use std::borrow::BorrowMut;
use std::marker::PhantomData;

use nom::Finish;

use std::io::Cursor;

use tokio::fs::File;
use tokio::io::{SeekFrom, AsyncRead, AsyncSeek, AsyncBufReadExt, AsyncSeekExt, BufReader};
use std::sync::Arc;
use tokio::sync::Mutex;

use pufferfish::prelude::*;

use crate::error;
use crate::tabix;

#[cfg(feature = "bigbed")]
use crate::bed::{bigbedrecord::BigBedIndex, ParseContext, BedKind};
use crate::bed::{BedRecord, BrowserMeta, Track, BedFormat};
use crate::bed::parser::detect_format_from_reader;
use crate::bed::parser::BedFields;
use crate::bed::parser::parse_browser_line;
use crate::bed::parser::parse_track_line;
use crate::store::TidResolver;

#[cfg(feature = "interning")]
use crate::store::TidStore;

pub(crate) enum FileKind<R>
where
	R: AsyncRead + AsyncSeek + std::marker::Send + std::marker::Unpin,
{
	Plain(BufReader<R>),
	BGZF(BufReader<R>),
	#[cfg(feature = "bigbed")]
	ZLIB(BufReader<R>),
}

pub(crate) enum Index
{
	BGZF(tabix::Reader),
	#[cfg(feature = "bigbed")]
	ZLIB(BigBedIndex),
}

pub struct Reader<R, T, F>
where
	R: AsyncRead + AsyncSeek + std::marker::Send + std::marker::Unpin,
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
	F: BedFields<T, T::Tid> + std::fmt::Debug,
{
	pub(crate) name: String,
	pub(crate) reader: FileKind<R>,
	pub format: BedFormat,

	pub(crate) index: Option<Index>,
	pub(crate) bgz_buffer: String,
	pub(crate) track_line: Option<Track>,
	pub(crate) last_browser: Option<BrowserMeta>,
	pub(crate) reset_browser: bool,
	pub(crate) resolver: Arc<Mutex<T>>,
	_phantom: PhantomData<F>,
}

#[cfg(not(feature = "interning"))]
impl<F> Reader<File, (), F>
where
	F: BedFields<(), String> + std::fmt::Debug,
{
	pub async fn from_path<P>(path: P, tabix_path: impl Into<Option<P>>) -> error::Result<Self>
	where
		P: AsRef<Path> + std::marker::Copy,
	{
		let name = path
			.as_ref()
			.file_name()
			.and_then(|s| s.to_str())
			.unwrap_or("unknown")
			.to_string();

		let (gzip_file, tabix_file) = Self::open_bed_file(path, tabix_path.into()).await?;
		let reader = Self::from_reader(name, gzip_file, tabix_file).await?;

		Ok(reader)
	}
}

#[cfg(feature = "interning")]
impl<F> Reader<File, TidStore, F>
where
	F: BedFields<TidStore, <TidStore as TidResolver>::Tid> + std::fmt::Debug,
{
	pub async fn from_path<P>(path: P, tabix_path: impl Into<Option<P>>) -> error::Result<Self>
	where
		P: AsRef<Path> + std::marker::Copy,
	{
		let name = path
			.as_ref()
			.file_name()
			.and_then(|s| s.to_str())
			.unwrap_or("unknown")
			.to_string();

		let (gzip_file, tabix_file) = Self::open_bed_file(path, tabix_path.into()).await?;
		let reader = Self::from_reader(name, gzip_file, tabix_file).await?;

		Ok(reader)
	}

	pub async fn from_path_with_resolver<P>(
		path: P,
		tabix_path: impl Into<Option<P>>,
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

		let (gzip_file, tabix_file) = Self::open_bed_file(path, tabix_path.into()).await?;
		let reader = Self::from_reader_with_resolver(name, gzip_file, tabix_file, resolver).await?;

		Ok(reader)
	}
}

#[cfg(not(feature = "interning"))]
impl<R, F> Reader<R, (), F>
where
	R: AsyncRead + AsyncSeek + std::marker::Send + std::marker::Unpin,
	F: BedFields<(), String> + std::fmt::Debug,
{
	pub async fn from_reader(
		name: String,
		reader: R,
		tbi_index: impl Into<Option<R>>,
	) -> error::Result<Self>
	{
		let (format, reader, index) =
			Self::open_bed_reader(&name, reader, tbi_index.into()).await?;

		Ok(Reader {
			name,
			reader,
			format,
			index,
			bgz_buffer: String::new(),
			track_line: None,
			last_browser: None,
			reset_browser: true,
			resolver: Arc::new(Mutex::new(())),
			_phantom: PhantomData,
		})
	}
}

#[cfg(feature = "interning")]
impl<R, F> Reader<R, TidStore, F>
where
	R: AsyncRead + AsyncSeek + std::marker::Send + std::marker::Unpin,
	F: BedFields<TidStore, <TidStore as TidResolver>::Tid> + std::fmt::Debug,
{
	pub async fn from_reader(
		name: String,
		reader: R,
		tbi_reader: impl Into<Option<R>>,
	) -> error::Result<Self>
	{
		let (format, reader, index) =
			Self::open_bed_reader(&name, reader, tbi_reader.into()).await?;

		let resolver = Arc::new(Mutex::new(TidStore::default()));

		Ok(Reader {
			name,
			reader,
			format,
			index,
			bgz_buffer: String::new(),
			track_line: None,
			last_browser: None,
			reset_browser: true,
			resolver,
			_phantom: PhantomData,
		})
	}

	pub async fn from_reader_with_resolver(
		name: String,
		reader: R,
		tbi_reader: impl Into<Option<R>>,
		resolver: Arc<Mutex<TidStore>>,
	) -> error::Result<Self>
	{
		let (format, reader, index) =
			Self::open_bed_reader(&name, reader, tbi_reader.into()).await?;

		Ok(Reader {
			name,
			reader,
			format,
			index,
			bgz_buffer: String::new(),
			track_line: None,
			last_browser: None,
			reset_browser: true,
			resolver,
			_phantom: PhantomData,
		})
	}
}

impl<R, T, F> Reader<R, T, F>
where
	R: AsyncRead + AsyncSeek + std::marker::Send + std::marker::Unpin,
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
	F: BedFields<T, T::Tid> + std::fmt::Debug,
{
	#[cfg(not(feature = "bigbed"))]
	async fn read_kind(
		is_bgz: bool,
		_is_big_bed: bool,
		name: &String,
		mut reader: BufReader<R>,
		tbi_reader: Option<R>,
	) -> error::Result<(BedFormat, FileKind<R>, Option<Index>)>
	{
		if is_bgz
		{
			let block = reader
				.read_bgzf_block(Some(is_bgzf_eof))
				.await
				.map_err(|_| error::Error::BedFormat(name.clone()))?
				.ok_or(error::Error::BedFormat(name.clone()))?;
			reader.seek(SeekFrom::Start(0)).await?;

			let tbi_reader = match tbi_reader
			{
				Some(reader) => Some(Index::BGZF(tabix::Reader::from_reader(reader).await?)),
				None => None,
			};

			let mut buffer = BufReader::new(Cursor::new(&block));
			Ok((
				detect_format_from_reader(name.clone(), &mut buffer, 10).await?,
				FileKind::BGZF(reader),
				tbi_reader,
			))
		}
		else
		{
			Ok((
				detect_format_from_reader(name.clone(), &mut reader, 10).await?,
				FileKind::Plain(reader),
				None,
			))
		}
	}

	#[cfg(feature = "bigbed")]
	async fn read_kind(
		is_bgz: bool,
		is_bigbed: bool,
		name: &String,
		mut reader: BufReader<R>,
		tbi_reader: Option<R>,
	) -> error::Result<(BedFormat, FileKind<R>, Option<Index>)>
	{
		if is_bgz
		{
			let block = reader
				.read_bgzf_block(Some(is_bgzf_eof))
				.await
				.map_err(|_| error::Error::BedFormat(name.clone()))?
				.ok_or(error::Error::BedFormat(name.clone()))?;
			reader.seek(SeekFrom::Start(0)).await?;

			let tbi_reader = match tbi_reader
			{
				Some(reader) => Some(Index::BGZF(tabix::Reader::from_reader(reader).await?)),
				None => None,
			};

			let mut buffer = BufReader::new(Cursor::new(&block));
			Ok((
				detect_format_from_reader(name.clone(), &mut buffer, 10).await?,
				FileKind::BGZF(reader),
				tbi_reader,
			))
		}
		else if is_bigbed
		{
			let header = BigBedIndex::read_index(&mut reader).await?;
			Ok((
				BedFormat {
					kind: BedKind::BigBed,
					has_tracks: None,
					has_browsers: None,
				},
				FileKind::ZLIB(reader),
				Some(Index::ZLIB(header)),
			))
		}
		else
		{
			Ok((
				detect_format_from_reader(name.clone(), &mut reader, 10).await?,
				FileKind::Plain(reader),
				None,
			))
		}
	}

	async fn open_bed_reader(
		name: &String,
		reader: R,
		tbi_reader: Option<R>,
	) -> error::Result<(BedFormat, FileKind<R>, Option<Index>)>
	{
		let mut reader = BufReader::new(reader);

		reader.seek(SeekFrom::Start(0)).await?;

		let is_bgz = reader.is_bgz().await;
		reader.seek(SeekFrom::Start(0)).await?;

		#[cfg(feature = "bigbed")]
		let is_bigbed = reader.is_bigbed().await;
		#[cfg(not(feature = "bigbed"))]
		let is_bigbed = false;

		reader.seek(SeekFrom::Start(0)).await?;

		let (format, reader, index) =
			Self::read_kind(is_bgz, is_bigbed, name, reader, tbi_reader).await?;
		Ok((format, reader, index))
	}

	async fn open_bed_file<P>(path: P, tabix_path: Option<P>) -> error::Result<(File, Option<File>)>
	where
		P: AsRef<Path> + Copy,
	{
		let path = path.as_ref();

		let gzip_file = File::open(path).await?;

		let tabix_file = if let Some(tabix_path) = tabix_path
		{
			Some(File::open(tabix_path).await?)
		}
		else if path.extension().and_then(|ext| ext.to_str()) == Some("gz")
		{
			let mut tbi_path = PathBuf::from(path);
			tbi_path.set_extension("gz.tbi");

			if tokio::fs::metadata(&tbi_path).await.is_ok()
			{
				Some(File::open(&tbi_path).await?)
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

	pub(crate) async fn read_bgzf_line(&mut self) -> error::Result<Option<String>>
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
				FileKind::BGZF(reader) => reader.read_bgzf_block(Some(is_bgzf_eof)).await?,
				_ => None,
			})
			else
			{
				// No more blocks and buffer empty
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

	pub async fn blank_record(&self) -> BedRecord<T, T::Tid, F>
	{
		BedFields::<T, T::Tid>::empty(self.resolver.clone()).await
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
			#[cfg(feature = "bigbed")]
			FileKind::ZLIB(reader) =>
			{
				reader.seek(SeekFrom::Start(0)).await?;
			}
		}

		Ok(())
	}

	pub async fn read_line(
		&mut self,
	) -> error::Result<Option<(&Option<Track>, BedRecord<T, T::Tid, F>)>>
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

			let mut record = self.blank_record().await;
			self.parse_record_into(line_bytes, &mut record).await?;
			return Ok(Some((&self.track_line, record)));
		}

		Ok(None)
	}

	pub async fn read_line_with_meta(
		&mut self,
		browser_meta: &mut Option<BrowserMeta>,
	) -> error::Result<Option<(&Option<Track>, BedRecord<T, T::Tid, F>)>>
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

			let mut record = self.blank_record().await;
			self.parse_record_into(line_bytes, &mut record).await?;
			*browser_meta = self.last_browser.clone();

			return Ok(Some((&self.track_line, record)));
		}

		Ok(None)
	}

	pub(crate) async fn read_line_io(&mut self) -> error::Result<Option<String>>
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
				#[cfg(feature = "bigbed")]
				FileKind::ZLIB(_) =>
				{
					return Err(error::Error::ReadLineNotSupported(
						self.format.kind.to_string(),
					))
				}
			}

			if line.trim().is_empty()
			{
				continue;
			}

			return Ok(Some(line));
		}
	}

	pub(crate) async fn parse_record_into<'a>(
		&self,
		input: &'a [u8],
		record: &mut BedRecord<T, T::Tid, F>,
	) -> error::Result<&'a [u8]>
	{
		match &self.reader
		{
			FileKind::Plain(_) => F::parse_into(self.resolver.clone(), input, None, record).await,
			FileKind::BGZF(_) => F::parse_into(self.resolver.clone(), input, None, record).await,
			#[cfg(feature = "bigbed")]
			FileKind::ZLIB(_) =>
			{
				let Some(Index::ZLIB(index_reader)) = self.index.as_ref()
				else
				{
					return Err(error::Error::NotBigBed);
				};

				F::parse_into(
					self.resolver.clone(),
					input,
					Some(ParseContext::BigBed(index_reader)),
					record,
				)
				.await
			}
		}
	}

	pub(crate) async fn read_bgzf_bytes_for_region(
		reader: &mut BufReader<R>,
		region: &[Range<u64>],
	) -> error::Result<Vec<u8>>
	{
		// Rough capacity estimate
		let estimated_size: usize = region.iter().map(|r| (r.end - r.start) as usize).sum();

		let mut buffer = Vec::with_capacity(estimated_size);

		let mut current_block: Option<u64> = None;

		for offset in region
		{
			if offset.start >= offset.end
			{
				continue;
			}

			let (block_start, uncompressed_start) = (offset.start >> 16, offset.start & 0xFFFF);
			let (block_end, uncompressed_end) = (offset.end >> 16, offset.end & 0xFFFF);

			for block in block_start..=block_end
			{
				if current_block != Some(block)
				{
					reader.seek(SeekFrom::Start(block)).await?;
					current_block = Some(block);
				}

				let start = if block == block_start
				{
					uncompressed_start
				}
				else
				{
					0
				};

				let end = if block == block_end
				{
					uncompressed_end
				}
				else
				{
					u64::MAX
				};

				Self::read_bgzf_subblock(reader, start, end, &mut buffer).await?;
			}
		}

		Ok(buffer)
	}

	#[cfg(feature = "bigbed")]
	pub(crate) async fn read_zlib_bytes_for_region(
		reader: &mut BufReader<R>,
		region: Vec<(u64, u32)>,
	) -> error::Result<Vec<u8>>
	{
		let mut bytes = Vec::new();

		for (offset, compressed_size) in region
		{
			reader.seek(SeekFrom::Start(offset)).await?;
			let decompressed = reader.read_zlib_block(compressed_size as usize).await?;
			bytes.extend_from_slice(&decompressed);
		}

		Ok(bytes)
	}

	pub async fn read_lines_in_tid(
		&mut self,
		tid: &str,
		out: &mut Vec<BedRecord<T, T::Tid, F>>,
	) -> error::Result<()>
	{
		match &mut self.reader
		{
			FileKind::Plain(_) =>
			{
				return Err(error::Error::PlainBedRegion(self.name.clone()));
			}
			FileKind::BGZF(reader) =>
			{
				let Some(Index::BGZF(index_reader)) = self.index.as_ref()
				else
				{
					return Err(error::Error::TabixNotOpen(self.name.clone()));
				};

				let Some(ranges) = index_reader.offsets_for_tid(tid)?
				else
				{
					out.clear();
					return Ok(());
				};

				let bytes = Self::read_bgzf_bytes_for_region(reader, &ranges).await?;
				if bytes.is_empty()
				{
					return Ok(());
				}

				self.parse_records_from_bytes(&bytes as &[u8], out).await
			}
			#[cfg(feature = "bigbed")]
			FileKind::ZLIB(reader) =>
			{
				let Some(Index::ZLIB(index_reader)) = self.index.as_ref()
				else
				{
					return Err(error::Error::NotBigBed);
				};

				let ranges = index_reader.offsets_for_tid(reader, tid).await?;
				// println!("ranges = {:?}", ranges);

				let bytes = Self::read_zlib_bytes_for_region(reader, ranges).await?;
				if bytes.is_empty()
				{
					return Ok(());
				}

				self.parse_records_from_bytes(&bytes as &[u8], out).await
			}
		}
	}

	pub async fn read_lines_in_tid_region(
		&mut self,
		tid: &str,
		start: u64,
		end: u64,
		out: &mut Vec<BedRecord<T, T::Tid, F>>,
	) -> error::Result<()>
	{
		match &mut self.reader
		{
			FileKind::Plain(_) => return Err(error::Error::PlainBedRegion(self.name.clone())),
			FileKind::BGZF(reader) =>
			{
				let Some(Index::BGZF(index_reader)) = self.index.as_ref()
				else
				{
					return Err(error::Error::TabixNotOpen(self.name.clone()));
				};

				let Some(ranges) = index_reader.offsets_for_tid_region(tid, start, end)?
				else
				{
					out.clear();
					return Ok(());
				};

				let bytes = Self::read_bgzf_bytes_for_region(reader, &ranges).await?;
				if bytes.is_empty()
				{
					return Ok(());
				}

				self.parse_records_from_bytes(&bytes as &[u8], out).await?
			}
			#[cfg(feature = "bigbed")]
			FileKind::ZLIB(reader) =>
			{
				let Some(Index::ZLIB(index_reader)) = self.index.as_ref()
				else
				{
					return Err(error::Error::NotBigBed);
				};

				let ranges = index_reader
					.offsets_for_tid_region(reader, tid, start, end)
					.await?;

				let bytes = Self::read_zlib_bytes_for_region(reader, ranges).await?;
				if bytes.is_empty()
				{
					return Ok(());
				}

				self.parse_records_from_bytes(&bytes as &[u8], out).await?
			}
		};

		let tid_id = {
			let r = self.resolver.lock().await;
			r.find(tid)
				.ok_or(error::Error::BedFormat(self.name.clone()))?
		};

		out.retain(|rec| rec.tid == tid_id && rec.start < end && rec.end > start);

		Ok(())
	}

	pub async fn parse_records_from_bytes(
		&mut self,
		bytes: &[u8],
		out: &mut Vec<BedRecord<T, T::Tid, F>>,
	) -> error::Result<()>
	{
		out.clear();
		let mut cursor = &bytes as &[u8];

		while !cursor.is_empty()
		{
			out.push(self.blank_record().await);
			let last = out.last_mut().unwrap();
			cursor = self.parse_record_into(cursor, last).await?;
		}

		Ok(())
	}

	async fn read_bgzf_subblock(
		reader: &mut BufReader<R>,
		start: u64,
		end: u64,
		bgzf_block: &mut Vec<u8>,
	) -> error::Result<()>
	{
		if let Ok(Some(block)) = reader.read_bgzf_block(Some(is_bgzf_eof)).await
		{
			let block_len = block.len() as u64;

			let start_offset = start.min(block_len);
			let end_offset = end.min(block_len);

			if start_offset < end_offset
			{
				bgzf_block.extend_from_slice(&block[start_offset as usize..end_offset as usize]);
			}
		}

		Ok(())
	}
}
