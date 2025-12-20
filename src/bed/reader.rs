use std::path::Path;
use std::ops::Range;
use std::path::PathBuf;
// use std::borrow::BorrowMut;
use std::marker::PhantomData;

use nom::Finish;

use std::io::Cursor;

use tokio::io::SeekFrom;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncRead, AsyncSeek, AsyncBufReadExt, AsyncSeekExt, BufReader as TokioBufReader};
use std::sync::Arc;
use tokio::sync::Mutex;

use pufferfish::BGZ;

use crate::error;
use crate::tabix;
use crate::bed::*;

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
	Plain(TokioBufReader<R>),
	BGZF(TokioBufReader<R>),
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

	pub(crate) tbi_reader: Option<tabix::Reader>,
	pub(crate) bgz_buffer: String,
	pub(crate) track_line: Option<Track>,
	pub(crate) last_browser: Option<BrowserMeta>,
	pub(crate) reset_browser: bool,
	pub(crate) resolver: Arc<Mutex<T>>,
	_phantom: PhantomData<F>,
}

#[cfg(not(feature = "interning"))]
impl<F> Reader<TokioFile, (), F>
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
impl<F> Reader<TokioFile, TidStore, F>
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
		tbi_reader: impl Into<Option<R>>,
	) -> error::Result<Self>
	{
		let (format, reader, tbi_reader) =
			Self::open_bed_reader(&name, reader, tbi_reader.into()).await?;

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
		let (format, reader, tbi_reader) =
			Self::open_bed_reader(&name, reader, tbi_reader.into()).await?;

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
		let (format, reader, tbi_reader) =
			Self::open_bed_reader(&name, reader, tbi_reader.into()).await?;

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
	async fn open_bed_reader(
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
			detect_format_from_reader(name.clone(), &mut buffer, 10).await?
		}
		else
		{
			detect_format_from_reader(name.clone(), &mut reader, 10).await?
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

	async fn open_bed_file<P>(
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

	// pub async fn blank_record(&self) -> Record<T::Tid>
	// {
	// 	match self.format.kind
	// 	{
	// 		BedKind::Bed3 =>
	// 		{
	// 			Box::new(BedRecord::<_, _, Bed3Fields>::empty(self.resolver.clone()).await)
	// 		}
	// 		BedKind::Bed4 =>
	// 		{
	// 			Box::new(BedRecord::<_, _, Bed4Extra>::empty(self.resolver.clone()).await)
	// 		}
	// 		BedKind::Bed5 =>
	// 		{
	// 			Box::new(BedRecord::<_, _, Bed5Extra>::empty(self.resolver.clone()).await)
	// 		}
	// 		BedKind::Bed6 =>
	// 		{
	// 			Box::new(BedRecord::<_, _, Bed6Extra>::empty(self.resolver.clone()).await)
	// 		}
	// 		BedKind::Bed12 =>
	// 		{
	// 			Box::new(BedRecord::<_, _, Bed12Extra>::empty(self.resolver.clone()).await)
	// 		}
	// 		BedKind::BedMethyl =>
	// 		{
	// 			Box::new(BedRecord::<_, _, BedMethylExtra>::empty(self.resolver.clone()).await)
	// 		}
	// 	}
	// }

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
		F::parse_into(self.resolver.clone(), input, record).await
	}

	pub(crate) async fn read_bytes_for_region(
		&mut self,
		region: &[Range<u64>],
	) -> error::Result<Vec<u8>>
	{
		let reader = match &mut self.reader
		{
			FileKind::Plain(_) =>
			{
				return Err(error::Error::PlainBedRegion(self.name.clone()));
			}
			FileKind::BGZF(r) => r,
		};

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

				Self::read_subblock(reader, start, end, &mut buffer).await?;
			}
		}

		// match &mut self.reader
		// {
		// 	FileKind::Plain(_) =>
		// 	{
		// 		return Err(error::Error::PlainBedRegion(self.name.clone()));
		// 	}
		// 	FileKind::BGZF(reader) =>
		// 	{
		// 		for offset in region
		// 		{
		// 			let (block_start, uncompressed_start) =
		// 				(offset.start >> 16, offset.start & 0xFFFF);
		// 			let (block_end, uncompressed_end) = (offset.end >> 16, offset.end & 0xFFFF);

		// 			if offset.start == offset.end
		// 			{
		// 				break;
		// 			}

		// 			reader.seek(SeekFrom::Start(block_start)).await?;

		// 			if block_start == block_end
		// 			{
		// 				Self::read_subblock(
		// 					reader,
		// 					uncompressed_start,
		// 					uncompressed_end,
		// 					&mut buffer,
		// 				)
		// 				.await?;
		// 			}
		// 			else
		// 			{
		// 				let mut current_position = block_start;
		// 				while current_position <= block_end
		// 				{
		// 					let start_offset = if current_position == block_start
		// 					{
		// 						uncompressed_start
		// 					}
		// 					else
		// 					{
		// 						0
		// 					};
		// 					let end_offset = if current_position == block_end
		// 					{
		// 						uncompressed_end
		// 					}
		// 					else
		// 					{
		// 						u64::MAX
		// 					};
		// 					Self::read_subblock(reader, start_offset, end_offset, &mut buffer)
		// 						.await?;
		// 					current_position = reader.seek(SeekFrom::Current(0)).await?;
		// 				}
		// 			}
		// 		}
		// 	}
		// }

		Ok(buffer)
	}

	pub async fn read_lines_in_tid(
		&mut self,
		tid: &str,
		out: &mut Vec<BedRecord<T, T::Tid, F>>,
	) -> error::Result<()>
	{
		if matches!(self.reader, FileKind::Plain(_))
		{
			return Err(error::Error::PlainBedRegion(self.name.clone()));
		}

		let reader = self
			.tbi_reader
			.as_ref()
			.ok_or(error::Error::TabixNotOpen(self.name.clone()))?;

		let Some(ranges) = reader.offsets_for_tid(tid)?
		else
		{
			out.clear();
			return Ok(());
		};

		self.read_lines_in_range(ranges, out).await
	}

	pub async fn read_lines_in_tid_region(
		&mut self,
		tid: &str,
		start: u64,
		end: u64,
		out: &mut Vec<BedRecord<T, T::Tid, F>>,
	) -> error::Result<()>
	{
		if matches!(self.reader, FileKind::Plain(_))
		{
			return Err(error::Error::PlainBedRegion(self.name.clone()));
		}

		let reader = self
			.tbi_reader
			.as_ref()
			.ok_or(error::Error::TabixNotOpen(self.name.clone()))?;

		let Some(ranges) = reader.offsets_for_tid_region(tid, start, end)?
		else
		{
			out.clear();
			return Ok(());
		};

		self.read_lines_in_range(ranges, out).await?;

		let tid_id = {
			let r = self.resolver.lock().await;
			r.find(tid)
				.ok_or(error::Error::BedFormat(self.name.clone()))?
		};

		out.retain(|rec| rec.tid == tid_id && rec.start < end && rec.end > start);

		Ok(())
	}

	pub async fn read_lines_in_range(
		&mut self,
		region: Vec<Range<u64>>,
		out: &mut Vec<BedRecord<T, T::Tid, F>>,
	) -> error::Result<()>
	{
		let bytes = self.read_bytes_for_region(&region).await?;
		if bytes.is_empty()
		{
			return Ok(());
		}

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
				bgzf_block.extend_from_slice(&block[start as usize..end as usize]);
			}
		}

		Ok(())
	}

	// async fn read_subblock(
	// 	reader: &mut TokioBufReader<R>,
	// 	start: u64,
	// 	end: u64,
	// 	bgzf_block: &mut Vec<u8>,
	// ) -> error::Result<()>
	// {
	// 	if let Ok(Some(block)) = reader.read_bgzf_block(Some(pufferfish::is_bgzf_eof)).await
	// 	{
	// 		let block_len = block.len() as u64;
	// 		let start_offset = start.min(block_len);
	// 		let end_offset = end.min(block_len);

	// 		if start_offset < end_offset
	// 		{
	// 			let mut bytes = vec![0u8; (end_offset - start_offset) as usize];
	// 			let cursor = Cursor::new(&block);
	// 			let mut buffer = BufReader::new(cursor);
	// 			buffer.seek(SeekFrom::Start(start_offset))?;
	// 			buffer.read_exact(&mut bytes)?;
	// 			bgzf_block.extend_from_slice(&bytes);
	// 		}
	// 	}

	// 	Ok(())
	// }
}
