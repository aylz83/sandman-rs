use tokio::io::{AsyncRead, AsyncSeek};
use tokio::sync::Mutex;
use tokio::fs::File;

use rayon::yield_now;

use std::marker::PhantomData;
use std::sync::Arc;
use std::path::Path;
use std::sync::atomic::Ordering;

use memchr::memchr;

use crate::error;
use crate::store::TidResolver;
use crate::bed::blocks::BgzfBlock;
use crate::bed::{BedSink, BedFieldsSink};
use crate::bed::{ReaderId, SourceId};
use crate::bed::NEXT_READER_ID;
use crate::bed::Strand;

use crate::filtering::ReadFilterContext;

use rayon::prelude::*;
use rayon::{ThreadPoolBuilder, ThreadPool};

use futures::stream::Buffered;
use futures::StreamExt;

#[cfg(feature = "interning")]
use crate::store::TidStore;

use pufferfish::prelude::*;

const DEFAULT_BUFFER_SIZE: usize = 200;

pub struct ReaderOptions<Interner>
{
	pub buffer_size: Option<usize>,
	pub interner: Option<Arc<Mutex<Interner>>>,
	pub read_filter: Option<Arc<Mutex<ReadFilterContext>>>,
	pub n_threads: Option<usize>,
}

impl<Interner> Default for ReaderOptions<Interner>
{
	fn default() -> Self
	{
		Self {
			buffer_size: Some(DEFAULT_BUFFER_SIZE),
			interner: None,
			read_filter: None,
			n_threads: None,
		}
	}
}

impl<Interner> ReaderOptions<Interner>
{
	pub fn with_buffer_size(mut self, buffer_size: usize) -> Self
	{
		self.buffer_size = Some(buffer_size);
		self
	}

	pub fn with_interner(mut self, interner: Arc<Mutex<Interner>>) -> Self
	{
		self.interner = Some(interner);
		self
	}

	pub fn with_read_filter(mut self, read_filter: Arc<Mutex<ReadFilterContext>>) -> Self
	{
		self.read_filter = Some(read_filter);
		self
	}

	pub fn with_n_threads(mut self, n: usize) -> Self
	{
		self.n_threads = Some(n);
		self
	}
}

pub struct OneShotBlockReader<R, T, F>
where
	R: AsyncRead + AsyncSeek + std::marker::Send + std::marker::Unpin + 'static,
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
	F: BedFieldsSink<T::Tid> + std::fmt::Debug,
{
	pub(crate) name: String,
	pub(crate) stream: Buffered<BgzfBlockStream<R>>,
	pub(crate) resolver: Arc<Mutex<T>>,
	pub(crate) filter_ctx: Option<Arc<Mutex<ReadFilterContext>>>,
	pub(crate) reader_id: ReaderId,
	pub(crate) source_id: Option<SourceId>,
	pub(crate) pending_tail: Option<Vec<u8>>,
	pub(crate) thread_pool: ThreadPool,

	_phantom: PhantomData<(R, F)>,
}

#[cfg(not(feature = "interning"))]
impl<F> OneShotBlockReader<File, (), F>
where
	F: BedFieldsSink<String> + std::fmt::Debug + 'static,
{
	pub async fn from_path<P>(
		path: P,
		source_id: impl Into<Option<SourceId>> + 'static,
		pool: Arc<pool::BgzfBlockPool>,
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

		let gzip_file = Self::open_bed_file(path).await?;
		Ok(Self::from_reader(name, gzip_file, source_id, pool).await)
	}

	pub async fn from_path_with_options<P>(
		path: P,
		source_id: impl Into<Option<SourceId>>,
		pool: Arc<pool::BgzfBlockPool>,
		options: ReaderOptions<()>,
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

		let gzip_file = Self::open_bed_file(path).await?;
		Ok(Self::from_reader_with_options(name, gzip_file, source_id, pool, options).await)
	}
}

#[cfg(feature = "interning")]
impl<F> OneShotBlockReader<File, TidStore, F>
where
	F: BedFieldsSink<<TidStore as TidResolver>::Tid> + std::fmt::Debug,
{
	pub async fn from_path<P>(
		path: P,
		source_id: impl Into<Option<SourceId>> + 'static,
		pool: Arc<pool::BgzfBlockPool>,
	) -> error::Result<Self>
	where
		P: AsRef<Path> + std::marker::Copy,
	{
		let name = path
			.as_ref()
			// .file_name()
			// .and_then(|s| s.to_str())
			// .unwrap_or("unknown")
			.to_string_lossy()
			.into_owned();

		let gzip_file = Self::open_bed_file(path).await?;
		let reader = Self::from_reader(name, gzip_file, source_id, pool).await?;

		Ok(reader)
	}

	pub async fn from_path_with_options<P>(
		path: P,
		source_id: impl Into<Option<SourceId>> + 'static,
		pool: Arc<pool::BgzfBlockPool>,
		options: ReaderOptions<TidStore>,
	) -> error::Result<Self>
	where
		P: AsRef<Path> + std::marker::Copy,
	{
		let name = path
			.as_ref()
			// .file_name()
			// .and_then(|s| s.to_str())
			// .unwrap_or("unknown")
			.to_string_lossy()
			.into_owned();

		let gzip_file = Self::open_bed_file(path).await?;
		let reader =
			Self::from_reader_with_options(name, gzip_file, source_id, pool, options).await?;

		Ok(reader)
	}
}

#[cfg(not(feature = "interning"))]
impl<R, F> OneShotBlockReader<R, (), F>
where
	R: AsyncRead + AsyncSeek + std::marker::Send + std::marker::Unpin + 'static,
	F: BedFieldsSink<String> + std::fmt::Debug,
{
	pub async fn from_reader(
		name: String,
		reader: R,
		source_id: impl Into<Option<SourceId>>,
		pool: Arc<pool::BgzfBlockPool>,
	) -> Self
	{
		let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::SeqCst);
		// let resolver = Arc::new(Mutex::new(TidStore::default()));

		let stream = BgzfBlockStream::new(reader, pool.clone(), Some(is_bgzf_eof))
			.buffered(DEFAULT_BUFFER_SIZE);

		let builder = ThreadPoolBuilder::new();
		let thread_pool = builder.build().expect("Unable to setup thread pool");

		OneShotBlockReader {
			name,
			stream,
			thread_pool,
			resolver: Arc::new(Mutex::new(())),
			filter_ctx: None,
			reader_id: ReaderId(reader_id),
			source_id: source_id.into(),
			pending_tail: None,
			_phantom: PhantomData,
		}
	}

	pub async fn from_reader_with_options(
		name: String,
		reader: R,
		source_id: impl Into<Option<SourceId>>,
		pool: Arc<pool::BgzfBlockPool>,
		options: ReaderOptions<()>,
	) -> Self
	{
		let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::SeqCst);
		// let resolver = Arc::new(Mutex::new(TidStore::default()));

		let stream = BgzfBlockStream::new(reader, pool.clone(), Some(is_bgzf_eof))
			.buffered(options.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE));

		let builder = ThreadPoolBuilder::new();
		let builder = if let Some(n) = options.n_threads
		{
			builder.num_threads(n)
		}
		else
		{
			builder
		};

		let thread_pool = builder.build().expect("Unable to setup thread pool");

		OneShotBlockReader {
			name,
			stream,
			thread_pool,
			resolver: Arc::new(Mutex::new(())),
			filter_ctx: options.read_filter.into(),
			reader_id: ReaderId(reader_id),
			source_id: source_id.into(),
			pending_tail: None,
			_phantom: PhantomData,
		}
	}
}

#[cfg(feature = "interning")]
impl<R, F> OneShotBlockReader<R, TidStore, F>
where
	R: AsyncRead + AsyncSeek + std::marker::Send + std::marker::Unpin + 'static,
	F: BedFieldsSink<<TidStore as TidResolver>::Tid> + std::fmt::Debug,
{
	pub async fn from_reader(
		name: String,
		reader: R,
		source_id: impl Into<Option<SourceId>> + 'static,
		pool: Arc<pool::BgzfBlockPool>,
	) -> error::Result<Self>
	{
		let stream = BgzfBlockStream::new(reader, pool.clone(), Some(is_bgzf_eof))
			.buffered(DEFAULT_BUFFER_SIZE);

		let resolver = Arc::new(Mutex::new(TidStore::default()));

		let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::SeqCst);

		let builder = ThreadPoolBuilder::new();
		let thread_pool = builder.build().expect("Unable to setup thread pool");

		Ok(Self {
			name,
			thread_pool,
			stream,
			resolver,
			filter_ctx: None,
			reader_id: ReaderId(reader_id),
			source_id: source_id.into(),
			pending_tail: None,
			_phantom: PhantomData,
		})
	}

	pub async fn from_reader_with_options(
		name: String,
		reader: R,
		source_id: impl Into<Option<SourceId>> + 'static,
		pool: Arc<pool::BgzfBlockPool>,
		options: ReaderOptions<TidStore>,
	) -> error::Result<Self>
	{
		let stream = BgzfBlockStream::new(reader, pool.clone(), Some(is_bgzf_eof))
			.buffered(options.buffer_size.unwrap_or(DEFAULT_BUFFER_SIZE));

		let resolver = options
			.interner
			.unwrap_or(Arc::new(Mutex::new(TidStore::default())));

		let reader_id = NEXT_READER_ID.fetch_add(1, Ordering::SeqCst);

		let builder = ThreadPoolBuilder::new();
		let builder = if let Some(n) = options.n_threads
		{
			builder.num_threads(n)
		}
		else
		{
			builder
		};

		let thread_pool = builder.build().expect("Unable to setup thread pool");

		Ok(Self {
			name,
			thread_pool,
			stream,
			resolver,
			filter_ctx: options.read_filter.into(),
			reader_id: ReaderId(reader_id),
			source_id: source_id.into(),
			pending_tail: None,
			_phantom: PhantomData,
		})
	}
}

impl<R, T, F> OneShotBlockReader<R, T, F>
where
	R: AsyncRead + AsyncSeek + std::marker::Send + std::marker::Unpin + 'static,
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
	F: BedFieldsSink<T::Tid> + std::fmt::Debug,
{
	async fn open_bed_file<P>(path: P) -> error::Result<File>
	where
		P: AsRef<Path> + Copy,
	{
		let path = path.as_ref();

		let gzip_file = File::open(path).await?;

		Ok(gzip_file)
	}

	pub async fn reset(&mut self) -> error::Result<()>
	{
		self.stream.get_mut().reset().await?;
		self.pending_tail = None;
		Ok(())
	}

	pub async fn store(&mut self) -> Arc<Mutex<T>>
	{
		self.resolver.clone()
	}

	pub async fn next_bgzf_blocks(&mut self, n: usize) -> error::Result<Option<BgzfBlock>>
	{
		let batch: Vec<_> = self.stream.by_ref().take(n).collect().await;

		let batch: Vec<_> = batch
			.into_iter()
			.filter_map(|res| match res
			{
				Ok(Some(block)) => Some(block), // keep the valid block
				_ => None,                      // skip empty blocks
			})
			.collect();

		if batch.is_empty()
		{
			return match self.pending_tail.take()
			{
				Some(tail) if !tail.is_empty() => Ok(Some(BgzfBlock {
					// block_offset,
					bytes: tail,
					filter_ctx: self.filter_ctx.clone(),
					source_id: self.source_id.clone(),
					reader_id: self.reader_id.clone(),
				})),
				_ => Ok(None),
			};
		}

		let decompressed: Vec<Vec<u8>> = self.thread_pool.install(|| {
			batch
				.into_par_iter()
				.map(|block| match decompress_bgzf_block(&block)
				{
					Ok(bytes) =>
					{
						yield_now();
						bytes
					}
					Err(e) =>
					{
						eprintln!("Decompression error: {:?}", e);
						Vec::new()
					}
				})
				.collect()
		});

		let total_size: usize = decompressed.iter().map(|b| b.len()).sum();

		let mut blocks = Vec::with_capacity(total_size);

		for bytes in decompressed
		{
			blocks.extend_from_slice(&bytes);
		}

		if let Some(tail) = self.pending_tail.as_mut()
		{
			if !tail.is_empty()
			{
				let mut merged = Vec::with_capacity(tail.len() + blocks.len());
				merged.extend_from_slice(tail);
				merged.extend_from_slice(&blocks);
				blocks = merged;
				tail.clear();
			}
		}

		if let Some(idx) = memchr::memrchr(b'\n', &blocks)
		{
			let remainder = blocks.split_off(idx + 1);
			*self.pending_tail.get_or_insert(Vec::new()) = remainder;
		}
		else
		{
			*self.pending_tail.get_or_insert(Vec::new()) = blocks;
			blocks = Vec::new();
		}

		Ok(Some(BgzfBlock {
			// block_offset,
			bytes: blocks,
			filter_ctx: self.filter_ctx.clone(),
			source_id: self.source_id.clone(),
			reader_id: self.reader_id.clone(),
		}))
	}

	pub async fn read_tids_in_block_sink<V>(
		&self,
		block: BgzfBlock,
		sink: &mut V,
	) -> error::Result<Option<usize>>
	where
		V: BedSink<T::Tid> + Send + Sync + ?Sized,
	{
		let mut cursor = &block.bytes as &[u8];
		let mut current_tid: Option<T::Tid> = None;
		let mut current_start: Option<u64> = None;
		let mut current_end: Option<u64> = None;
		let mut last_strand = Strand::Both;

		let mut filtered_out: Option<usize> = None;

		while !cursor.is_empty()
		{
			let (rest, parsed) = if let Some(filter_arc) = &self.filter_ctx
			{
				let locked = filter_arc.lock().await;
				let filter_ref: &ReadFilterContext = &*locked;
				F::parse_sink(cursor, Some(filter_ref)).await?
			}
			else
			{
				F::parse_sink(cursor, None).await?
			};

			if rest.len() == cursor.len()
			{
				let skip = memchr(b'\n', rest).map(|p| p + 1).unwrap_or(1);
				cursor = &rest[skip..];
				continue;
			}
			else
			{
				cursor = rest;
			}

			if let Some((tid, strand, start, end, value)) = parsed
			{
				last_strand = strand;

				let tid = self.resolver.lock().await.to_symbol_id(tid);

				if current_tid.as_ref() != Some(&tid)
				{
					if let Some(prev_end) = current_end
					{
						sink.end_position(prev_end);
					}

					if let Some(prev_tid) = current_tid
					{
						sink.end_tid(&prev_tid, &strand);
					}

					sink.begin_tid(&tid, &strand);

					current_tid = Some(tid);
					current_start = None;
					current_end = None;
				}

				if current_start != Some(start) || current_end != Some(end)
				{
					if let Some(prev_end) = current_end
					{
						sink.end_position(prev_end);
					}

					sink.begin_position(start);

					current_start = Some(start);
					current_end = Some(end);
				}

				sink.push_value(&self.source_id, &self.reader_id, value);
			}
			else
			{
				let value = filtered_out.get_or_insert_with(|| 0);
				*value += 1;
			}
		}

		if let Some(end) = current_end
		{
			sink.end_position(end);
		}

		if let Some(tid) = current_tid
		{
			sink.end_tid(&tid, &last_strand);
		}

		Ok(filtered_out)
	}
}
