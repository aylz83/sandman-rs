use std::sync::Arc;
use std::path::Path;

use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek, AsyncBufRead};

use pufferfish::prelude::*;

use crate::store::TidResolver;
use crate::bed::{Bed3Fields, Bed4Extra, Bed5Extra, Bed6Extra, Bed12Extra, BedMethylExtra};
use crate::bed::{BedSink, BedFieldsSink};
use crate::bed::oneshotreader::OneShotBlockReader;
use crate::bed::SourceId;
use crate::bed::BedKind;

#[cfg(feature = "interning")]
use {crate::store::TidStore};

use crate::bed::blocks::BgzfBlock;
use crate::bed::{detect_format, detect_format_from_reader};
use crate::bed::oneshotreader::ReaderOptions;

use crate::error;

#[cfg(not(feature = "interning"))]
pub async fn from_path<P>(
	path: P,
	source_id: impl Into<Option<SourceId>> + 'static,
	pool: Arc<pool::BgzfBlockPool>,
) -> error::Result<AutoOneShotBlockReader<File, ()>>
where
	P: AsRef<Path> + Copy,
{
	let format = detect_format(path).await?;

	let inner = match format
	{
		BedKind::Bed3 => InnerAutoOneShotBlockReader::Bed3(
			OneShotBlockReader::<File, (), Bed3Fields>::from_path(path, source_id, pool).await?,
		),
		BedKind::Bed4 => InnerAutoOneShotBlockReader::Bed4(
			OneShotBlockReader::<File, (), Bed4Extra>::from_path(path, source_id, pool).await?,
		),
		BedKind::Bed5 => InnerAutoOneShotBlockReader::Bed5(
			OneShotBlockReader::<File, (), Bed5Extra>::from_path(path, source_id, pool).await?,
		),
		BedKind::Bed6 => InnerAutoOneShotBlockReader::Bed6(
			OneShotBlockReader::<File, (), Bed6Extra>::from_path(path, source_id, pool).await?,
		),
		BedKind::Bed12 => InnerAutoOneShotBlockReader::Bed12(
			OneShotBlockReader::<File, (), Bed12Extra>::from_path(path, source_id, pool).await?,
		),
		BedKind::BedMethyl => InnerAutoOneShotBlockReader::BedMethyl(
			OneShotBlockReader::<File, (), BedMethylExtra>::from_path(path, source_id, pool)
				.await?,
		),
	};

	Ok(AutoOneShotBlockReader { inner })
}

#[cfg(not(feature = "interning"))]
pub async fn from_path_with_options<P>(
	path: P,
	source_id: impl Into<Option<SourceId>> + 'static,
	pool: Arc<pool::BgzfBlockPool>,
	options: ReaderOptions<()>,
) -> error::Result<AutoOneShotBlockReader<File, ()>>
where
	P: AsRef<Path> + Copy,
{
	let format = detect_format(path).await?;

	let inner = match format
	{
		BedKind::Bed3 => InnerAutoOneShotBlockReader::Bed3(
			OneShotBlockReader::<File, (), Bed3Fields>::from_path_with_options(
				path, source_id, pool, options,
			)
			.await?,
		),
		BedKind::Bed4 => InnerAutoOneShotBlockReader::Bed4(
			OneShotBlockReader::<File, (), Bed4Extra>::from_path_with_options(
				path, source_id, pool, options,
			)
			.await?,
		),
		BedKind::Bed5 => InnerAutoOneShotBlockReader::Bed5(
			OneShotBlockReader::<File, (), Bed5Extra>::from_path_with_options(
				path, source_id, pool, options,
			)
			.await?,
		),
		BedKind::Bed6 => InnerAutoOneShotBlockReader::Bed6(
			OneShotBlockReader::<File, (), Bed6Extra>::from_path_with_options(
				path, source_id, pool, options,
			)
			.await?,
		),
		BedKind::Bed12 => InnerAutoOneShotBlockReader::Bed12(
			OneShotBlockReader::<File, (), Bed12Extra>::from_path_with_options(
				path, source_id, pool, options,
			)
			.await?,
		),
		BedKind::BedMethyl => InnerAutoOneShotBlockReader::BedMethyl(
			OneShotBlockReader::<File, (), BedMethylExtra>::from_path_with_options(
				path, source_id, pool, options,
			)
			.await?,
		),
	};

	Ok(AutoOneShotBlockReader { inner })
}

#[cfg(not(feature = "interning"))]
pub async fn from_reader<R>(
	name: String,
	mut reader: R,
	source_id: impl Into<Option<SourceId>> + 'static,
	pool: Arc<pool::BgzfBlockPool>,
) -> error::Result<AutoOneShotBlockReader<R, ()>>
where
	R: AsyncRead
		+ AsyncSeek
		+ AsyncBufRead
		+ std::marker::Send
		+ std::marker::Unpin
		+ std::marker::Sync
		+ 'static,
{
	let format = detect_format_from_reader(name.clone(), &mut reader, 10).await?;

	let inner = match format
	{
		BedKind::Bed3 => InnerAutoOneShotBlockReader::Bed3(
			OneShotBlockReader::<_, (), Bed3Fields>::from_reader(name, reader, source_id, pool)
				.await,
		),
		BedKind::Bed4 => InnerAutoOneShotBlockReader::Bed4(
			OneShotBlockReader::<_, (), Bed4Extra>::from_reader(name, reader, source_id, pool)
				.await,
		),
		BedKind::Bed5 => InnerAutoOneShotBlockReader::Bed5(
			OneShotBlockReader::<_, (), Bed5Extra>::from_reader(name, reader, source_id, pool)
				.await,
		),
		BedKind::Bed6 => InnerAutoOneShotBlockReader::Bed6(
			OneShotBlockReader::<_, (), Bed6Extra>::from_reader(name, reader, source_id, pool)
				.await,
		),
		BedKind::Bed12 => InnerAutoOneShotBlockReader::Bed12(
			OneShotBlockReader::<_, (), Bed12Extra>::from_reader(name, reader, source_id, pool)
				.await,
		),
		BedKind::BedMethyl => InnerAutoOneShotBlockReader::BedMethyl(
			OneShotBlockReader::<_, (), BedMethylExtra>::from_reader(name, reader, source_id, pool)
				.await,
		),
	};

	Ok(AutoOneShotBlockReader { inner })
}

#[cfg(not(feature = "interning"))]
pub async fn from_reader_with_options<R>(
	name: String,
	mut reader: R,
	source_id: impl Into<Option<SourceId>> + 'static,
	pool: Arc<pool::BgzfBlockPool>,
	options: ReaderOptions<()>,
) -> error::Result<AutoOneShotBlockReader<R, ()>>
where
	R: AsyncRead
		+ AsyncSeek
		+ AsyncBufRead
		+ std::marker::Send
		+ std::marker::Unpin
		+ std::marker::Sync
		+ 'static,
{
	let format = detect_format_from_reader(name.clone(), &mut reader, 10).await?;

	let inner = match format
	{
		BedKind::Bed3 => InnerAutoOneShotBlockReader::Bed3(
			OneShotBlockReader::<_, (), Bed3Fields>::from_reader_with_options(
				name, reader, source_id, pool, options,
			)
			.await,
		),
		BedKind::Bed4 => InnerAutoOneShotBlockReader::Bed4(
			OneShotBlockReader::<_, (), Bed4Extra>::from_reader_with_options(
				name, reader, source_id, pool, options,
			)
			.await,
		),
		BedKind::Bed5 => InnerAutoOneShotBlockReader::Bed5(
			OneShotBlockReader::<_, (), Bed5Extra>::from_reader_with_options(
				name, reader, source_id, pool, options,
			)
			.await,
		),
		BedKind::Bed6 => InnerAutoOneShotBlockReader::Bed6(
			OneShotBlockReader::<_, (), Bed6Extra>::from_reader_with_options(
				name, reader, source_id, pool, options,
			)
			.await,
		),
		BedKind::Bed12 => InnerAutoOneShotBlockReader::Bed12(
			OneShotBlockReader::<_, (), Bed12Extra>::from_reader_with_options(
				name, reader, source_id, pool, options,
			)
			.await,
		),
		BedKind::BedMethyl => InnerAutoOneShotBlockReader::BedMethyl(
			OneShotBlockReader::<_, (), BedMethylExtra>::from_reader_with_options(
				name, reader, source_id, pool, options,
			)
			.await,
		),
	};

	Ok(AutoOneShotBlockReader { inner })
}

#[cfg(feature = "interning")]
pub async fn from_path<P>(
	path: P,
	source_id: impl Into<Option<SourceId>> + 'static,
	pool: Arc<pool::BgzfBlockPool>,
) -> error::Result<AutoOneShotBlockReader<File, TidStore>>
where
	P: AsRef<Path> + Copy,
{
	let format = detect_format(path).await?;

	println!("format = {:?}", format);

	let inner = match format
	{
		BedKind::Bed3 => InnerAutoOneShotBlockReader::Bed3(
			OneShotBlockReader::<File, TidStore, Bed3Fields>::from_path(path, source_id, pool)
				.await?,
		),
		BedKind::Bed4 => InnerAutoOneShotBlockReader::Bed4(
			OneShotBlockReader::<File, TidStore, Bed4Extra>::from_path(path, source_id, pool)
				.await?,
		),
		BedKind::Bed5 => InnerAutoOneShotBlockReader::Bed5(
			OneShotBlockReader::<File, TidStore, Bed5Extra>::from_path(path, source_id, pool)
				.await?,
		),
		BedKind::Bed6 => InnerAutoOneShotBlockReader::Bed6(
			OneShotBlockReader::<File, TidStore, Bed6Extra>::from_path(path, source_id, pool)
				.await?,
		),
		BedKind::Bed12 => InnerAutoOneShotBlockReader::Bed12(
			OneShotBlockReader::<File, TidStore, Bed12Extra>::from_path(path, source_id, pool)
				.await?,
		),
		BedKind::BedMethyl => InnerAutoOneShotBlockReader::BedMethyl(
			OneShotBlockReader::<File, TidStore, BedMethylExtra>::from_path(path, source_id, pool)
				.await?,
		),
	};

	Ok(AutoOneShotBlockReader { inner })
}

#[cfg(feature = "interning")]
pub async fn from_path_with_options<P>(
	path: P,
	source_id: impl Into<Option<SourceId>> + 'static,
	pool: Arc<pool::BgzfBlockPool>,
	options: ReaderOptions<TidStore>,
) -> error::Result<AutoOneShotBlockReader<File, TidStore>>
where
	P: AsRef<Path> + Copy,
{
	let format = detect_format(path).await?;

	let inner = match format
	{
		BedKind::Bed3 => InnerAutoOneShotBlockReader::Bed3(
			OneShotBlockReader::<File, TidStore, Bed3Fields>::from_path_with_options(
				path, source_id, pool, options,
			)
			.await?,
		),
		BedKind::Bed4 => InnerAutoOneShotBlockReader::Bed4(
			OneShotBlockReader::<File, TidStore, Bed4Extra>::from_path_with_options(
				path, source_id, pool, options,
			)
			.await?,
		),
		BedKind::Bed5 => InnerAutoOneShotBlockReader::Bed5(
			OneShotBlockReader::<File, TidStore, Bed5Extra>::from_path_with_options(
				path, source_id, pool, options,
			)
			.await?,
		),
		BedKind::Bed6 => InnerAutoOneShotBlockReader::Bed6(
			OneShotBlockReader::<File, TidStore, Bed6Extra>::from_path_with_options(
				path, source_id, pool, options,
			)
			.await?,
		),
		BedKind::Bed12 => InnerAutoOneShotBlockReader::Bed12(
			OneShotBlockReader::<File, TidStore, Bed12Extra>::from_path_with_options(
				path, source_id, pool, options,
			)
			.await?,
		),
		BedKind::BedMethyl => InnerAutoOneShotBlockReader::BedMethyl(
			OneShotBlockReader::<File, TidStore, BedMethylExtra>::from_path_with_options(
				path, source_id, pool, options,
			)
			.await?,
		),
	};

	Ok(AutoOneShotBlockReader { inner })
}

#[cfg(feature = "interning")]
pub async fn from_reader<R>(
	name: String,
	mut reader: R,
	source_id: impl Into<Option<SourceId>> + 'static,
	pool: Arc<pool::BgzfBlockPool>,
) -> error::Result<AutoOneShotBlockReader<R, TidStore>>
where
	R: AsyncRead
		+ AsyncSeek
		+ AsyncBufRead
		+ std::marker::Send
		+ std::marker::Unpin
		+ std::marker::Sync
		+ 'static,
{
	let format = detect_format_from_reader(name.clone(), &mut reader, 10).await?;

	let inner = match format
	{
		BedKind::Bed3 => InnerAutoOneShotBlockReader::Bed3(
			OneShotBlockReader::<_, TidStore, Bed3Fields>::from_reader(
				name, reader, source_id, pool,
			)
			.await?,
		),
		BedKind::Bed4 => InnerAutoOneShotBlockReader::Bed4(
			OneShotBlockReader::<_, TidStore, Bed4Extra>::from_reader(
				name, reader, source_id, pool,
			)
			.await?,
		),
		BedKind::Bed5 => InnerAutoOneShotBlockReader::Bed5(
			OneShotBlockReader::<_, TidStore, Bed5Extra>::from_reader(
				name, reader, source_id, pool,
			)
			.await?,
		),
		BedKind::Bed6 => InnerAutoOneShotBlockReader::Bed6(
			OneShotBlockReader::<_, TidStore, Bed6Extra>::from_reader(
				name, reader, source_id, pool,
			)
			.await?,
		),
		BedKind::Bed12 => InnerAutoOneShotBlockReader::Bed12(
			OneShotBlockReader::<_, TidStore, Bed12Extra>::from_reader(
				name, reader, source_id, pool,
			)
			.await?,
		),
		BedKind::BedMethyl => InnerAutoOneShotBlockReader::BedMethyl(
			OneShotBlockReader::<_, TidStore, BedMethylExtra>::from_reader(
				name, reader, source_id, pool,
			)
			.await?,
		),
	};

	Ok(AutoOneShotBlockReader { inner })
}

#[cfg(feature = "interning")]
pub async fn from_reader_with_options<R>(
	name: String,
	mut reader: R,
	source_id: impl Into<Option<SourceId>> + 'static,
	pool: Arc<pool::BgzfBlockPool>,
	options: ReaderOptions<TidStore>,
) -> error::Result<AutoOneShotBlockReader<R, TidStore>>
where
	R: AsyncRead
		+ AsyncSeek
		+ AsyncBufRead
		+ std::marker::Send
		+ std::marker::Unpin
		+ std::marker::Sync
		+ 'static,
{
	let format = detect_format_from_reader(name.clone(), &mut reader, 10).await?;

	let inner = match format
	{
		BedKind::Bed3 => InnerAutoOneShotBlockReader::Bed3(
			OneShotBlockReader::<_, TidStore, Bed3Fields>::from_reader_with_options(
				name, reader, source_id, pool, options,
			)
			.await?,
		),
		BedKind::Bed4 => InnerAutoOneShotBlockReader::Bed4(
			OneShotBlockReader::<_, TidStore, Bed4Extra>::from_reader_with_options(
				name, reader, source_id, pool, options,
			)
			.await?,
		),
		BedKind::Bed5 => InnerAutoOneShotBlockReader::Bed5(
			OneShotBlockReader::<_, TidStore, Bed5Extra>::from_reader_with_options(
				name, reader, source_id, pool, options,
			)
			.await?,
		),
		BedKind::Bed6 => InnerAutoOneShotBlockReader::Bed6(
			OneShotBlockReader::<_, TidStore, Bed6Extra>::from_reader_with_options(
				name, reader, source_id, pool, options,
			)
			.await?,
		),
		BedKind::Bed12 => InnerAutoOneShotBlockReader::Bed12(
			OneShotBlockReader::<_, TidStore, Bed12Extra>::from_reader_with_options(
				name, reader, source_id, pool, options,
			)
			.await?,
		),
		BedKind::BedMethyl => InnerAutoOneShotBlockReader::BedMethyl(
			OneShotBlockReader::<_, TidStore, BedMethylExtra>::from_reader_with_options(
				name, reader, source_id, pool, options,
			)
			.await?,
		),
	};

	Ok(AutoOneShotBlockReader { inner })
}

enum InnerAutoOneShotBlockReader<R, T>
where
	R: AsyncRead + AsyncSeek + Unpin + Send + Sync + 'static,
	T: TidResolver + Clone + std::fmt::Debug + Send + Sync + 'static,
{
	Bed3(OneShotBlockReader<R, T, Bed3Fields>),
	Bed4(OneShotBlockReader<R, T, Bed4Extra>),
	Bed5(OneShotBlockReader<R, T, Bed5Extra>),
	Bed6(OneShotBlockReader<R, T, Bed6Extra>),
	Bed12(OneShotBlockReader<R, T, Bed12Extra>),
	BedMethyl(OneShotBlockReader<R, T, BedMethylExtra>),
}

pub trait AutoOneShotBlockReaderTrait<T>
where
	T: TidResolver + Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn next_bgzf_blocks<'a>(
		&'a mut self,
		n: usize,
	) -> impl Future<Output = error::Result<Option<BgzfBlock>>> + 'a;

	fn read_tids_in_block_sink<'a, S>(
		&'a self,
		block: BgzfBlock,
		sink: &'a mut S,
	) -> impl Future<Output = error::Result<Option<usize>>> + 'a
	where
		S: BedSink<T::Tid> + ?Sized;

	fn name(&self) -> String;
}

pub struct AutoOneShotBlockReader<R, T>
where
	R: AsyncRead + AsyncSeek + Unpin + Send + Sync + 'static,
	T: TidResolver + Clone + std::fmt::Debug + Send + Sync + 'static,
{
	inner: InnerAutoOneShotBlockReader<R, T>,
}

impl<R, T> AutoOneShotBlockReaderTrait<T> for AutoOneShotBlockReader<R, T>
where
	R: AsyncRead + AsyncSeek + Unpin + Send + Sync + 'static,
	T: TidResolver + Clone + std::fmt::Debug + Send + Sync + 'static,
{
	fn name(&self) -> String
	{
		match &self.inner
		{
			InnerAutoOneShotBlockReader::Bed3(r) => r.name(),
			InnerAutoOneShotBlockReader::Bed4(r) => r.name(),
			InnerAutoOneShotBlockReader::Bed5(r) => r.name(),
			InnerAutoOneShotBlockReader::Bed6(r) => r.name(),
			InnerAutoOneShotBlockReader::Bed12(r) => r.name(),
			InnerAutoOneShotBlockReader::BedMethyl(r) => r.name(),
		}
	}

	async fn next_bgzf_blocks<'a>(&'a mut self, n: usize) -> error::Result<Option<BgzfBlock>>
	{
		match &mut self.inner
		{
			InnerAutoOneShotBlockReader::Bed3(r) => r.next_bgzf_blocks(n).await,
			InnerAutoOneShotBlockReader::Bed4(r) => r.next_bgzf_blocks(n).await,
			InnerAutoOneShotBlockReader::Bed5(r) => r.next_bgzf_blocks(n).await,
			InnerAutoOneShotBlockReader::Bed6(r) => r.next_bgzf_blocks(n).await,
			InnerAutoOneShotBlockReader::Bed12(r) => r.next_bgzf_blocks(n).await,
			InnerAutoOneShotBlockReader::BedMethyl(r) => r.next_bgzf_blocks(n).await,
		}
	}

	async fn read_tids_in_block_sink<'a, S>(
		&'a self,
		block: BgzfBlock,
		sink: &'a mut S,
	) -> error::Result<Option<usize>>
	where
		S: BedSink<T::Tid> + ?Sized,
	{
		match &self.inner
		{
			InnerAutoOneShotBlockReader::Bed3(r) => r.read_tids_in_block_sink(block, sink).await,
			InnerAutoOneShotBlockReader::Bed4(r) => r.read_tids_in_block_sink(block, sink).await,
			InnerAutoOneShotBlockReader::Bed5(r) => r.read_tids_in_block_sink(block, sink).await,
			InnerAutoOneShotBlockReader::Bed6(r) => r.read_tids_in_block_sink(block, sink).await,
			InnerAutoOneShotBlockReader::Bed12(r) => r.read_tids_in_block_sink(block, sink).await,
			InnerAutoOneShotBlockReader::BedMethyl(r) =>
			{
				r.read_tids_in_block_sink(block, sink).await
			}
		}
	}
}

// #[async_trait::async_trait(?Send)]
// pub trait AutoOneShotBlockReader<T>
// where
// 	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
// {
// 	fn next_bgzf_blocks<'a>(
// 		&'a mut self,
// 		n: usize,
// 	) -> impl Future<Output = error::Result<Option<BgzfBlock>>> + 'a;
// 	fn read_tids_in_block_sink<'a, S>(
// 		&'a self,
// 		block: BgzfBlock,
// 		sink: &'a mut S,
// 	) -> impl Future<Output = error::Result<()>> + 'a
// 	where
// 		S: BedSink<T::Tid> + ?Sized;

// 	fn name(&self) -> String;
// }

// #[async_trait::async_trait(?Send)]
impl<R, T, F> AutoOneShotBlockReaderTrait<T> for OneShotBlockReader<R, T, F>
where
	R: AsyncRead + AsyncSeek + Send + Unpin + Sync,
	T: TidResolver + Clone + std::fmt::Debug + Send + Sync + 'static,
	F: BedFieldsSink<T::Tid> + std::fmt::Debug,
{
	fn name(&self) -> String
	{
		self.name.clone()
	}

	async fn next_bgzf_blocks<'a>(&'a mut self, n: usize) -> error::Result<Option<BgzfBlock>>
	{
		OneShotBlockReader::next_bgzf_blocks(self, n).await
	}

	async fn read_tids_in_block_sink<'a, S>(
		&'a self,
		block: BgzfBlock,
		sink: &'a mut S,
	) -> error::Result<Option<usize>>
	where
		S: BedSink<T::Tid> + ?Sized,
	{
		OneShotBlockReader::read_tids_in_block_sink(self, block, sink).await
	}
}

// impl<R, T, F> AutoOneShotBlockReader<T> for OneShotBlockReader<R, T, F>
// where
// 	R: AsyncRead + AsyncSeek + Send + Unpin + Sync,
// 	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
// 	F: BedFieldsSink<T::Tid> + std::fmt::Debug,
// {
// 	fn name(&self) -> String
// 	{
// 		self.name.clone()
// 	}

// 	async fn next_bgzf_blocks(&mut self, n: usize) -> error::Result<Option<BgzfBlock>>
// 	{
// 		self.next_bgzf_blocks(n).await
// 	}

// 	async fn read_tids_in_block_sink<'a, S>(
// 		&self,
// 		block: BgzfBlock,
// 		sink: &'a mut S,
// 	) -> error::Result<()>
// 	where
// 		S: BedSink<T::Tid> + ?Sized,
// 	{
// 		self.read_tids_in_block_sink(block, sink).await
// 	}
// }
