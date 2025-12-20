use crate::error;
use crate::AsyncReadSeek;
use crate::bed::{FileKind, BedKind};
use crate::bed::{BedRecord, AnyBedRecord, AutoBedRecord};
use crate::bed::{BedFields, Bed3Fields, Bed4Extra, Bed5Extra, Bed6Extra, Bed12Extra, BedMethylExtra};
use crate::bed::Track;
use crate::bed::Reader;
use crate::bed::BrowserMeta;
use crate::bed::IntoAnyBedRecord;
use crate::bed::detect_format;
use crate::bed::detect_format_from_reader;

use crate::store::TidResolver;

use tokio::io::AsyncBufRead;
use tokio::fs::File as TokioFile;

#[cfg(feature = "interning")]
use {crate::store::TidStore, tokio::sync::Mutex, std::sync::Arc};

use std::path::Path;

#[cfg(not(feature = "interning"))]
pub async fn from_path<P>(
	path: P,
	tabix_path: impl Into<Option<P>>,
) -> error::Result<Box<dyn AutoReader<()>>>
where
	P: AsRef<Path> + Copy,
{
	let format = detect_format(path).await?;

	Ok(match format.kind
	{
		BedKind::Bed3 =>
		{
			Box::new(Reader::<TokioFile, (), Bed3Fields>::from_path(path, tabix_path).await?)
		}
		BedKind::Bed4 =>
		{
			Box::new(Reader::<TokioFile, (), Bed4Extra>::from_path(path, tabix_path).await?)
		}
		BedKind::Bed5 =>
		{
			Box::new(Reader::<TokioFile, (), Bed5Extra>::from_path(path, tabix_path).await?)
		}
		BedKind::Bed6 =>
		{
			Box::new(Reader::<TokioFile, (), Bed6Extra>::from_path(path, tabix_path).await?)
		}
		BedKind::Bed12 =>
		{
			Box::new(Reader::<TokioFile, (), Bed12Extra>::from_path(path, tabix_path).await?)
		}
		BedKind::BedMethyl =>
		{
			Box::new(Reader::<TokioFile, (), BedMethylExtra>::from_path(path, tabix_path).await?)
		}
	})
}

#[cfg(not(feature = "interning"))]
pub async fn from_reader<R>(
	name: String,
	mut reader: R,
	tbi_reader: impl Into<Option<R>>,
) -> error::Result<Box<dyn AutoReader<()>>>
where
	R: AsyncReadSeek
		+ AsyncBufRead
		+ std::marker::Send
		+ std::marker::Unpin
		+ std::marker::Sync
		+ 'static,
{
	let format = detect_format_from_reader(name.clone(), &mut reader, 10).await?;

	Ok(match format.kind
	{
		BedKind::Bed3 =>
		{
			Box::new(Reader::<_, (), Bed3Fields>::from_reader(name, reader, tbi_reader).await?)
		}
		BedKind::Bed4 =>
		{
			Box::new(Reader::<_, (), Bed4Extra>::from_reader(name, reader, tbi_reader).await?)
		}
		BedKind::Bed5 =>
		{
			Box::new(Reader::<_, (), Bed5Extra>::from_reader(name, reader, tbi_reader).await?)
		}
		BedKind::Bed6 =>
		{
			Box::new(Reader::<_, (), Bed6Extra>::from_reader(name, reader, tbi_reader).await?)
		}
		BedKind::Bed12 =>
		{
			Box::new(Reader::<_, (), Bed12Extra>::from_reader(name, reader, tbi_reader).await?)
		}
		BedKind::BedMethyl =>
		{
			Box::new(Reader::<_, (), BedMethylExtra>::from_reader(name, reader, tbi_reader).await?)
		}
	})
}

#[cfg(feature = "interning")]
pub async fn from_path<P>(
	path: P,
	tabix_path: impl Into<Option<P>>,
) -> error::Result<Box<dyn AutoReader<TidStore>>>
where
	P: AsRef<Path> + Copy,
{
	let format = detect_format(path).await?;

	println!("{:?}", format);
	Ok(match format.kind
	{
		BedKind::Bed3 =>
		{
			Box::new(Reader::<TokioFile, TidStore, Bed3Fields>::from_path(path, tabix_path).await?)
		}
		BedKind::Bed4 =>
		{
			Box::new(Reader::<TokioFile, TidStore, Bed4Extra>::from_path(path, tabix_path).await?)
		}
		BedKind::Bed5 =>
		{
			Box::new(Reader::<TokioFile, TidStore, Bed5Extra>::from_path(path, tabix_path).await?)
		}
		BedKind::Bed6 =>
		{
			Box::new(Reader::<TokioFile, TidStore, Bed6Extra>::from_path(path, tabix_path).await?)
		}
		BedKind::Bed12 =>
		{
			Box::new(Reader::<TokioFile, TidStore, Bed12Extra>::from_path(path, tabix_path).await?)
		}
		BedKind::BedMethyl => Box::new(
			Reader::<TokioFile, TidStore, BedMethylExtra>::from_path(path, tabix_path).await?,
		),
	})
}

#[cfg(feature = "interning")]
pub async fn from_path_with_resolver<P>(
	path: P,
	tabix_path: impl Into<Option<P>>,
	resolver: Arc<Mutex<TidStore>>,
) -> error::Result<Box<dyn AutoReader<TidStore>>>
where
	P: AsRef<Path> + Copy,
{
	let format = detect_format(path).await?;

	Ok(match format.kind
	{
		BedKind::Bed3 => Box::new(
			Reader::<TokioFile, TidStore, Bed3Fields>::from_path_with_resolver(
				path, tabix_path, resolver,
			)
			.await?,
		),
		BedKind::Bed4 => Box::new(
			Reader::<TokioFile, TidStore, Bed4Extra>::from_path_with_resolver(
				path, tabix_path, resolver,
			)
			.await?,
		),
		BedKind::Bed5 => Box::new(
			Reader::<TokioFile, TidStore, Bed5Extra>::from_path_with_resolver(
				path, tabix_path, resolver,
			)
			.await?,
		),
		BedKind::Bed6 => Box::new(
			Reader::<TokioFile, TidStore, Bed6Extra>::from_path_with_resolver(
				path, tabix_path, resolver,
			)
			.await?,
		),
		BedKind::Bed12 => Box::new(
			Reader::<TokioFile, TidStore, Bed12Extra>::from_path_with_resolver(
				path, tabix_path, resolver,
			)
			.await?,
		),
		BedKind::BedMethyl => Box::new(
			Reader::<TokioFile, TidStore, BedMethylExtra>::from_path_with_resolver(
				path, tabix_path, resolver,
			)
			.await?,
		),
	})
}

#[cfg(feature = "interning")]
pub async fn from_reader<R>(
	name: String,
	mut reader: R,
	tbi_reader: impl Into<Option<R>>,
) -> error::Result<Box<dyn AutoReader<TidStore>>>
where
	R: AsyncReadSeek
		+ AsyncBufRead
		+ std::marker::Send
		+ std::marker::Unpin
		+ std::marker::Sync
		+ 'static,
{
	let format = detect_format_from_reader(name.clone(), &mut reader, 10).await?;

	Ok(match format.kind
	{
		BedKind::Bed3 => Box::new(
			Reader::<_, TidStore, Bed3Fields>::from_reader(name, reader, tbi_reader).await?,
		),
		BedKind::Bed4 =>
		{
			Box::new(Reader::<_, TidStore, Bed4Extra>::from_reader(name, reader, tbi_reader).await?)
		}
		BedKind::Bed5 =>
		{
			Box::new(Reader::<_, TidStore, Bed5Extra>::from_reader(name, reader, tbi_reader).await?)
		}
		BedKind::Bed6 =>
		{
			Box::new(Reader::<_, TidStore, Bed6Extra>::from_reader(name, reader, tbi_reader).await?)
		}
		BedKind::Bed12 => Box::new(
			Reader::<_, TidStore, Bed12Extra>::from_reader(name, reader, tbi_reader).await?,
		),
		BedKind::BedMethyl => Box::new(
			Reader::<_, TidStore, BedMethylExtra>::from_reader(name, reader, tbi_reader).await?,
		),
	})
}

#[cfg(feature = "interning")]
pub async fn from_reader_with_resolver<R>(
	name: String,
	mut reader: R,
	tbi_reader: impl Into<Option<R>>,
	resolver: Arc<Mutex<TidStore>>,
) -> error::Result<Box<dyn AutoReader<TidStore>>>
where
	R: AsyncReadSeek
		+ AsyncBufRead
		+ std::marker::Send
		+ std::marker::Unpin
		+ std::marker::Sync
		+ 'static,
{
	let format = detect_format_from_reader(name.clone(), &mut reader, 10).await?;

	Ok(match format.kind
	{
		BedKind::Bed3 => Box::new(
			Reader::<_, TidStore, Bed3Fields>::from_reader_with_resolver(
				name, reader, tbi_reader, resolver,
			)
			.await?,
		),
		BedKind::Bed4 => Box::new(
			Reader::<_, TidStore, Bed4Extra>::from_reader_with_resolver(
				name, reader, tbi_reader, resolver,
			)
			.await?,
		),
		BedKind::Bed5 => Box::new(
			Reader::<_, TidStore, Bed5Extra>::from_reader_with_resolver(
				name, reader, tbi_reader, resolver,
			)
			.await?,
		),
		BedKind::Bed6 => Box::new(
			Reader::<_, TidStore, Bed6Extra>::from_reader_with_resolver(
				name, reader, tbi_reader, resolver,
			)
			.await?,
		),
		BedKind::Bed12 => Box::new(
			Reader::<_, TidStore, Bed12Extra>::from_reader_with_resolver(
				name, reader, tbi_reader, resolver,
			)
			.await?,
		),
		BedKind::BedMethyl => Box::new(
			Reader::<_, TidStore, BedMethylExtra>::from_reader_with_resolver(
				name, reader, tbi_reader, resolver,
			)
			.await?,
		),
	})
}

#[async_trait::async_trait]
pub trait AutoReader<T>
where
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
{
	async fn read_line(&mut self) -> error::Result<Option<(&Option<Track>, AnyBedRecord<T>)>>;
	async fn read_line_with_meta(
		&mut self,
		browser_meta: &mut Option<BrowserMeta>,
	) -> error::Result<Option<(&Option<Track>, AnyBedRecord<T>)>>;
	async fn read_lines_in_tid_region(
		&mut self,
		tid: &str,
		start: u64,
		end: u64,
		out: &mut Vec<AnyBedRecord<T>>,
	) -> error::Result<()>;
	async fn read_lines_in_tid(
		&mut self,
		tid: &str,
		out: &mut Vec<AnyBedRecord<T>>,
	) -> error::Result<()>;
}

#[async_trait::async_trait]
impl<R, T, F> AutoReader<T> for crate::bed::Reader<R, T, F>
where
	R: AsyncReadSeek + Send + Unpin + Sync,
	T: TidResolver + std::clone::Clone + std::fmt::Debug + Send + Sync + 'static,
	F: BedFields<T, T::Tid> + std::fmt::Debug,
	BedRecord<T, T::Tid, F>: IntoAnyBedRecord<T>,
{
	async fn read_line(&mut self) -> error::Result<Option<(&Option<Track>, AnyBedRecord<T>)>>
	{
		let (track, record) = match self.read_line().await?
		{
			Some(v) => v,
			None => return Ok(None),
		};

		Ok(Some((track, record.into_any())))
	}

	async fn read_line_with_meta(
		&mut self,
		browser_meta: &mut Option<BrowserMeta>,
	) -> error::Result<Option<(&Option<Track>, AnyBedRecord<T>)>>
	{
		let (track, record) = match self.read_line_with_meta(browser_meta).await?
		{
			Some(v) => v,
			None => return Ok(None),
		};

		Ok(Some((track, record.into_any())))
	}

	async fn read_lines_in_tid_region(
		&mut self,
		tid: &str,
		start: u64,
		end: u64,
		out: &mut Vec<AnyBedRecord<T>>,
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

		let bytes = self.read_bytes_for_region(&ranges).await?;
		out.clear();
		let mut cursor = &bytes[..];

		while !cursor.is_empty()
		{
			let mut scratch = self.blank_record().await;
			cursor = self.parse_record_into(cursor, &mut scratch).await?;

			out.push(scratch.into_any());
		}

		let tid_id = {
			let r = self.resolver.lock().await;
			r.find(tid)
				.ok_or(error::Error::BedFormat(self.name.clone()))?
		};

		out.retain(|rec| *rec.tid() == tid_id && rec.start() < end && rec.end() > start);

		Ok(())
	}
	async fn read_lines_in_tid(
		&mut self,
		tid: &str,
		out: &mut Vec<AnyBedRecord<T>>,
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

		let bytes = self.read_bytes_for_region(&ranges).await?;
		out.clear();
		let mut cursor = &bytes[..];

		while !cursor.is_empty()
		{
			let mut scratch = self.blank_record().await;
			cursor = self.parse_record_into(cursor, &mut scratch).await?;

			out.push(scratch.into_any());
		}

		Ok(())
	}
}
