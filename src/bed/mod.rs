pub mod autoreader;
mod autorecord;
mod bed;
#[cfg(feature = "bigbed")]
pub(crate) mod bigbedrecord;
mod extra;
mod parser;
mod reader;
mod record;

pub use reader::*;
pub use bed::*;
pub use autorecord::*;

use crate::error;

use tokio::fs::File as TokioFile;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncSeekExt, BufReader as TokioBufReader};
use std::path::Path;
use pufferfish::prelude::*;

pub async fn detect_format<P>(path: P) -> error::Result<BedFormat>
where
	P: AsRef<Path>,
{
	let file = TokioFile::open(&path).await?;
	let mut reader = TokioBufReader::new(file);

	let is_bgzf = reader.is_bgz().await;
	reader.seek(std::io::SeekFrom::Start(0)).await?;

	let lines = if is_bgzf
	{
		// Read first BGZF block
		let block = reader
			.read_bgzf_block(Some(is_bgzf_eof))
			.await
			.map_err(|_| error::Error::BedFormat(path.as_ref().display().to_string()))?
			.ok_or_else(|| error::Error::BedFormat(path.as_ref().display().to_string()))?;

		let mut block_reader = TokioBufReader::new(std::io::Cursor::new(&block));
		read_lines(&mut block_reader, 10).await?
	}
	else
	{
		// Plain text
		read_lines(&mut reader, 10).await?
	};

	BedFormat::try_from(&lines).map_err(|_| {
		error::Error::BedFormat(
			path.as_ref()
				.file_name()
				.and_then(|s| s.to_str())
				.unwrap_or("unknown")
				.to_string(),
		)
	})
}

/// Helper to read up to `max_lines` lines from a buffered reader.
async fn read_lines<B>(reader: &mut B, max_lines: usize) -> error::Result<Vec<String>>
where
	B: AsyncBufRead + Unpin,
{
	let mut lines = Vec::new();
	let mut buf = String::new();

	for _ in 0..max_lines
	{
		buf.clear();
		let n = reader.read_line(&mut buf).await?;
		if n == 0
		{
			break;
		}
		let trimmed = buf.trim();
		if !trimmed.is_empty()
		{
			lines.push(buf.clone());
		}
	}

	Ok(lines)
}
