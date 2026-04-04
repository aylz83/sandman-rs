use crate::filtering::ReadFilterContext;

use tokio::sync::Mutex;

use std::ops::Deref;
use std::sync::Arc;

use crate::bed::{ReaderId, SourceId};

#[allow(dead_code)]
pub struct BgzfBlock
{
	// Compressed block file offset
	// pub(crate) block_offset: u64,

	// Decompressed payload (no BGZF header/trailer)
	pub(crate) bytes: Vec<u8>,

	pub(crate) filter_ctx: Option<Arc<Mutex<ReadFilterContext>>>,
	pub(crate) source_id: Option<SourceId>,
	pub(crate) reader_id: ReaderId,
}

impl Deref for BgzfBlock
{
	type Target = Vec<u8>;

	fn deref(&self) -> &Self::Target
	{
		&self.bytes
	}
}

// impl BgzfBlock
// {
// 	pub fn len(&self) -> usize
// 	{
// 		self.bytes.len()
// 	}

// 	pub fn is_empty(&self) -> bool
// 	{
// 		self.bytes.is_empty()
// 	}
// }
