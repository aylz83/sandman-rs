use std::env;
use std::sync::Arc;

use std::time::Instant;

use sandman::pufferfish::pool::BgzfBlockPool;

use sandman::prelude::*;

#[derive(Default)]
struct ExampleSink;

impl BedSink<DefaultTid> for ExampleSink
{
	fn begin_tid(&mut self, _tid: &DefaultTid, _strand: &Strand) {}
	fn end_tid(&mut self, _tid: &DefaultTid, _strand: &Strand) {}

	fn begin_position(&mut self, _start: u64) {}
	fn end_position(&mut self, _end: u64) {}

	fn push_value(
		&mut self,
		_source_id: &Option<SourceId>,
		_reader_id: &ReaderId,
		_value: BedSinkValue,
	)
	{
		// println!("sink values = {:?}", value);
	}
}

#[tokio::main]
async fn main() -> anyhow::Result<()>
{
	env_logger::init();

	let args: Vec<String> = env::args().collect();

	let bed_file = &args[1];

	println!("Input file: {}", &bed_file);

	let block_pool = Arc::new(BgzfBlockPool::new(10000, 64 * 1024));

	let start = Instant::now();

	let mut reader =
		sandman::bed::autooneshotreader::from_path(bed_file, SourceId(0), block_pool).await?;

	let mut sink = ExampleSink::default();
	while let Some(block) = reader.next_bgzf_blocks(200).await?
	{
		reader.read_tids_in_block_sink(block, &mut sink).await?;
	}

	let elapsed = start.elapsed();
	println!("Reader creation + processing took {:?}", elapsed);

	Ok(())
}
