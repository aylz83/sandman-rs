use std::env;

use sandman::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()>
{
	env_logger::init();

	let args: Vec<String> = env::args().collect();

	let bed_file = &args[1];

	println!("Input file: {}", &bed_file);

	let mut reader = sandman::bed::autoreader::from_path(bed_file, None).await?;

	// let mut lines = Vec::new();
	// reader.read_lines_in_tid("chr1", &mut lines).await?;
	// for line in lines
	// {
	// 	println!("Bed line: {:?}", line);
	// }

	let mut browser_meta: Option<sandman::bed::BrowserMeta> = None;
	while let Some((track, line)) = reader.read_line_with_meta(&mut browser_meta).await?
	{
		// println!("Browser meta data = {:?}", browser_meta);
		println!("Bed line: {:?} in track {:?}", line, track);
		println!("Resolved tid = {:?}", line.pretty_tid().await);
	}

	Ok(())
}
