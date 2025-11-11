use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()>
{
	env_logger::init();

	let args: Vec<String> = env::args().collect();

	let bed_file = &args[1];

	println!("Input file: {}", &bed_file);

	let mut reader = sandman::bed::Reader::from_path(bed_file, None).await?;

	// let lines = reader.read_lines_in_tid("chr3").await?;
	// if let Some(ref lines) = lines
	// {
	// 	for line in lines
	// 	{
	// 		println!("Bed line: {:?}", line);
	// 	}
	// }

	let mut browser_meta: Option<sandman::bed::BrowserMeta> = None;
	while let Some((track, line)) = reader.read_line_with_meta(&mut browser_meta).await?
	{
		println!("Browser meta data = {:?}", browser_meta);
		println!("Bed line: {:?} in track {:?}", line, track);
		println!("Resolved tid = {:?}", line.pretty_tid(reader.store()));
	}

	Ok(())
}
