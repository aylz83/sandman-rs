use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()>
{
	env_logger::init();

	let args: Vec<String> = env::args().collect();

	let bed_file = &args[1];

	println!("Input file: {}", &bed_file);

	let mut reader = sandman::bed::Reader::from_path(bed_file).await?;

	while let Some(line) = reader.read_line().await?
	{
		println!("Bed line: {:?}", line);
	}

	Ok(())
}
