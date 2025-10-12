# sandman-rs

A Rust crate for asynchronous reading of bed, bgzipped bed, and tabix indexed beds.

## Features -

 - Transparently handles and detects bed files with 3, 4, 5, 6, 12 columns automatically as well as bedmethyl formats created by modkit.
 - Transparently handles and detects bed or bed.gz files upon opening
 - Supports bed files with browser and track lines
 - Support for reading line by line or requesting regions such as chromomosomes/tids and/or start and end (Requires tabix file)

## Example usage -

```rust
// "my_bed.gz.tbi" will be opened if exists, or this can be supplied by
// changing None to a path if the tbi is under a different name/location
let mut reader = sandman::bed::Reader::from_path("my_bed.gz", None).await?;

while let Some((track, line)) = reader.read_line().await?
{
	// track will be None if no 'track' lines are present within the bed
	println!("Bed line: {:?} in track {:?}", line, track);
}

// To read specific regions
let regions = reader.read_lines_in_tid("chr3").await?;
// or
let regions = reader.read_lines_in_tid_in_region("chr3", 1000, 5000).await?;

// To obtain browser lines if required ...
let mut browser_meta: Option<sandman::bed::BrowserMeta> = None;
let mut reader = reader.read_line_with_meta(&mut browser_meta).await?
```

## TODO -

 - Documentation
 - bigBed?
 - Custom bed column formats?
 - Don't allocate Record on heap on every read_line
