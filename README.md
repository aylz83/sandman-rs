# sandman-rs

A Rust crate for asynchronous reading of bed, bgzipped bed, and tabix indexed beds.

## Features -

 - Transparently handles and detects bed files with 3, 4, 5, 6, 12 columns automatically as well as bedmethyl formats created by modkit.
 - Transparently handles and detects bed or bed.gz files upon opening
 - Supports bed files with browser and track lines
 - Support for reading line by line or requesting regions such as chromomosomes/tids and/or start and end (Requires tabix file)

## Example usage -

```rust
use sandman::prelude::*;

// "my_bed.bed.gz.tbi" will be opened if exists, or this can be supplied by
// changing None to a path if the tbi is under a different name/location
// requires known Bed3, 4, 5, 6, 12 or Methyl format at compile time
let mut reader = sandman::bed::Reader::<_, _, Bed3Fields>::from_path("my_bed.bed.gz", None).await?;

// Alternatively, dynamic determination of bed formats can be handled at runtime with
let mut reader = sandman::bed::autoreader::from_path("my_bed.bed.gz", None).await?;

while let Some((track, line)) = reader.read_line().await?
{
	// track will be None if no 'track' lines are present within the bed
	println!("Bed line: {:?} in track {:?}", line, track);
}

// To read specific regions
let mut regions = Vec::new();
reader.read_lines_in_tid("chr3", &mut regions).await?;
// or
reader.read_lines_in_tid_in_region("chr3", 1000, 5000, &mut regions).await?;

// To obtain browser lines if required ...
let mut browser_meta: Option<sandman::bed::BrowserMeta> = None;
let mut reader = reader.read_line_with_meta(&mut browser_meta).await?
```

## TODO -

- [ ] Documentation
- [ ] bigBed?
- [ ] Custom bed column formats?
- [X] Don't allocate Record on heap on every read_line
