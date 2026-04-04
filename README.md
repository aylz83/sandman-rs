# sandman-rs

A Rust crate for asynchronous FAST reading of bzgipped bed files.


## Features -

 - Transparently handles and detects bed files with 3, 4, 5, 6, 12 columns automatically as well as bedmethyl formats created by modkit.
 - Allows reading bgzipped bed chunks in numbers of blocks at once.
 - Passes parsed reads back as parsed - keeping minimal amount in memory at once.
 - Decompresses blocks with multiple cores.
 - Support removal of reads not hitting a filtering threshold (such as base mismatches or minimum scores) before passed back.
 - Supports a block pool to reuse decompressed blocks.

## Example usage -

Needs to be rewritten due to change in infrastructure - see example folder for now 

## TODO -

- [ ] Documentation
- [ ] Custom bed column formats API?
- [X] Don't allocate Record on heap on every read_line
