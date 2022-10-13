[![Build](https://github.com/anergictcell/s3reader/actions/workflows/build.yml/badge.svg)](https://github.com/anergictcell/s3reader/actions/workflows/build.yml)
[![crates.io](https://img.shields.io/crates/v/s3reader?color=#3fb911)](https://crates.io/crates/s3reader)
[![doc-rs](https://img.shields.io/docsrs/s3reader/latest)](https://docs.rs/s3reader/latest/s3reader/)

# S3Reader

A `Rust` library to read from S3 object as if they were files on a local filesystem (almost). The `S3Reader` adds both `Read` and `Seek` traits, allowing to place the cursor anywhere within the S3 object and read from any byte offset. This allows random access to bytes within S3 objects.

## Usage
Add this to your `Cargo.toml`:

```text
[dependencies]
s3reader = "0.5.0"
```

### Use `BufRead` to read line by line
```rust
use std::io::{BufRead, BufReader};

use s3reader::S3Reader;
use s3reader::S3ObjectUri;


fn read_lines_manually() -> std::io::Result<()> {
    let uri = S3ObjectUri::new("s3://my-bucket/path/to/huge/file").unwrap();
    let s3obj = S3Reader::open(uri).unwrap();

    let mut reader = BufReader::new(s3obj);

    let mut line = String::new();
    let len = reader.read_line(&mut line).unwrap();
    println!("The first line >>{line}<< is {len} bytes long");

    let mut line2 = String::new();
    let len = reader.read_line(&mut line2).unwrap();
    println!("The next line >>{line2}<< is {len} bytes long");

    Ok(())
}

fn use_line_iterator() -> std::io::Result<()> {
    let uri = S3ObjectUri::new("s3://my-bucket/path/to/huge/file").unwrap();
    let s3obj = S3Reader::open(uri).unwrap();

    let reader = BufReader::new(s3obj);

    let mut count = 0;
    for line in reader.lines() {
        println!("{}", line.unwrap());
        count += 1;
    }

    Ok(())
}
```

### Use `Seek` to jump to positions
```rust
use std::io::{Read, Seek, SeekFrom};

use s3reader::S3Reader;
use s3reader::S3ObjectUri;

fn jump_within_file() -> std::io::Result<()> {
    let uri = S3ObjectUri::new("s3://my-bucket/path/to/huge/file").unwrap();
    let mut reader = S3Reader::open(uri).unwrap();

    let len = reader.len();

    let cursor_1 = reader.seek(SeekFrom::Start(len as u64)).unwrap();
    let cursor_2 = reader.seek(SeekFrom::End(0)).unwrap();
    assert_eq!(cursor_1, cursor_2);

    reader.seek(SeekFrom::Start(10)).unwrap();
    let mut buf = [0; 100];
    let bytes = reader.read(&mut buf).unwrap();
    assert_eq!(buf.len(), 100);
    assert_eq!(bytes, 100);

    Ok(())
}
```


## Q/A
**Does this library really provide random access to S3 objects?**  
According to this [StackOverflow answer](https://stackoverflow.com/questions/60176997/does-aws-s3-getobject-provide-random-access), yes.

**Are the reads sync or async?**  
The S3-SDK uses mostly async operations, but the `Read` and `Seek` traits require sync methods. Due to this, I'm using a blocking tokio runtime to wrap the async calls. This might not be the best solution, but works well for me. Any improvement suggestions are very welcome

**Why is this useful?**  
Depends on your use-cases. If you need to access random bytes in the middle of large files/S3 object, this library is useful. For example, you can read it to stream mp4 files. It's also quite useful for some bioinformatic applications, where you might have a huge, several GB reference genome, but only need to access data of a few genes, accounting to only a few MB.
