#![doc = include_str!("../README.md")]

use aws_sdk_s3::output::HeadObjectOutput;
use bytes::Buf;
use log::{debug, error};
use std::io::{Read, Seek, SeekFrom};
use thiserror::Error;
use tokio::runtime::Runtime;

#[derive(Error, Debug)]
pub enum S3ReaderError {
    #[error("missing protocol in URI")]
    MissingS3Protocol,
    #[error("missing bucket or object in URI")]
    MissingObjectUri,
    #[error("object could not be fetched: {0}")]
    ObjectNotFetched(String),
    #[error("could not read from body of object")]
    InvalidContent,
    #[error("invalid read range {0}-{1}")]
    InvalidRange(u64, u64),
}

impl From<aws_sdk_s3::types::SdkError<aws_sdk_s3::error::GetObjectError>> for S3ReaderError {
    fn from(err: aws_sdk_s3::types::SdkError<aws_sdk_s3::error::GetObjectError>) -> S3ReaderError {
        S3ReaderError::ObjectNotFetched(err.to_string())
    }
}

impl From<S3ReaderError> for std::io::Error {
    fn from(error: S3ReaderError) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::InvalidData, error)
    }
}

/// The URI of an S3 object
#[derive(Clone, Debug)]
pub struct S3ObjectUri {
    bucket: String,
    key: String,
}

impl S3ObjectUri {
    /// Returns an `S3ObjectUri` for the provided S3 URI
    ///
    /// # Example
    ///
    /// ```
    /// use s3reader::S3ObjectUri;
    /// let uri = S3ObjectUri::new("s3://mybucket/path/to/file.xls").unwrap();
    ///
    /// assert_eq!(uri.bucket() , "mybucket");
    /// assert_eq!(uri.key() , "path/to/file.xls");
    /// ```
    pub fn new(uri: &str) -> Result<S3ObjectUri, S3ReaderError> {
        if &uri[0..5] != "s3://" {
            return Err(S3ReaderError::MissingS3Protocol);
        }
        if let Some(idx) = uri[5..].find(&['/']) {
            Ok(S3ObjectUri {
                bucket: uri[5..idx + 5].to_string(),
                key: uri[idx + 6..].to_string(),
            })
        } else {
            Err(S3ReaderError::MissingObjectUri)
        }
    }

    /// Returns the bucket name
    /// # Example
    ///
    /// ```
    /// use s3reader::S3ObjectUri;
    /// let uri = S3ObjectUri::new("s3://mybucket/path/to/file.xls").unwrap();
    ///
    /// assert_eq!(uri.bucket() , "mybucket");
    /// ```
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the object's key
    /// # Example
    ///
    /// ```
    /// use s3reader::S3ObjectUri;
    /// let uri = S3ObjectUri::new("s3://mybucket/path/to/file.xls").unwrap();
    ///
    /// assert_eq!(uri.key() , "path/to/file.xls");
    /// ```
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// A Reader for S3 objects that implements the `Read` and `Seek` traits
///
/// This reader allows byte-offset acces to any S3 objects
///
/// # Example
/// ```no_run
/// use std::io::{Read, Seek};
/// use s3reader::S3Reader;
/// use s3reader::S3ObjectUri;
///
/// let uri = S3ObjectUri::new("s3://my-bucket/path/to/huge/file").unwrap();
/// let mut reader = S3Reader::open(uri).unwrap();
///
/// reader.seek(std::io::SeekFrom::Start(100)).unwrap();
///
/// let mut buf: Vec<u8> = [0; 1024].to_vec();
/// reader.read(&mut buf).expect("Error reading from S3");
/// ```
pub struct S3Reader {
    client: aws_sdk_s3::Client,
    uri: S3ObjectUri,
    pos: u64,
    header: Option<HeadObjectOutput>,
}

impl S3Reader {
    /// Creates a new `S3Reader`.
    ///
    /// This method does not check for presence of an actual object in S3 or for connectivity.
    /// Use [`S3Reader::open`] instead to ensure that the S3 object actually exists.
    pub fn new(uri: S3ObjectUri) -> S3Reader {
        let config = Runtime::new()
            .unwrap()
            .block_on(aws_config::load_from_env());
        S3Reader::from_config(&config, uri)
    }

    /// Creates a new `S3Reader` and checks for presence of the S3 object
    ///
    /// This method is the preferred way to create a Reader. It has a minor overhead
    /// because it fetches the object's header from S3, but this ensures that the
    /// object is actually available and thus prevents possible runtime errors.
    pub fn open(uri: S3ObjectUri) -> Result<S3Reader, S3ReaderError> {
        let mut reader = S3Reader::new(uri);
        match Runtime::new().unwrap().block_on(reader.fetch_header()) {
            Err(err) => Err(S3ReaderError::ObjectNotFetched(err.to_string())),
            Ok(_) => Ok(reader),
        }
    }

    /// Creates a new `S3Reader` with a custom AWS `SdkConfig`
    ///
    /// This method is useful if you don't want to use the default configbuilder using the environment.
    /// It does not check for correctness, connectivity to the S3 bucket or presence of the S3 object.
    pub fn from_config(config: &aws_types::sdk_config::SdkConfig, uri: S3ObjectUri) -> S3Reader {
        let client = aws_sdk_s3::Client::new(config);
        S3Reader {
            client,
            uri,
            pos: 0,
            header: None,
        }
    }

    /// Returns the bytes read from the S3 object for the specified byte-range
    ///
    /// This method does not update the internal cursor position. To maintain
    /// an internal state, use [`S3Reader::seek`] and [`S3Reader::read`] instead.
    ///
    /// # Example
    /// ```no_run
    /// use tokio::runtime::Runtime;
    ///
    /// use s3reader::S3Reader;
    /// use s3reader::S3ObjectUri;
    ///
    /// let uri = S3ObjectUri::new("s3://my-bucket/path/to/huge/file").unwrap();
    /// let mut reader = S3Reader::open(uri).unwrap();
    ///
    /// // `read_range` is an async function, we must wrap it in a runtime in the doctest
    /// let bytes = Runtime::new().unwrap().block_on(
    ///     reader.read_range(100, 250)
    /// ).unwrap().into_bytes();
    /// assert_eq!(bytes.len(), 150);
    /// ```
    pub async fn read_range(
        &mut self,
        from: u64,
        to: u64,
    ) -> Result<aws_sdk_s3::types::AggregatedBytes, S3ReaderError> {
        if to < from || from > self.len() {
            return Err(S3ReaderError::InvalidRange(from, to));
        }
        debug!("Reading range {}-{}", from, to);
        let object_output = self
            .client
            .get_object()
            .bucket(self.uri.bucket())
            .key(self.uri.key())
            .range(format!("bytes={}-{}", from, to))
            .send()
            .await?;

        match object_output.body.collect().await {
            Ok(x) => Ok(x),
            Err(_) => Err(S3ReaderError::InvalidContent),
        }
    }

    /// Fetches the object's header from S3
    ///
    /// # Example
    /// ```no_run
    /// use tokio::runtime::Runtime;
    ///
    /// use s3reader::S3Reader;
    /// use s3reader::S3ObjectUri;
    ///
    /// let uri = S3ObjectUri::new("s3://my-bucket/path/to/huge/file").unwrap();
    /// let mut reader = S3Reader::open(uri).unwrap();
    ///
    /// // `fetch_header` is an async function, we must wrap it in a runtime in the doctest
    /// Runtime::new().unwrap().block_on(
    ///     reader.fetch_header()
    /// ).unwrap();
    /// assert_eq!(reader.len(), 150);
    /// ```
    pub async fn fetch_header(
        &mut self,
    ) -> Result<(), aws_sdk_s3::types::SdkError<aws_sdk_s3::error::HeadObjectError>> {
        let header = self
            .client
            .head_object()
            .bucket(self.uri.bucket())
            .key(self.uri.key())
            .send()
            .await?;
        self.header = Some(header);
        Ok(())
    }

    /// Returns the `content_length` of the S3 object
    ///
    /// # Panics
    /// This method can panic if the header cannot be fetched (e.g. due to network issues, wrong URI etc).
    /// This can be prevented by using [`S3Reader::open`] which guarantees that the header is present.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&mut self) -> u64 {
        if let Some(header) = &self.header {
            header.content_length() as u64
        } else {
            Runtime::new()
                .unwrap()
                .block_on(self.fetch_header())
                .expect("unable to determine the object size");
            self.len()
        }
    }

    pub fn pos(&self) -> u64 {
        self.pos
    }
}

impl Read for S3Reader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        if self.pos >= self.len() {
            return Ok(0);
        }
        let end_pos = self.pos + buf.len() as u64;
        let s3_data = Runtime::new()
            .unwrap()
            .block_on(self.read_range(self.pos, end_pos))?;
        self.pos = end_pos;
        let mut reader = s3_data.reader();
        reader.read(buf)
    }

    /// Custom implementation to avoid too many `read` calls. The default trait
    /// reads in 32 bytes blocks that grow over time. However, the IO for S3 has way
    /// more latency so `S3Reader` tries to fetch all data in a single call
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize, std::io::Error> {
        let reader_len = self.len();
        let s3_data = Runtime::new()
            .unwrap()
            .block_on(self.read_range(self.pos, reader_len))?;

        self.pos = reader_len;
        let data_len = s3_data.remaining();

        buf.reserve(data_len);
        for b in s3_data.into_bytes() {
            buf.push(b);
        }
        Ok(data_len)
    }

    /// Custom implementation to avoid too many `read` calls. The default trait
    /// reads in 32 bytes blocks that grow over time. However, the IO for S3 has way
    /// more latency so `S3Reader` tries to fetch all data in a single call
    fn read_to_string(&mut self, buf: &mut String) -> Result<usize, std::io::Error> {
        let mut bytes = Vec::new();
        match self.read_to_end(&mut bytes) {
            Ok(n) => {
                buf.reserve(n);
                for byte in bytes {
                    buf.push(byte.into());
                }
                Ok(n)
            }
            Err(err) => Err(err),
        }
    }
}

impl Seek for S3Reader {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, std::io::Error> {
        match s3reader_seek(self.len(), self.pos, pos) {
            Ok(x) => {
                self.pos = x;
                Ok(x)
            }
            Err(err) => Err(err),
        }
    }
}

/// Calculates the new cursor for a `Seek` operation
///
/// This function is declared outside of `S3Reader` so that it can be
/// unit-tested.
fn s3reader_seek(len: u64, cursor: u64, pos: SeekFrom) -> Result<u64, std::io::Error> {
    match pos {
        SeekFrom::Start(x) => {
            if x > len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "cannot seek out of bounds",
                ));
            }
            Ok(x)
        }
        SeekFrom::Current(x) => match x >= 0 {
            true => {
                // we can safely cast this to u64, positive i64 will always be smaller
                let x = x as u64;
                if (x + cursor) > len {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "cannot seek out of bounds",
                    ));
                }
                Ok(cursor + x)
            }
            false => {
                // we can safely cast this to u64, since abs i64 will always be smaller than u64
                let x = x.unsigned_abs();
                if x > cursor {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "position cannot be negative",
                    ));
                }
                Ok(cursor - x)
            }
        },
        SeekFrom::End(x) => {
            if x > 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "cannot seek out of bounds",
                ));
            }
            // we can safely cast this to u64, since abs i64 will always be smaller than u64
            let x = x.unsigned_abs();
            if x > len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "position cannot be negative",
                ));
            };
            Ok(len - x as u64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_absolute_position() {
        assert_eq!(
            s3reader_seek(100, 1, std::io::SeekFrom::Start(30)).unwrap(),
            30
        );
        assert_eq!(
            s3reader_seek(100, 1, std::io::SeekFrom::Start(0)).unwrap(),
            0
        );
        assert_eq!(
            s3reader_seek(100, 1, std::io::SeekFrom::Start(100)).unwrap(),
            100
        );
        assert!(s3reader_seek(100, 1, std::io::SeekFrom::Start(101)).is_err());
    }

    #[test]
    fn test_relative_position() {
        assert_eq!(
            s3reader_seek(100, 1, std::io::SeekFrom::Current(30)).unwrap(),
            31
        );
        assert_eq!(
            s3reader_seek(100, 1, std::io::SeekFrom::Current(99)).unwrap(),
            100
        );
        assert_eq!(
            s3reader_seek(100, 1, std::io::SeekFrom::Current(0)).unwrap(),
            1
        );
        assert_eq!(
            s3reader_seek(100, 1, std::io::SeekFrom::Current(-1)).unwrap(),
            0
        );
        assert_eq!(
            s3reader_seek(100, 0, std::io::SeekFrom::Current(0)).unwrap(),
            0
        );
        assert_eq!(
            s3reader_seek(100, 0, std::io::SeekFrom::Current(1)).unwrap(),
            1
        );
        assert!(s3reader_seek(100, 1, std::io::SeekFrom::Current(-2)).is_err());
        assert!(s3reader_seek(100, 1, std::io::SeekFrom::Current(100)).is_err());
    }

    #[test]
    fn test_seek_from_end() {
        assert!(s3reader_seek(100, 1, std::io::SeekFrom::End(1)).is_err());
        assert!(s3reader_seek(100, 1, std::io::SeekFrom::End(-101)).is_err());
        assert_eq!(
            s3reader_seek(100, 1, std::io::SeekFrom::End(0)).unwrap(),
            100
        );
        assert_eq!(
            s3reader_seek(100, 1, std::io::SeekFrom::End(-100)).unwrap(),
            0
        );
        assert_eq!(
            s3reader_seek(100, 1, std::io::SeekFrom::End(-50)).unwrap(),
            50
        );
    }
}
