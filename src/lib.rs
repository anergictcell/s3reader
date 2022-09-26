#![doc = include_str!("../README.md")]

use aws_sdk_s3::output::HeadObjectOutput;
use bytes::Buf;
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

const DEFAULT_READ_SIZE: usize = 1024 * 1024; // 1 MB

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

impl<'a> S3Reader{
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
    pub fn from_config(
        config: &aws_types::sdk_config::SdkConfig,
        uri: S3ObjectUri,
    ) -> S3Reader {
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
        if to < from {
            return Err(S3ReaderError::InvalidRange(from, to));
        }
        let object_output = self
            .client
            .get_object()
            .bucket(self.uri.bucket())
            .key(self.uri.key())
            .range(format!("bytes={}-{}", from, to))
            .send()
            .await?;

        match object_output.body.collect().await {
            Ok(x) => {
                // update cursor
                self.pos = to;
                Ok(x)
            }
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
    pub fn len(&mut self) -> i64 {
        if let Some(header) = &self.header {
            header.content_length()
        } else {
            Runtime::new()
                .unwrap()
                .block_on(self.fetch_header())
                .expect("unable to determine the object size");
            self.len()
        }
    }
}

impl Read for S3Reader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let len = std::cmp::min(buf.len(), DEFAULT_READ_SIZE);
        let s3_data = Runtime::new()
            .unwrap()
            .block_on(self.read_range(self.pos, self.pos + len as u64 - 1))?;
        let mut reader = s3_data.reader();
        reader.read(buf)
    }
}

impl Seek for S3Reader {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, std::io::Error> {
        match pos {
            SeekFrom::Start(x) => self.pos = x,
            SeekFrom::Current(x) => self.pos = (self.pos as i64 + x) as u64,
            SeekFrom::End(x) => self.pos = (self.len() + x) as u64,
        }
        Ok(self.pos)
    }
}
