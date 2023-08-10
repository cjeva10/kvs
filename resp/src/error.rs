use thiserror::Error;

pub type Result<Resp> = std::result::Result<Resp, Error>;

/// An error either serializing or deserializing a Resp object
#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error("deserialization failed")]
    DeserializeFailed,
    #[error("end of file")]
    Eof,
    #[error("encountered an invalid prefix")]
    InvalidPrefix,
    #[error("expected a simple string")]
    ExpectedSimpleString,
    #[error("expected an integer")]
    ExpectedInteger,
    #[error("expected a bulk string")]
    ExpectedBulkString,
    #[error("expected an error")]
    ExpectedError,
    #[error("expecting a CRLF")]
    ExpectedCRLF,
    #[error("expected a null value")]
    ExpectedNull,
    #[error("expected a null array")]
    ExpectedNullArray,
    #[error("found trailing characters")]
    TrailingCharacters,
    #[error("Expected to get an array")]
    ExpectedArray,
    #[error("Couldn't parse bytes to string")]
    CantParseBytes( #[from] core::str::Utf8Error)
}
