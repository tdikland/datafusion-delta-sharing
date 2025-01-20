use core::fmt;

use reqwest::Response;

mod api;
mod error;
mod line;
mod util;

// pub use api::GetShareResponse;
// pub use api::ListSchemasResponse;
// pub use api::ListSharesResponse;
// pub use api::ListTablesResponse;
// pub use api::MetadataResponseLines;
// pub use api::QueryTableChangesResponse;
// pub use api::QueryTableDataResponse;
// pub use api::QueryTableMetadataResponse;
// pub use api::QueryTableVersionResponse;
pub use api::*;

pub use error::ErrorResponse;

const DELTA_TABLE_VERSION_HEADER: &'static str = "Delta-Table-Version";

#[async_trait::async_trait]
pub(crate) trait FromResponse: Sized {
    type Error;

    async fn parse(res: Response) -> Result<Self, Self::Error>;
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub enum ParseResponseError {
    MissingJsonContentType,
    MissingNdJsonContentType,
    MissingRequiredHeader { header_name: String },
    InvalidHeader { header_name: String, err: String },
    BodyError { source: BoxError },
    DecodeBody { path: String, source: BoxError },
    MissingProtocol,
    MissingMetadata,
    UnexpectedEndOfStream,
    UnexpectedWrapperObject { expected: String },
}

impl fmt::Display for ParseResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseResponseError::MissingJsonContentType => {
                write!(f, "missing 'application/json' content type")
            }
            ParseResponseError::MissingNdJsonContentType => {
                write!(f, "missing 'application/x-ndjson' content type")
            }
            ParseResponseError::MissingRequiredHeader { header_name } => {
                write!(f, "missing required header: {}", header_name)
            }
            ParseResponseError::InvalidHeader { header_name, err } => {
                write!(f, "invalid header '{}': {}", header_name, err)
            }
            ParseResponseError::BodyError { source } => {
                write!(f, "error reading response body: {}", source)
            }
            ParseResponseError::DecodeBody { path, source } => {
                write!(f, "error decoding response body at '{}': {}", path, source)
            }
            ParseResponseError::UnexpectedEndOfStream => {
                write!(f, "unexpected end of stream")
            }
            ParseResponseError::UnexpectedWrapperObject { expected } => {
                write!(f, "unexpected wrapper object: expected '{}'", expected)
            }
            ParseResponseError::MissingProtocol => {
                write!(f, "missing protocol response line")
            }
            ParseResponseError::MissingMetadata => {
                write!(f, "missing metadata response line")
            }
        }
    }
}

impl std::error::Error for ParseResponseError {}

// #[derive(Debug, Deserialize)]
// #[serde(untagged)]
// pub enum ResponseLine {
//     Parquet(ParquetResponseLine),
// }

// #[derive(Debug, Deserialize)]
// #[serde(rename_all = "camelCase")]
// pub enum ParquetResponseLine {
//     Protocol(Protocol),
//     #[serde(rename = "metaData")]
//     Metadata(Metadata),
//     File(File),
// }

// impl ParquetResponseLine {
//     pub fn to_protocol(self) -> Option<Protocol> {
//         match self {
//             ParquetResponseLine::Protocol(p) => Some(p),
//             _ => None,
//         }
//     }

//     pub fn to_metadata(self) -> Option<Metadata> {
//         match self {
//             ParquetResponseLine::Metadata(m) => Some(m),
//             _ => None,
//         }
//     }

//     pub fn to_file(self) -> Option<File> {
//         match self {
//             ParquetResponseLine::File(f) => Some(f),
//             _ => None,
//         }
//     }
// }
