use crate::model::{
    self,
    action::parquet::{ParquetFile, ParquetMetadata, ParquetProtocol},
};

mod delta;
mod parquet;
mod old;

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSharesResponse {
    pub items: Vec<model::Share>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetShareResponse {
    pub share: model::Share,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSchemasResponse {
    pub items: Vec<model::Schema>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTablesResponse {
    pub items: Vec<model::Table>,
    pub next_page_token: Option<String>,
}

pub enum QueryTableResponse {
    Parquet(QueryTableParquetResponse),
}

pub struct QueryTableParquetResponse {
    pub protocol: ParquetProtocol,
    pub metadata: ParquetMetadata,
    pub files: Vec<ParquetFile>,
}

#[derive(Debug, serde::Deserialize)]
pub enum ResponseLine {
    Parquet(ParquetResponseLine),
}

#[derive(Debug, serde::Deserialize)]
pub enum ParquetResponseLine {
    Protocol(ParquetProtocol),
    Metadata(ParquetMetadata),
    File(ParquetFile),
}
