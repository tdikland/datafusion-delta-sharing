use std::ops::{Range, RangeBounds};
use std::sync::Arc;

use bytes::Bytes;

use futures::future::BoxFuture;
use futures::FutureExt;
use parquet::arrow::async_reader::MetadataLoader;
use parquet::file::metadata::ParquetMetaData;
use reqwest::{header::RANGE, Client, Method, Url};

use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::{ParquetError, Result};

mod exec;

struct ParquetFileReader {
    url: String,
    size: usize,
    client: Client,
}

impl ParquetFileReader {
    fn new(url: String, size: usize) -> Self {
        let client = reqwest::Client::new();
        Self { url, size, client }
    }

    async fn get_range(&self, range: Range<usize>) -> Result<Bytes> {
        let url = Url::parse(&self.url).unwrap();

        let first = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => i + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let range_string = match range.end_bound() {
            std::ops::Bound::Included(i) => format!("bytes={}-{}", first, i + 1),
            std::ops::Bound::Excluded(i) => format!("bytes={}-{}", first, i),
            std::ops::Bound::Unbounded => format!("bytes={}-", first),
        };

        self.client
            .request(Method::GET, url)
            .header(RANGE, range_string)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .map_err(|e| ParquetError::General(format!("[PARQUET_FILE_READER_ERROR] source: {e}")))
    }
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>> {
        async move {
            self.get_range(range).await.map_err(|e| {
                ParquetError::General(format!("[PARQUET_FILE_READER_ERROR] source: {e}"))
            })
        }
        .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let size = self.size;
            let mut loader = MetadataLoader::load(self, size, None).await?;
            loader.load_page_index(false, false).await?;
            Ok(Arc::new(loader.finish()))
        })
    }
}
