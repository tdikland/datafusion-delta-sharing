use std::ops::{Range, RangeBounds};
use std::sync::Arc;

use bytes::Bytes;

use datafusion::datasource::physical_plan::{FileMeta, ParquetFileReaderFactory};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::errors::{ParquetError, Result};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::future::BoxFuture;
use futures::FutureExt;
use reqwest::{header::RANGE, Client, Method, Url};

struct SignedParquetFileReader {
    url: Arc<String>,
    client: Client,
    size: usize,
}

impl SignedParquetFileReader {
    fn new(client: Client, url: Arc<String>, size: usize) -> Self {
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

impl AsyncFileReader for SignedParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Bytes>> {
        async move {
            self.get_range(range).await.map_err(|e| {
                ParquetError::General(format!("[PARQUET_FILE_READER_ERROR] source: {e}"))
            })
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<
        '_,
        datafusion::parquet::errors::Result<
            Arc<datafusion::parquet::file::metadata::ParquetMetaData>,
        >,
    > {
        Box::pin(async move {
            let size = self.size;
            let mut loader =
                datafusion::parquet::arrow::async_reader::MetadataLoader::load(self, size, None)
                    .await?;
            loader.load_page_index(false, false).await?;
            Ok(Arc::new(loader.finish()))
        })
    }
}

#[derive(Debug)]
pub struct SignedParquetFileReaderFactory {
    client: Client,
}

impl SignedParquetFileReaderFactory {
    pub fn new() -> Self {
        let client = Client::new();
        Self { client }
    }
}

impl ParquetFileReaderFactory for SignedParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::error::Result<Box<dyn AsyncFileReader + Send>> {
        let client = self.client.clone();
        let url = file_meta.extensions.unwrap().downcast::<String>().unwrap();
        let size = file_meta.object_meta.size;
        Ok(Box::new(SignedParquetFileReader::new(client, url, size)))
    }
}
