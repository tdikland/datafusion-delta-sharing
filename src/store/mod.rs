use std::collections::HashMap;
use std::fmt::Display;
use std::ops::{Range, RangeBounds};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, ObjectMeta, ObjectStore,
    PutOptions, PutResult, Result,
};
use reqwest::header::{HeaderMap, CONTENT_LENGTH, CONTENT_RANGE, ETAG, LAST_MODIFIED, RANGE};
use reqwest::{Client, Method, Response, Url};
use tokio::io::AsyncWrite;

use object_store::Error;

pub struct SignedHttpStoreBuilder {
    signed_urls: Vec<String>,
    base: String,
}

impl SignedHttpStoreBuilder {
    pub fn new(base: String) -> Self {
        Self {
            base,
            signed_urls: vec![],
        }
    }

    pub fn with_signed_urls(mut self, urls: Vec<String>) -> Self {
        self.signed_urls = urls;
        self
    }

    pub fn build(self) -> SignedHttpStore {
        let mut map = HashMap::new();

        for signed_url in self.signed_urls {
            let url = Url::parse(&signed_url).unwrap();
            let path = Path::parse(url.path()).unwrap();
            let qs = url.query().unwrap_or("").to_string();
            map.insert(path, qs);
        }

        let client = reqwest::Client::new();
        SignedHttpStore {
            base: self.base,
            map,
            client,
        }
    }
}

#[derive(Debug)]
pub struct SignedHttpStore {
    base: String,
    map: HashMap<Path, String>,
    client: Client,
}

impl Display for SignedHttpStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HttpStore with Signed URLs")
    }
}

#[async_trait]
impl ObjectStore for SignedHttpStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _bytes: Bytes,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        Err(Error::NotImplemented)
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Err(Error::NotImplemented)
    }

    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let mut url = Url::parse(&self.base).unwrap();
        url.path_segments_mut().unwrap().extend(location.parts());

        let qs = self.map.get(location).unwrap();
        url.set_query(Some(qs));

        let mut request = self.client.request(Method::GET, url);
        request = if let Some(range) = options.range.clone() {
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

            request.header(RANGE, range_string)
        } else {
            request
        };

        let response = request.send().await.unwrap();

        let get_result = get_result(location, options.range.clone(), response);
        Ok(get_result)
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        Box::pin(futures::stream::iter(Err(Error::NotImplemented)))
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        Err(Error::NotImplemented)
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }
}

struct ContentRange {
    /// The range of the object returned
    range: Range<usize>,
    /// The total size of the object being requested
    size: usize,
}

use futures::StreamExt;
use futures::TryStreamExt;

impl ContentRange {
    /// Parse a content range of the form `bytes <range-start>-<range-end>/<size>`
    ///
    /// <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range>
    fn from_str(s: &str) -> Option<Self> {
        let rem = s.trim().strip_prefix("bytes ")?;
        let (range, size) = rem.split_once('/')?;
        let size = size.parse().ok()?;

        let (start_s, end_s) = range.split_once('-')?;

        let start = start_s.parse().ok()?;
        let end: usize = end_s.parse().ok()?;

        Some(Self {
            size,
            range: start..end + 1,
        })
    }
}

fn get_result(location: &Path, range: Option<Range<usize>>, response: Response) -> GetResult {
    let header_config = HeaderConfig {
        etag_required: false,
        last_modified_required: false,
        version_header: None,
    };
    let mut meta = header_meta(location, response.headers(), header_config);

    // ensure that we receive the range we asked for
    let range = if let Some(_) = range {
        let val = response.headers().get(CONTENT_RANGE).unwrap();

        let value = val.to_str().unwrap();
        let value = ContentRange::from_str(value).unwrap();
        let actual = value.range;

        // Update size to reflect full size of object (#5272)
        meta.size = value.size;

        actual
    } else {
        0..meta.size
    };

    let stream = response
        .bytes_stream()
        .map_err(|source| object_store::Error::Generic {
            store: "signed_http_store",
            source: Box::new(source),
        })
        .boxed();

    GetResult {
        range,
        meta,
        payload: GetResultPayload::Stream(stream),
    }
}

#[derive(Debug, Copy, Clone)]
/// Configuration for header extraction
pub struct HeaderConfig {
    /// Whether to require an ETag header when extracting [`ObjectMeta`] from headers.
    ///
    /// Defaults to `true`
    pub etag_required: bool,
    /// Whether to require a Last-Modified header when extracting [`ObjectMeta`] from headers.
    ///
    /// Defaults to `true`
    pub last_modified_required: bool,

    /// The version header name if any
    pub version_header: Option<&'static str>,
}

/// Extracts an etag from the provided [`HeaderMap`]
pub fn get_etag(headers: &HeaderMap) -> String {
    let e_tag = headers.get(ETAG).unwrap();
    e_tag.to_str().unwrap().to_string()
}

/// Extracts [`ObjectMeta`] from the provided [`HeaderMap`]
pub fn header_meta(location: &Path, headers: &HeaderMap, cfg: HeaderConfig) -> ObjectMeta {
    let last_modified = match headers.get(LAST_MODIFIED) {
        Some(last_modified) => {
            let last_modified = last_modified.to_str().unwrap();
            DateTime::parse_from_rfc2822(last_modified)
                .unwrap()
                .with_timezone(&Utc)
        }
        None if cfg.last_modified_required => panic!("welp"),
        None => Utc.timestamp_nanos(0),
    };

    let e_tag = Some(get_etag(headers));
    let content_length = headers.get(CONTENT_LENGTH).unwrap();

    let content_length = content_length.to_str().unwrap();
    let size = content_length.parse().unwrap();

    let version = match cfg.version_header.and_then(|h| headers.get(h)) {
        Some(v) => Some(v.to_str().unwrap().to_string()),
        None => None,
    };

    ObjectMeta {
        location: location.clone(),
        last_modified,
        version,
        size,
        e_tag,
    }
}
