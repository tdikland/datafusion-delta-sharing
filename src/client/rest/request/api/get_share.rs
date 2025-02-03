use bon::Builder;
use bytes::Bytes;
use http::{Request, Uri};

use super::response::GetShareResponse;
use super::{IntoRequest, RequestError};

#[derive(Debug, Builder)]
pub struct GetShareRequest<'req> {
    share: &'req str,
}

impl IntoRequest for GetShareRequest<'_> {
    type Response = GetShareResponse;

    fn into_request(self) -> Result<Request<Bytes>, RequestError> {
        let path_and_query = format!("/shares/{}", self.share);

        let uri = Uri::builder().path_and_query(path_and_query).build()?;
        let req = Request::builder().uri(uri).body(Bytes::new())?;
        Ok(req)
    }
}

#[cfg(test)]
mod test {
    use http::{Method, Version};

    use super::*;

    #[test]
    fn example() {
        let req = GetShareRequest::builder()
            .share("test_share")
            .build()
            .into_request()
            .unwrap();

        assert_eq!(req.method(), Method::GET);
        assert_eq!(req.uri().path(), "/shares/test_share");
        assert!(req.uri().query().is_none());
        assert_eq!(req.version(), Version::HTTP_11);
        assert!(req.headers().is_empty());
        assert!(req.body().is_empty());
    }
}
