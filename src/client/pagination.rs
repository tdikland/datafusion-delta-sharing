//! Pagination support for the client.

use url::Url;

const QUERY_PARAM_MAX_RESULTS: &str = "maxResults";
const QUERY_PARAM_PAGE_TOKEN: &str = "pageToken";

/// Pagination information for the request.
#[derive(Debug)]
pub struct Pagination {
    max_results: Option<u32>,
    page_token: Option<String>,
    is_start: bool,
}

impl Pagination {
    /// Create a new Pagination
    fn new(max_results: Option<u32>, page_token: Option<String>, is_start: bool) -> Self {
        Self {
            max_results,
            page_token,
            is_start,
        }
    }

    /// Create a new Pagination for the first page
    pub fn from_start(max_results: Option<u32>) -> Self {
        Self::new(max_results, None, true)
    }

    pub fn from_token(max_results: Option<u32>, page_token: String) -> Self {
        Self::new(max_results, Some(page_token), false)
    }

    /// Set the next page token
    pub fn next_token(&mut self, token: Option<String>) {
        self.is_start = false;
        self.page_token = token;
    }

    /// Check if there is another page of results
    pub fn has_next_page(&self) -> bool {
        self.is_start || (self.page_token.is_some() && self.page_token.as_deref() != Some(""))
    }

    /// Check if the pagination is finished
    pub fn is_finished(&self) -> bool {
        !self.has_next_page()
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self::new(None, None, true)
    }
}

pub(crate) trait PaginationExt {
    fn with_pagination(self, pagination: &Pagination) -> Self;
}

impl PaginationExt for Url {
    fn with_pagination(mut self, pagination: &Pagination) -> Self {
        if pagination.max_results.is_none() && pagination.page_token.is_none() {
            return self;
        }

        let mut query_pairs = self.query_pairs_mut();
        if let Some(m) = pagination.max_results {
            query_pairs.append_pair(QUERY_PARAM_MAX_RESULTS, &m.to_string());
        };
        if let Some(token) = &pagination.page_token {
            query_pairs.append_pair(QUERY_PARAM_PAGE_TOKEN, token);
        };
        drop(query_pairs);

        self
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn from_start() {
        let pagination = Pagination::from_start(Some(100));

        assert_eq!(pagination.is_start, true);
        assert_eq!(pagination.max_results, Some(100));
        assert_eq!(pagination.page_token, None);
        assert!(pagination.has_next_page());
    }

    #[test]
    fn from_token() {
        let pagination = Pagination::from_token(Some(100), String::from("foo"));

        assert_eq!(pagination.is_start, false);
        assert_eq!(pagination.max_results, Some(100));
        assert_eq!(pagination.page_token.as_deref(), Some("foo"));
        assert!(pagination.has_next_page());
    }

    #[test]
    fn advance_pagination_from_start() {
        let mut p = Pagination::from_start(None);
        assert!(p.has_next_page());

        p.next_token(Some("foo".to_owned()));
        assert!(p.has_next_page());

        p.next_token(None);
        assert!(!p.has_next_page());
    }

    #[test]
    fn advance_pagination_from_token() {
        let mut p = Pagination::from_token(None, String::from("foo"));
        assert!(p.has_next_page());

        p.next_token(Some("bar".to_owned()));
        assert!(p.has_next_page());

        p.next_token(None);
        assert!(!p.has_next_page());
    }

    #[test]
    fn add_pagination_to_url() {
        let pagination = Pagination::from_token(Some(7), "foo".to_owned());

        let basic_url = Url::parse("http://delta.io/")
            .unwrap()
            .with_pagination(&pagination);
        assert_eq!(
            basic_url.as_str(),
            "http://delta.io/?maxResults=7&pageToken=foo"
        )
    }
}
