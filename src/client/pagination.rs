//! Pagination support for the client.

use reqwest::Url;

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
    pub fn new(max_results: Option<u32>, page_token: Option<String>, is_start: bool) -> Self {
        Self {
            max_results,
            page_token,
            is_start,
        }
    }

    /// Create a new Pagination for the first page
    pub fn start(max_results: Option<u32>, page_token: Option<String>) -> Self {
        Self::new(max_results, page_token, true)
    }

    /// Set the next page token
    pub fn set_next_token(&mut self, token: Option<String>) {
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
    fn set_pagination(&mut self, pagination: &Pagination);
}

impl PaginationExt for Url {
    fn set_pagination(&mut self, pagination: &Pagination) {
        if pagination.max_results.is_none() && pagination.page_token.is_none() {
            return;
        }

        let mut query_pairs = self.query_pairs_mut();
        if let Some(m) = pagination.max_results {
            query_pairs.append_pair(QUERY_PARAM_MAX_RESULTS, &m.to_string());
        };
        if let Some(token) = &pagination.page_token {
            query_pairs.append_pair(QUERY_PARAM_PAGE_TOKEN, token);
        };
        drop(query_pairs);
    }
}
