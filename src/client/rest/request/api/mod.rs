use super::error::RequestError;
use super::response;
use super::IntoRequest;

pub mod get_share;
pub mod list_schemas;
pub mod list_shares;
pub mod list_tables_in_schema;
pub mod list_tables_in_share;
pub mod query_table_changes;
pub mod query_table_data;
pub mod query_table_metadata;
pub mod query_table_version;

pub use get_share::GetShareRequest;
pub use list_schemas::ListSchemasRequest;
pub use list_shares::ListSharesRequest;
pub use list_tables_in_schema::ListTablesInSchemaRequest;
pub use list_tables_in_share::ListTablesInShareRequest;
pub use query_table_changes::QueryTableChangesRequest;
pub use query_table_data::QueryTableDataRequest;
pub use query_table_metadata::QueryTableMetadataRequest;
pub use query_table_version::QueryTableVersionRequest;
use serde::Serialize;

const DELTA_SHARING_CAPABILITIES_HEADERNAME: &str = "delta-sharing-capabilities";

fn make_path_and_query<T: Serialize>(path: String, query: T) -> Result<String, RequestError> {
    let mut path_and_query = path;
    let mut form_serializer = form_urlencoded::Serializer::new(String::new());
    let serializer = serde_urlencoded::Serializer::new(&mut form_serializer);
    query
        .serialize(serializer)
        .map_err(|e| RequestError::invalid_uri(e.to_string()))?;
    let ser_query_param = form_serializer.finish();
    if !ser_query_param.is_empty() {
        path_and_query.push('?');
        path_and_query.push_str(&ser_query_param);
    }
    Ok(path_and_query)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn path_and_query() {
        #[derive(serde::Serialize)]
        struct Query {
            a: i32,
            b: i32,
        }

        let path = "/path".to_string();
        let query = Query { a: 1, b: 2 };
        let result = make_path_and_query(path, query).unwrap();
        assert_eq!(result, "/path?a=1&b=2");
    }

    #[test]
    fn empty_path_empty_query() {
        let path = "".to_string();
        let query = ();
        let result = make_path_and_query(path, query).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn only_path() {
        let path = "/path".to_string();
        let query = ();
        let result = make_path_and_query(path, query).unwrap();
        assert_eq!(result, "/path");
    }

    #[test]
    fn only_query() {
        #[derive(serde::Serialize)]
        struct Query {
            a: i32,
            b: i32,
        }

        let path = "".to_string();
        let query = Query { a: 1, b: 2 };
        let result = make_path_and_query(path, query).unwrap();
        assert_eq!(result, "?a=1&b=2");
    }
}
