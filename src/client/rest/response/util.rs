use http::{header::CONTENT_TYPE, HeaderMap};

use super::{line::ResponseLine, ParseResponseError, DELTA_TABLE_VERSION_HEADER};

pub fn extract_delta_table_version(headers: &HeaderMap) -> Result<u64, ParseResponseError> {
    let table_version_number = headers
        .get(DELTA_TABLE_VERSION_HEADER)
        .ok_or(ParseResponseError::MissingRequiredHeader {
            header_name: DELTA_TABLE_VERSION_HEADER.to_string(),
        })?
        .to_str()
        .map_err(|e| ParseResponseError::InvalidHeader {
            header_name: DELTA_TABLE_VERSION_HEADER.to_string(),
            err: e.to_string(),
        })?
        .parse::<u64>()
        .map_err(|e| ParseResponseError::InvalidHeader {
            header_name: DELTA_TABLE_VERSION_HEADER.to_string(),
            err: e.to_string(),
        })?;
    Ok(table_version_number)
}

pub fn has_json_content_type(headers: &HeaderMap) -> bool {
    let content_type = if let Some(content_type) = headers.get(CONTENT_TYPE) {
        content_type
    } else {
        return false;
    };

    let content_type = if let Ok(content_type) = content_type.to_str() {
        content_type
    } else {
        return false;
    };

    let mime = if let Ok(mime) = content_type.parse::<mime::Mime>() {
        mime
    } else {
        return false;
    };

    let is_json_content_type = mime.type_() == "application"
        && (mime.subtype() == "json" || mime.suffix().is_some_and(|name| name == "json"));

    is_json_content_type
}

pub fn has_ndjson_content_type(headers: &HeaderMap) -> bool {
    let content_type = if let Some(content_type) = headers.get(CONTENT_TYPE) {
        content_type
    } else {
        return false;
    };

    let content_type = if let Ok(content_type) = content_type.to_str() {
        content_type
    } else {
        return false;
    };

    let mime = if let Ok(mime) = content_type.parse::<mime::Mime>() {
        mime
    } else {
        return false;
    };

    let is_json_content_type = mime.type_() == "application"
        && (mime.subtype() == "x-ndjson" || mime.suffix().is_some_and(|name| name == "x-ndjson"));

    is_json_content_type
}

pub fn first_line_is_protocol(lines: &[ResponseLine]) -> bool {
    match lines.first() {
        Some(ResponseLine::Parquet(p)) => p.is_protocol(),
        Some(ResponseLine::Delta(d)) => d.is_protocol(),
        None => false,
    }
}

pub fn second_line_is_metadata(lines: &[ResponseLine]) -> bool {
    match lines.iter().nth(1) {
        Some(ResponseLine::Parquet(p)) => p.is_metadata(),
        Some(ResponseLine::Delta(d)) => d.is_metadata(),
        None => false,
    }
}
