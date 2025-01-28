use serde::Deserialize;

use crate::model::action::parquet::{Add, Cdf, File, Metadata, Protocol, Remove};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ParquetResponseLine {
    Protocol(Protocol),
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    File(File),
    Add(Add),
    Cdf(Cdf),
    Remove(Remove),
}

impl ParquetResponseLine {
    pub fn to_protocol(self) -> Option<Protocol> {
        match self {
            ParquetResponseLine::Protocol(p) => Some(p),
            _ => None,
        }
    }

    pub fn to_metadata(self) -> Option<Metadata> {
        match self {
            ParquetResponseLine::Metadata(m) => Some(m),
            _ => None,
        }
    }

    pub fn to_file(self) -> Option<File> {
        match self {
            ParquetResponseLine::File(f) => Some(f),
            _ => None,
        }
    }

    pub fn to_add(self) -> Option<Add> {
        match self {
            ParquetResponseLine::Add(a) => Some(a),
            _ => None,
        }
    }

    pub fn to_cdf(self) -> Option<Cdf> {
        match self {
            ParquetResponseLine::Cdf(c) => Some(c),
            _ => None,
        }
    }

    pub fn to_remove(self) -> Option<Remove> {
        match self {
            ParquetResponseLine::Remove(r) => Some(r),
            _ => None,
        }
    }

    pub fn is_protocol(&self) -> bool {
        matches!(self, ParquetResponseLine::Protocol(_))
    }

    pub fn is_metadata(&self) -> bool {
        matches!(self, ParquetResponseLine::Metadata(_))
    }

    pub fn is_file(&self) -> bool {
        matches!(self, ParquetResponseLine::File(_))
    }

    pub fn is_add(&self) -> bool {
        matches!(self, ParquetResponseLine::Add(_))
    }

    pub fn is_cdf(&self) -> bool {
        matches!(self, ParquetResponseLine::Cdf(_))
    }

    pub fn is_remove(&self) -> bool {
        matches!(self, ParquetResponseLine::Remove(_))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn deserialize_protocol_line() {
        let line = r#"{"protocol":{"minReaderVersion":1}}"#;
        let response: ParquetResponseLine = serde_json::from_str(line).unwrap();
        assert!(response.is_protocol());
    }

    #[test]
    fn doc() {
        let ex = r#"{"protocol":{"minReaderVersion":1}}
{"metaData":{"id":"f8d5c169-3d01-4ca3-ad9e-7dc3355aedb2","format":{"provider":"parquet"},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["date"]}}
{"file":{"url":"https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-8b0086f2-7b27-4935-ac5a-8ed6215a6640.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010516Z&X-Amz-SignedHeaders=host&X-Amz-Expires=900&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=97b6762cfd8e4d7e94b9d707eff3faf266974f6e7030095c1d4a66350cfd892e","id":"8b0086f2-7b27-4935-ac5a-8ed6215a6640","partitionValues":{"date":"2021-04-28"},"size":573,"stats":"{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:57.955Z\"},\"nullCount\":{\"eventTime\":0}}"}}
{"file":{"url":"https://<s3-bucket-name>.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20210501T010516Z&X-Amz-SignedHeaders=host&X-Amz-Expires=899&X-Amz-Credential=AKIAISZRDL4Q4Q7AIONA%2F20210501%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Signature=0f7acecba5df7652457164533a58004936586186c56425d9d53c52db574f6b62","id":"591723a8-6a27-4240-a90e-57426f4736d2","partitionValues":{"date":"2021-04-28"},"size":573,"stats":"{\"numRecords\":1,\"minValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"maxValues\":{\"eventTime\":\"2021-04-28T23:33:48.719Z\"},\"nullCount\":{\"eventTime\":0}}"}}"#;

        let lines: Vec<_> = serde_json::Deserializer::from_slice(ex.as_bytes())
            .into_iter::<ParquetResponseLine>()
            .collect();
        println!("{:?}", lines);
        assert_eq!(lines.len(), 4);

        assert!(matches!(&lines[0], Ok(ParquetResponseLine::Protocol(_))));
        assert!(matches!(&lines[1], Ok(ParquetResponseLine::Metadata(_))));
        assert!(matches!(&lines[2], Ok(ParquetResponseLine::File(_))));
        assert!(matches!(&lines[3], Ok(ParquetResponseLine::File(_))));
    }
}
