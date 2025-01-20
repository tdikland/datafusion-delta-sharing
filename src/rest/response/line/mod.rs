use serde::Deserialize;

mod delta;
mod parquet;

pub use delta::DeltaResponseLine;
pub use parquet::ParquetResponseLine;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ResponseLine {
    Parquet(ParquetResponseLine),
    Delta(DeltaResponseLine),
}

impl ResponseLine {
    pub fn to_parquet(self) -> Option<ParquetResponseLine> {
        match self {
            ResponseLine::Parquet(p) => Some(p),
            _ => None,
        }
    }

    pub fn to_delta(self) -> Option<DeltaResponseLine> {
        match self {
            ResponseLine::Delta(d) => Some(d),
            _ => None,
        }
    }

    pub fn as_parquet(&self) -> Option<&ParquetResponseLine> {
        match self {
            ResponseLine::Parquet(p) => Some(p),
            _ => None,
        }
    }

    pub fn as_delta(&self) -> Option<&DeltaResponseLine> {
        match self {
            ResponseLine::Delta(d) => Some(d),
            _ => None,
        }
    }

    pub fn is_parquet(&self) -> bool {
        matches!(self, ResponseLine::Parquet(_))
    }

    pub fn is_delta(&self) -> bool {
        matches!(self, ResponseLine::Delta(_))
    }
}
