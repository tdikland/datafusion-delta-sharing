pub struct DeltaSharingClientConfig {
    response_format: Vec<ResponseFormat>,
    reader_features: Vec<DeltaReaderFeature>,
}

// TODO: questionable choice, maybe just plain strings?
// Or leave Other(String) in there?
pub enum DeltaReaderFeature {
    DeletionVectors,
}

pub enum ResponseFormat {
    Parquet,
    Delta,
}


