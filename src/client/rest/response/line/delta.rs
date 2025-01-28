use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DeltaResponseLine {}

impl DeltaResponseLine {
    pub fn is_protocol(&self) -> bool {
        // TODO implement properly
        true
    }

    pub fn is_metadata(&self) -> bool {
        // TODO implement properly
        true
    }
}
