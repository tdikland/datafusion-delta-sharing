use std::str::FromStr;

#[derive(Debug)]
pub struct TableVersion(pub u64);

impl FromStr for TableVersion {
    // TODO: better error type
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>().map(TableVersion).map_err(|_| ())
    }
}
