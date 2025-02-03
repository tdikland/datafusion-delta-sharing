use std::str::FromStr;

#[derive(Debug, Clone, Copy)]
pub struct TableVersionNumber(pub u64);

impl FromStr for TableVersionNumber {
    // TODO: better error type
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>().map(TableVersionNumber).map_err(|_| ())
    }
}
