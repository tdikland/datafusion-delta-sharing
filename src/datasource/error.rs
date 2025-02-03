#[derive(Debug)]
pub enum DataSourceError {
    ConnectionString(String),
    Profile(String),
    Client(String),
    ParseTableSchema(String),
}
