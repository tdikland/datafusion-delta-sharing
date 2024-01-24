pub struct DeltaSharingTableBuilder {
    endpoint: String,
    token: String,
    share_name: String,
    schema_name: String,
    table_name: String,
}

impl DeltaSharingTableBuilder {
    pub fn new() -> Self {
        let endpoint = std::env::var("SHARING_ENDPOINT").unwrap();
        let token = std::env::var("SHARING_TOKEN").unwrap();
        Self {
            endpoint,
            token,
            share_name: String::from("tim_dikland_share"),
            schema_name: String::from("sse"),
            table_name: String::from("config"),
        }
    }

    pub async fn build(self) -> DeltaSharingTable {
        let client = DeltaSharingClient::new(self.endpoint, self.token);

        let share = client::securable::Share::new(self.share_name, None);
        let schema = client::securable::Schema::new(share, self.schema_name, None);
        let table =
            client::securable::Table::new(schema, self.table_name, None, String::new(), None);

        let metadata = client.get_table_metadata(&table).await;

        DeltaSharingTable {
            client,
            table,
            metadata,
        }
    }
}
