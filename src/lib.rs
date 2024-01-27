pub mod catalog;
pub mod client;
mod datasource;
pub mod error;

pub mod securable;

pub use client::profile::DeltaSharingProfile;
pub use datasource::DeltaSharingTableBuilder;
pub use securable::{Schema, Share, Table};

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tracing_test::traced_test;

    use crate::securable::Table;

    use super::*;

    #[traced_test]
    #[tokio::test]
    async fn it_works_df() {
        use datafusion::assert_batches_sorted_eq;
        use datafusion::prelude::*;

        let ctx = SessionContext::new();

        let endpoint = std::env::var("SHARING_ENDPOINT").unwrap();
        let token = std::env::var("SHARING_TOKEN").unwrap();
        let profile = DeltaSharingProfile::new(endpoint, token);
        let table = Table::new("tim_dikland_share", "sse", "config", None, None);

        let table = DeltaSharingTableBuilder::new(profile.clone(), table.clone())
            .with_profile(profile)
            .with_table(table)
            .build()
            .await
            .unwrap();

        ctx.register_table("demo", Arc::new(table)).unwrap();

        let df = ctx.sql("select * from demo").await.unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+----+-----+",
            "| id | val |",
            "+----+-----+",
            "| 1  | foo |",
            "| 2  | bar |",
            "+----+-----+",
        ];

        assert_batches_sorted_eq!(&expected, &actual);
    }
}
