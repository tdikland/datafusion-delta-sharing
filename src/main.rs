use std::sync::Arc;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use datafusion::{assert_batches_sorted_eq, prelude::*};
use datafusion_delta_sharing::{securable::Table, DeltaSharingTableBuilder};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let ctx = SessionContext::new();
    let t = Table::new("tim_dikland_share", "sse", "config", None, None);
    let table = DeltaSharingTableBuilder::new(&t).build().await;

    ctx.register_table("demo", Arc::new(table)).unwrap();

    let df = ctx.sql("select * from demo").await.unwrap();
    let actual = df.collect().await.unwrap();

    assert_batches_sorted_eq!(
        &vec![
            "+----+-----+",
            "| id | val |",
            "+----+-----+",
            "| 1  | foo |",
            "| 2  | bar |",
            "+----+-----+"
        ],
        &actual
    );

    println!("{actual:?}");
}
