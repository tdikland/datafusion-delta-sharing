use std::sync::Arc;

use datafusion::prelude::*;
use datafusion_delta_sharing::{catalog::DeltaSharingCatalog, Profile};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let cfg = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(cfg);

    let profile = Profile::try_from_path("./examples/hack-dataset.share")?;
    let delta_sharing_catalog = DeltaSharingCatalog::try_new(profile, "delta_sharing").await?;
    ctx.register_catalog("delta_sharing", Arc::new(delta_sharing_catalog));

    ctx.sql("SELECT * FROM information_schema.tables;")
        .await?
        .show()
        .await?;

    Ok(())
}
