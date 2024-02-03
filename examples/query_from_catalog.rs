use std::sync::Arc;

use datafusion::prelude::*;
use datafusion_delta_sharing::{DeltaSharingCatalogList, DeltaSharingProfile};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let cfg = SessionConfig::new().with_information_schema(true);
    let mut ctx = SessionContext::new_with_config(cfg);

    let profile = DeltaSharingProfile::from_path("./examples/open-datasets.share")?;
    let delta_sharing_catalog = DeltaSharingCatalogList::new(profile).await;
    ctx.register_catalog_list(Arc::new(delta_sharing_catalog));

    ctx.sql("SELECT iso_code, continent, location, date, total_cases FROM delta_sharing.default.`owid-covid-data` WHERE total_cases > 5.0 LIMIT 20;")
        .await?
        .show()
        .await?;

    Ok(())
}
