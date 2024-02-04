# DataFusion Delta Sharing

This create provides the integration between Delta Sharing and DataFusion.

Delta Sharing is an open protocol for securely sharing large datasets. It
is a REST protocol that securely provides access to a part of a cloud
dataset stored on popular cloud storage systems like S3, ADLS, and GCS.

![Maintenance](https://img.shields.io/badge/maintenance-actively--developed-brightgreen.svg)
[![main](https://github.com/tdikland/datafusion-delta-sharing/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/tdikland/cloud-file-signer/actions/workflows/main.yml)
[![crates-io](https://img.shields.io/crates/v/datafusion-delta-sharing.svg)](https://crates.io/crates/datafusion-delta-sharing)
[![api-docs](https://docs.rs/datafusion-delta-sharing/badge.svg)](https://docs.rs/datafusion-delta-sharing)

## Example

An example of the integration is shown below:

```rust
use std::sync::Arc;
use datafusion::prelude::*;

use datafusion_delta_sharing::DeltaSharingTable;

let ctx = SessionContext::new();
let table = DeltaSharingTable::try_from_str("./path/to/profile.share#share.schema.table").await?;

ctx.register_table("demo", Arc::new(table))?;
ctx.sql("select * from demo").await?.show().await?;
```
