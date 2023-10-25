use std::fmt::Debug;

use opendal::{layers::LoggingLayer, services, BlockingLister, Builder, Lister, Operator};
use serde::{Deserialize, Serialize};
use storage_dal_derive::StructuredDAL;

use crate::{build_key, StorageData, STRUCTURED_TREE_NAME};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct CloudConfig {
    pub service_type: String,
    pub access_key_id: String, //as Cos secret_id, as Azblob account_name
    pub secret_access_key: String, // as Cos secret_key, as Azblob account_key
    pub endpoint: String,
    pub bucket: String, // as Azblob container
    pub root: String,
}

#[derive(Debug, StructuredDAL)]
pub struct Cloud {
    pub op: Operator,
}

impl Default for Cloud {
    /// NOTE: Here, the storage engines are selected in a default specific order,
    /// this is a temporary solution to support multiple storage engines.
    fn default() -> Self {
        cfg_if::cfg_if! {
            if #[cfg(feature = "remote-s3")] {
                Self::init_s3(&CloudConfig::default())
            }
        }
    }
}

impl Cloud {
    fn new(ab: impl Builder) -> Self {
        let op = Operator::new(ab)
            .unwrap()
            // Init with logging layer enabled.
            .layer(LoggingLayer::default())
            .finish();
        Self { op }
    }

    #[cfg(feature = "remote-s3")]
    pub fn init_s3(config: &CloudConfig) -> Self {
        let mut builder = services::S3::default();
        builder.access_key_id(config.access_key_id.as_str());
        builder.secret_access_key(config.secret_access_key.as_str());
        builder.endpoint(config.endpoint.as_str());
        builder.bucket(config.bucket.as_str());
        builder.root(config.root.as_str());
        Self::new(builder)
    }
}
