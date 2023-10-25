use opendal::{layers::LoggingLayer, BlockingLister, Builder, Lister, Operator};
use serde::{Deserialize, Serialize};
use storage_dal_derive::StructuredDAL;

use crate::{build_key, StorageData, STRUCTURED_TREE_NAME};

#[derive(Debug, StructuredDAL)]
pub struct Cache {
    pub op: Operator,
}

impl Default for Cache {
    /// NOTE: Here, the storage engines are selected in a default specific order,
    /// this is a temporary solution to support multiple storage engines.
    fn default() -> Self {
        cfg_if::cfg_if! {
            if #[cfg(feature = "cache-moka")] {
                Self::init_moka()
            }
        }
    }
}

impl Cache {
    fn new(ab: impl Builder) -> Self {
        let op = Operator::new(ab)
            .unwrap()
            // Init with logging layer enabled.
            .layer(LoggingLayer::default())
            .finish();
        Self { op }
    }

    #[cfg(feature = "cache-moka")]
    pub fn init_moka() -> Self {
        use opendal::services::Moka;

        let mut builder = Moka::default();
        builder.thread_pool_enabled(true);
        Self::new(builder)
    }
}
