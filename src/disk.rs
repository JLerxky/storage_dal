use std::fmt::Debug;

use opendal::{layers::LoggingLayer, services, BlockingLister, Builder, Lister, Operator};
use serde::{Deserialize, Serialize};
use storage_dal_derive::StructuredDAL;

use crate::{build_key, StorageData, SEQUENCE_TREE_NAME, STRUCTURED_TREE_NAME};

#[derive(Debug, StructuredDAL)]
pub struct Disk {
    pub op: Operator,
}

impl Default for Disk {
    /// NOTE: Here, the storage engines are selected in a default specific order,
    /// this is a temporary solution to support multiple storage engines.
    fn default() -> Self {
        cfg_if::cfg_if! {
            if #[cfg(feature = "disk-sled")] {
                Self::init_sled("default.db")
            } else if #[cfg(feature = "disk-rocksdb")] {
                Self::init_rocksdb("default.db")
            } else if #[cfg(feature = "disk-redb")] {
                Self::init_redb("default.db")
            }
        }
    }
}

impl Disk {
    fn new(ab: impl Builder) -> Self {
        let op = Operator::new(ab)
            .unwrap()
            // Init with logging layer enabled.
            .layer(LoggingLayer::default())
            .finish();
        Self { op }
    }

    #[cfg(feature = "disk-sled")]
    pub fn init_sled(path: &str) -> Self {
        let mut builder = services::Sled::default();
        builder.datadir(path);
        Self::new(builder)
    }

    #[cfg(feature = "disk-redb")]
    pub fn init_redb(path: &str) -> Self {
        let mut builder = services::Redb::default();
        builder.datadir(path);
        builder.table("default");
        Self::new(builder)
    }

    #[cfg(feature = "disk-rocksdb")]
    pub fn init_rocksdb(path: &str) -> Self {
        let mut builder = services::Rocksdb::default();
        builder.datadir(path);
        Self::new(builder)
    }
}

// SEQUENCE
impl Disk {
    pub fn next(&self, name: &str) -> u32 {
        let path = format!("{}/{}", SEQUENCE_TREE_NAME, name);
        let op = self.op.blocking();
        match op.read(&path).ok().and_then(|v| match v.try_into() {
            Ok(v) => Some(u32::from_be_bytes(v)),
            Err(_) => None,
        }) {
            Some(next) => {
                if let Ok(()) = op.write(&path, (next + 1).to_be_bytes().to_vec()) {
                    next + 1
                } else {
                    0
                }
            }
            None => {
                if let Ok(()) = op.write(&path, 1u32.to_be_bytes().to_vec()) {
                    1
                } else {
                    0
                }
            }
        }
    }

    pub async fn next_async(&self, name: &str) -> u32 {
        let path = format!("{}/{}", SEQUENCE_TREE_NAME, name);
        match self
            .op
            .read(&path)
            .await
            .ok()
            .and_then(|v| match v.try_into() {
                Ok(v) => Some(u32::from_be_bytes(v)),
                Err(_) => None,
            }) {
            Some(next) => {
                if let Ok(()) = self
                    .op
                    .write(&path, (next + 1).to_be_bytes().to_vec())
                    .await
                {
                    next + 1
                } else {
                    0
                }
            }
            None => {
                if let Ok(()) = self.op.write(&path, 1u32.to_be_bytes().to_vec()).await {
                    1
                } else {
                    0
                }
            }
        }
    }

    pub fn current(&self, name: &str) -> u32 {
        if let Ok(v) = self
            .op
            .blocking()
            .read(&(SEQUENCE_TREE_NAME.to_string() + "/" + name))
        {
            if let Ok(v) = v.try_into() {
                return u32::from_be_bytes(v);
            }
        }
        0
    }

    pub async fn current_async(&self, name: &str) -> u32 {
        if let Ok(v) = self
            .op
            .read(&(SEQUENCE_TREE_NAME.to_string() + "/" + name))
            .await
        {
            if let Ok(v) = v.try_into() {
                return u32::from_be_bytes(v);
            }
        }
        0
    }
}

#[test]
fn sequence() {
    let store = Disk::default();
    println!("{}", store.current("test"));
    for _ in 0..100 {
        println!("{}", store.next("test"));
    }
}

#[test]
fn list() {
    #[derive(StorageData, Debug, Clone, Default, Deserialize, Serialize)]
    struct Test {
        name: String,
    }
    #[derive(StorageData, Debug, Clone, Default, Deserialize, Serialize)]
    struct Test1 {
        name: String,
    }
    let store = Disk::default();
    for i in 0..5 {
        store
            .insert(
                &i.to_string(),
                Test {
                    name: i.to_string(),
                },
            )
            .unwrap();
        store
            .insert(
                &i.to_string(),
                Test1 {
                    name: i.to_string(),
                },
            )
            .unwrap();
    }
    for i in store.scan::<Test>() {
        println!("{:?}", i);
        println!(
            "{:?}",
            store.get_by_path::<Test>(i.unwrap().path()).unwrap()
        );
    }
}
