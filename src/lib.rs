#![forbid(unsafe_code)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    unused_crate_dependencies,
    clippy::missing_const_for_fn,
    unused_extern_crates
)]

pub use storage_dal_derive::StorageData;

use std::fmt::Debug;

use opendal::{layers::LoggingLayer, services, BlockingLister, Builder, Lister, Operator};
use serde::{Deserialize, Serialize};

const SEQUENCE_TREE_NAME: &str = "SEQUENCE";

pub trait StorageData: Debug + Clone + Default + for<'a> Deserialize<'a> + Serialize {
    fn name() -> String;
}

#[derive(Debug)]
pub struct Storage {
    op: Operator,
}

impl Default for Storage {
    /// NOTE: Here, the storage engines are selected in a default specific order,
    /// this is a temporary solution to support multiple storage engines.
    fn default() -> Self {
        cfg_if::cfg_if! {
            if #[cfg(feature = "moka")] {
                Self::init_moka()
            }else if #[cfg(feature = "sled")] {
                Self::init_sled("default.db")
            } else if #[cfg(feature = "rocksdb")] {
                Self::init_rocksdb("default.db")
            } else if #[cfg(feature = "redb")] {
                Self::init_redb("default.db")
            }
        }
    }
}

impl Storage {
    fn new(ab: impl Builder) -> Self {
        let op = Operator::new(ab)
            .unwrap()
            // Init with logging layer enabled.
            .layer(LoggingLayer::default())
            .finish();
        Self { op }
    }

    #[cfg(feature = "moka")]
    pub fn init_moka() -> Self {
        let mut builder = services::Moka::default();
        builder.thread_pool_enabled(true);
        Self::new(builder)
    }

    #[cfg(feature = "sled")]
    pub fn init_sled(path: &str) -> Self {
        let mut builder = services::Sled::default();
        builder.datadir(path);
        Self::new(builder)
    }

    #[cfg(feature = "redb")]
    pub fn init_redb(path: &str) -> Self {
        let mut builder = services::Redb::default();
        builder.datadir(path);
        builder.table("default");
        Self::new(builder)
    }

    #[cfg(feature = "rocksdb")]
    pub fn init_rocksdb(path: &str) -> Self {
        let mut builder = services::Rocksdb::default();
        builder.datadir(path);
        Self::new(builder)
    }

    pub fn get<T: for<'a> Deserialize<'a> + StorageData>(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Option<T> {
        match self.op.blocking().read(&build_key::<T>(key)) {
            Ok(v) => bincode::deserialize(&v).ok(),
            _ => None,
        }
    }

    pub async fn get_async<T: for<'a> Deserialize<'a> + StorageData>(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Option<T> {
        match self.op.read(&build_key::<T>(key)).await {
            Ok(v) => bincode::deserialize(&v).ok(),
            _ => None,
        }
    }

    pub fn read<T: for<'a> Deserialize<'a> + StorageData>(&self, path: &str) -> Option<T> {
        match self.op.blocking().read(path) {
            Ok(v) => bincode::deserialize(&v).ok(),
            _ => None,
        }
    }

    pub async fn read_async<T: for<'a> Deserialize<'a> + StorageData>(
        &self,
        path: &str,
    ) -> Option<T> {
        match self.op.read(path).await {
            Ok(v) => bincode::deserialize(&v).ok(),
            _ => None,
        }
    }

    pub fn scan<T: for<'a> Deserialize<'a> + StorageData>(&self) -> BlockingLister {
        let op = self.op.blocking();
        op.lister(&(T::name() + "/")).unwrap()
    }

    pub async fn scan_async<T: for<'a> Deserialize<'a> + StorageData>(&self) -> Lister {
        self.op.lister(&(T::name() + "/")).await.unwrap()
    }

    pub fn insert<T: Serialize + StorageData>(&self, key: impl AsRef<[u8]>, value: T) -> Option<T> {
        if self
            .op
            .blocking()
            .write(&build_key::<T>(key), bincode::serialize(&value).unwrap())
            .is_ok()
        {
            Some(value)
        } else {
            None
        }
    }

    pub async fn insert_async<T: Serialize + StorageData>(
        &self,
        key: impl AsRef<[u8]>,
        value: T,
    ) -> Option<T> {
        if self
            .op
            .write(&build_key::<T>(key), bincode::serialize(&value).unwrap())
            .await
            .is_ok()
        {
            Some(value)
        } else {
            None
        }
    }

    pub fn remove<T: Serialize + StorageData>(&self, key: impl AsRef<[u8]>) -> bool {
        self.op.blocking().delete(&build_key::<T>(key)).is_ok()
    }

    pub async fn remove_async<T: Serialize + StorageData>(&self, key: impl AsRef<[u8]>) -> bool {
        self.op.delete(&build_key::<T>(key)).await.is_ok()
    }

    pub fn next(&self, name: &str) -> u32 {
        let path = format!("{}/{}", SEQUENCE_TREE_NAME, name);
        let op = self.op.blocking();
        match op
            .read(&path)
            .ok()
            .and_then(|v| bincode::deserialize::<u32>(&v).ok())
        {
            Some(next) => {
                if let Ok(()) = op.write(&path, bincode::serialize(&(next + 1)).unwrap()) {
                    next + 1
                } else {
                    0
                }
            }
            None => {
                if let Ok(()) = op.write(&path, bincode::serialize(&1).unwrap()) {
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
            .and_then(|v| bincode::deserialize::<u32>(&v).ok())
        {
            Some(next) => {
                if let Ok(()) = self
                    .op
                    .write(&path, bincode::serialize(&(next + 1)).unwrap())
                    .await
                {
                    next + 1
                } else {
                    0
                }
            }
            None => {
                if let Ok(()) = self.op.write(&path, bincode::serialize(&1).unwrap()).await {
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
            if let Ok(next) = bincode::deserialize(&v[..4]) {
                return next;
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
            if let Ok(next) = bincode::deserialize(&v[..4]) {
                return next;
            }
        }
        0
    }
}

fn build_key<T: for<'a> Deserialize<'a> + StorageData>(key: impl AsRef<[u8]>) -> String {
    T::name() + "/" + &String::from_utf8(key.as_ref().to_vec()).unwrap()
}

#[test]
fn sequence() {
    let store = Storage::default();
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
    let store = Storage::default();
    for i in 0..5 {
        store.insert(
            i.to_string(),
            Test {
                name: i.to_string(),
            },
        );
        store.insert(
            i.to_string(),
            Test1 {
                name: i.to_string(),
            },
        );
    }
    for i in store.scan::<Test>() {
        println!("{:?}", store.read::<Test>(i.unwrap().path()).unwrap());
    }
}
