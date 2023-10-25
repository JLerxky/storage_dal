use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::{cache::Cache, cloud::Cloud, disk::Disk, StorageData};

#[derive(Debug, Default)]
pub struct Storage {
    pub cache: Cache,
    pub disk: Disk,
    pub cloud: Option<Cloud>,
}

impl Storage {
    pub const fn new(cache: Cache, disk: Disk, cloud: Option<Cloud>) -> Self {
        Self { cache, disk, cloud }
    }

    pub fn get<T: for<'a> Deserialize<'a> + StorageData>(&self, key: &str) -> Option<T> {
        if let Some(v) = self.cache.get::<T>(key) {
            return Some(v);
        }

        if let Some(v) = self.disk.get::<T>(key) {
            self.cache.insert(key, v.clone());
            return Some(v);
        }

        if let Some(cloud) = &self.cloud {
            if let Some(v) = cloud.get::<T>(key) {
                self.disk.insert(key, v.clone());
                self.cache.insert(key, v.clone());
                return Some(v);
            }
        }

        None
    }

    pub async fn get_async<T: for<'a> Deserialize<'a> + StorageData>(
        &self,
        key: &str,
    ) -> Option<T> {
        if let Some(v) = self.cache.get_async::<T>(key).await {
            return Some(v);
        }

        if let Some(v) = self.disk.get_async::<T>(key).await {
            self.cache.insert_async(key, v.clone()).await;
            return Some(v);
        }

        if let Some(cloud) = &self.cloud {
            if let Some(v) = cloud.get_async::<T>(key).await {
                self.disk.insert_async(key, v.clone()).await;
                self.cache.insert_async(key, v.clone()).await;
                return Some(v);
            }
        }

        None
    }

    pub fn insert<T: Serialize + StorageData>(&self, key: &str, value: T) -> Option<T> {
        if self.cache.insert(key, value.clone()).is_some() {
            if let Some(v) = self.disk.insert(key, value) {
                return Some(v);
            }
        }

        None
    }

    pub async fn insert_async<T: Serialize + StorageData>(&self, key: &str, value: T) -> Option<T> {
        if self.cache.insert_async(key, value.clone()).await.is_some() {
            if let Some(v) = self.disk.insert_async(key, value).await {
                return Some(v);
            }
        }

        None
    }

    pub fn remove<T: Serialize + StorageData>(&self, key: &str) -> bool {
        if self.cache.remove::<T>(key) && self.disk.remove::<T>(key) {
            if let Some(cloud) = &self.cloud {
                if cloud.remove::<T>(key) {
                    return true;
                }
            } else {
                return true;
            }
        }

        false
    }

    pub async fn remove_async<T: Serialize + StorageData>(&self, key: &str) -> bool {
        if self.cache.remove_async::<T>(key).await && self.disk.remove_async::<T>(key).await {
            if let Some(cloud) = &self.cloud {
                if cloud.remove_async::<T>(key).await {
                    return true;
                }
            } else {
                return true;
            }
        }

        false
    }
}

// SEQUENCE
impl Storage {
    pub fn next(&self, name: &str) -> u32 {
        self.disk.next(name)
    }

    pub async fn next_async(&self, name: &str) -> u32 {
        self.disk.next_async(name).await
    }

    pub fn current(&self, name: &str) -> u32 {
        self.disk.current(name)
    }

    pub async fn current_async(&self, name: &str) -> u32 {
        self.disk.current_async(name).await
    }
}

// TODO expiration policies
impl Storage {}

#[test]
fn sequence() {
    let store = Storage::default();
    println!("{}", store.current("test"));
    for _ in 0..100 {
        println!("{}", store.next("test"));
    }
}

#[test]
fn storage() {
    #[derive(StorageData, Debug, Clone, Default, Deserialize, Serialize)]
    struct Test {
        pub name: String,
        pub age: u32,
    }

    let store = Storage::default();
    let test = Test {
        name: "test".to_string(),
        age: 10,
    };
    store.insert("test", test.clone());
    println!("{:?}", store.get::<Test>("test"));
    println!("{:?}", store.get::<Test>("test"));
}

#[test]
fn cache_control() {
    let store = Cache::default();
    store
        .op
        .blocking()
        .write_with("path", b"bs".to_vec())
        .cache_control("max-age=1")
        .call()
        .unwrap();
    println!("{:?}", store.op.blocking().read("path"));
    std::thread::sleep(std::time::Duration::from_secs(2));
    println!("{:?}", store.op.blocking().read("path"));
}
