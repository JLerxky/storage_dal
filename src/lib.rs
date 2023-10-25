#![forbid(unsafe_code)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    unused_crate_dependencies,
    clippy::missing_const_for_fn,
    unused_extern_crates
)]

pub mod cache;
pub mod cloud;
pub mod disk;
pub mod storage;

pub use storage_dal_derive::StorageData;

use std::fmt::Debug;

use serde::{Deserialize, Serialize};

pub trait StorageData: Debug + Clone + Default + for<'a> Deserialize<'a> + Serialize {
    fn name() -> String;
}

pub(crate) const SEQUENCE_TREE_NAME: &str = "SEQUENCE";
pub(crate) const STRUCTURED_TREE_NAME: &str = "STRUCTURED";

pub(crate) fn build_key<T: for<'a> Deserialize<'a> + StorageData>(key: &str) -> String {
    format!("{STRUCTURED_TREE_NAME}/{}/{}", T::name(), key)
}
