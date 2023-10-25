#![forbid(unsafe_code)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    unused_crate_dependencies,
    clippy::missing_const_for_fn,
    unused_extern_crates
)]

use proc_macro::TokenStream;

use quote::quote;

#[proc_macro_derive(StorageData)]
pub fn storage_data_macro_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_storage_data_macro(&ast)
}

fn impl_storage_data_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl StorageData for #name {
            fn name() -> String {
                stringify!(#name).to_string()
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(StructuredDAL)]
pub fn structured_dal_macro_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_structured_dal_macro(&ast)
}

fn impl_structured_dal_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl #name {
            pub fn get<T: for<'a> Deserialize<'a> + StorageData>(&self, key: &str) -> Option<T> {
                match self.op.blocking().read(&build_key::<T>(key)) {
                    Ok(v) => bincode::deserialize(&v).ok(),
                    _ => None,
                }
            }

            pub async fn get_async<T: for<'a> Deserialize<'a> + StorageData>(
                &self,
                key: &str,
            ) -> Option<T> {
                match self.op.read(&build_key::<T>(key)).await {
                    Ok(v) => bincode::deserialize(&v).ok(),
                    _ => None,
                }
            }

            pub fn get_by_path<T: for<'a> Deserialize<'a>>(&self, path: &str) -> Option<T> {
                match self.op.blocking().read(path) {
                    Ok(v) => bincode::deserialize(&v).ok(),
                    _ => None,
                }
            }

            pub async fn get_async_by_path<T: for<'a> Deserialize<'a>>(&self, path: &str) -> Option<T> {
                match self.op.read(path).await {
                    Ok(v) => bincode::deserialize(&v).ok(),
                    _ => None,
                }
            }

            pub fn scan<T: StorageData>(&self) -> BlockingLister {
                let op = self.op.blocking();
                op.lister(&format!("{STRUCTURED_TREE_NAME}/{}/", T::name()))
                    .unwrap()
            }

            pub async fn scan_async<T: StorageData>(&self) -> Lister {
                self.op
                    .lister(&format!("{STRUCTURED_TREE_NAME}/{}/", T::name()))
                    .await
                    .unwrap()
            }

            pub fn insert<T: Serialize + StorageData>(&self, key: &str, value: T) -> Option<T> {
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

            pub async fn insert_async<T: Serialize + StorageData>(&self, key: &str, value: T) -> Option<T> {
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

            pub fn remove<T: Serialize + StorageData>(&self, key: &str) -> bool {
                self.op.blocking().delete(&build_key::<T>(key)).is_ok()
            }

            pub async fn remove_async<T: Serialize + StorageData>(&self, key: &str) -> bool {
                self.op.delete(&build_key::<T>(key)).await.is_ok()
            }
        }
    };
    gen.into()
}
