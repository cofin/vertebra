//! napi-rs bindings for Vertebra (Bun/Node.js).

use napi_derive::napi;

#[napi]
pub fn get_version() -> String {
    vertebra_core::VERSION.to_string()
}
