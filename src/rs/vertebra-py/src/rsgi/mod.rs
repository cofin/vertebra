//! RSGI v1.6 protocol and scope module.

pub mod http;
pub mod scope;

pub use http::{
    RsgiHttpProtocol, RsgiReadyAwaitable, RsgiTransport, drive_rsgi_batch, get_rsgi_done,
};
pub use scope::RsgiScope;
