//! Transport-agnostic request, response, and executor contracts.

pub mod request;
pub mod response;
pub mod session;
pub mod traits;

pub use request::{DeleteRequest, InsertRequest, ScanRequest, UpdateRequest};
pub use response::{MutationResponse, ScanResponse};
pub use session::{RemoteSessionContext, TenantContext};
pub use traits::KalamBackendExecutor;
