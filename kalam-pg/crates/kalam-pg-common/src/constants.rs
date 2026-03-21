/// Virtual user-id system column injected by the FDW layer.
pub const USER_ID_COLUMN: &str = "_userid";

/// Session GUC used by the PostgreSQL extension to scope user and stream tables.
pub const USER_ID_GUC: &str = "kalam.user_id";

/// Virtual sequence/version system column injected by the FDW layer.
pub const SEQ_COLUMN: &str = "_seq";

/// Virtual soft-delete system column injected by the FDW layer.
pub const DELETED_COLUMN: &str = "_deleted";
