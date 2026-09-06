//! Schema-aware ordinal codecs for USER/SHARED table stores.

use std::sync::Arc;

use kalamdb_commons::{
    ids::{SharedTableRowId, UserTableRowId},
    models::rows::{SharedTableRow, UserTableRow},
    schemas::TableDefinition,
};
use kalamdb_serialization::{
    decode_shared_row, decode_user_row, encode_shared_row, encode_user_row,
    storage_schema_from_table, StorageDataType, StorageField, StorageSchema,
};
use kalamdb_store::{EntityCodec, StorageError};

fn map_ser(err: kalamdb_serialization::SerializationError) -> StorageError {
    StorageError::SerializationError(err.to_string())
}

/// Build a storage schema from a catalog table definition.
pub fn storage_schema_for_table(table: &TableDefinition) -> Result<Arc<StorageSchema>, String> {
    storage_schema_from_table(table).map(Arc::new).map_err(|err| err.to_string())
}

/// Empty schema for DROP / cleanup paths that never encode or decode rows.
pub fn empty_storage_schema() -> Arc<StorageSchema> {
    Arc::new(StorageSchema::new(1, Vec::new()))
}

/// Build a storage schema from named live fields (tests and simple stores).
pub fn storage_schema_from_named_fields(
    fields: impl IntoIterator<Item = (impl Into<String>, StorageDataType)>,
) -> Arc<StorageSchema> {
    Arc::new(StorageSchema::new(
        1,
        fields
            .into_iter()
            .map(|(name, data_type)| StorageField::new(name, data_type))
            .collect(),
    ))
}

/// Ordinal USER row codec. Identity is reconstructed from [`UserTableRowId`].
#[derive(Debug, Clone)]
pub struct UserRowCodec {
    schema: Arc<StorageSchema>,
}

impl UserRowCodec {
    pub fn new(schema: Arc<StorageSchema>) -> Self {
        Self { schema }
    }
}

impl EntityCodec<UserTableRowId, UserTableRow> for UserRowCodec {
    fn encode(
        &self,
        _key: &UserTableRowId,
        entity: &UserTableRow,
    ) -> kalamdb_store::storage_trait::Result<Vec<u8>> {
        encode_user_row(entity, &self.schema)
            .map(|encoded| encoded.into_bytes())
            .map_err(map_ser)
    }

    fn decode(
        &self,
        key: &UserTableRowId,
        bytes: &[u8],
    ) -> kalamdb_store::storage_trait::Result<UserTableRow> {
        decode_user_row(bytes, &self.schema, key.user_id.clone(), key.seq).map_err(map_ser)
    }
}

/// Ordinal SHARED row codec. Identity is reconstructed from [`SharedTableRowId`].
#[derive(Debug, Clone)]
pub struct SharedRowCodec {
    schema: Arc<StorageSchema>,
}

impl SharedRowCodec {
    pub fn new(schema: Arc<StorageSchema>) -> Self {
        Self { schema }
    }
}

impl EntityCodec<SharedTableRowId, SharedTableRow> for SharedRowCodec {
    fn encode(
        &self,
        _key: &SharedTableRowId,
        entity: &SharedTableRow,
    ) -> kalamdb_store::storage_trait::Result<Vec<u8>> {
        encode_shared_row(entity._commit_seq, entity._deleted, &entity.fields, &self.schema)
            .map(|encoded| encoded.into_bytes())
            .map_err(map_ser)
    }

    fn decode(
        &self,
        key: &SharedTableRowId,
        bytes: &[u8],
    ) -> kalamdb_store::storage_trait::Result<SharedTableRow> {
        let (seq, commit_seq, deleted, fields) =
            decode_shared_row(bytes, &self.schema, *key).map_err(map_ser)?;
        Ok(SharedTableRow {
            _seq: seq,
            _commit_seq: commit_seq,
            _deleted: deleted,
            fields,
        })
    }
}
