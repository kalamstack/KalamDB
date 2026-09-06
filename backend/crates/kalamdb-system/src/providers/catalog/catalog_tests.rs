use std::sync::Arc;

use kalamdb_commons::{
    models::{
        ArtifactId, CatalogTypeKind, FunctionModuleId, FunctionRevisionId, FunctionRuntime,
        NamespaceId, RoutineGrantId, RoutineGrantee, RoutineId, RoutineParameterId,
        RoutineSecurityMode, TableId, TypeFieldId, TypeId, UserId,
    },
    StorageKey, SystemTable,
};
use kalamdb_store::{entity_store::EntityStore, test_utils::InMemoryBackend};
use serde_json::Value;

use super::{
    models::{
        CatalogFunctionArtifact, CatalogFunctionModule, CatalogFunctionRevision, CatalogRoutine,
        CatalogRoutineGrant, CatalogRoutineParameter, CatalogType, CatalogTypeField,
    },
    ActivateFunctionOutcome, CatalogStores, TypesTableProvider,
};
use crate::system_row_mapper::{model_to_system_row, system_row_to_model};

fn stores() -> CatalogStores {
    CatalogStores::new(Arc::new(InMemoryBackend::new()))
}

fn chat_ns() -> NamespaceId {
    NamespaceId::new("chat")
}

fn implicit_row_type() -> CatalogType {
    CatalogType {
        type_id:        TypeId::from_parts(Some(&chat_ns()), "message"),
        namespace_id:   chat_ns(),
        name:           "message".to_string(),
        kind:           CatalogTypeKind::ImplicitTableRow,
        table_id:       Some(TableId::from_strings("chat", "messages")),
        source_type_id: None,
        comment:        Some("implicit row".to_string()),
    }
}

fn address_type() -> CatalogType {
    CatalogType {
        type_id:        TypeId::from_parts(Some(&chat_ns()), "address"),
        namespace_id:   chat_ns(),
        name:           "address".to_string(),
        kind:           CatalogTypeKind::Composite,
        table_id:       None,
        source_type_id: None,
        comment:        None,
    }
}

fn street_field() -> CatalogTypeField {
    CatalogTypeField {
        type_field_id: TypeFieldId::new(&TypeId::from_parts(Some(&chat_ns()), "address"), "street")
            .unwrap(),
        type_id:       TypeId::from_parts(Some(&chat_ns()), "address"),
        name:          "street".to_string(),
        ordinal:       0,
        field_type_id: None,
        type_name:     "text".to_string(),
        is_array:      false,
        not_null:      true,
        nonempty:      false,
        data_type:     Some(kalamdb_commons::KalamDataType::Text),
    }
}

fn nested_location_field() -> CatalogTypeField {
    CatalogTypeField {
        type_field_id: TypeFieldId::new(
            &TypeId::from_parts(Some(&chat_ns()), "message"),
            "location",
        )
        .unwrap(),
        type_id:       TypeId::from_parts(Some(&chat_ns()), "message"),
        name:          "location".to_string(),
        ordinal:       0,
        field_type_id: Some(TypeId::from_parts(Some(&chat_ns()), "address")),
        type_name:     "chat.address".to_string(),
        is_array:      false,
        not_null:      false,
        nonempty:      false,
        data_type:     None,
    }
}

fn create_message_routine() -> CatalogRoutine {
    CatalogRoutine {
        routine_id:       RoutineId::from_parts(Some(&chat_ns()), "create_message"),
        namespace_id:     chat_ns(),
        name:             "create_message".to_string(),
        owner:            UserId::new("root"),
        security:         RoutineSecurityMode::Definer,
        language:         Some("typescript".to_string()),
        body:             None,
        return_type_id:   Some(TypeId::from_parts(Some(&chat_ns()), "message")),
        return_type_name: Some("chat.message".to_string()),
        return_is_array:  false,
        return_not_null:  true,
        comment:          None,
        return_data_type: None,
    }
}

#[test]
fn catalog_objects_persist_through_entity_store_as_kobj() {
    let stores = stores();
    let catalog_type = implicit_row_type();
    stores.upsert_type(catalog_type.clone()).unwrap();

    let raw = stores
        .types
        .backend()
        .get(&stores.types.partition(), &catalog_type.type_id.storage_key())
        .unwrap()
        .expect("persisted type bytes");

    assert_eq!(&raw[..4], b"KOBJ");
    assert!(serde_json::from_slice::<Value>(&raw).is_err());
    assert_eq!(stores.get_type(&catalog_type.type_id).unwrap(), Some(catalog_type));
}

#[test]
fn implicit_row_type_and_alias_have_catalog_relationships() {
    let stores = stores();
    stores.upsert_type(implicit_row_type()).unwrap();

    let alias = CatalogType {
        type_id:        TypeId::from_parts(Some(&chat_ns()), "Message"),
        namespace_id:   chat_ns(),
        name:           "Message".to_string(),
        kind:           CatalogTypeKind::RowAlias,
        table_id:       None,
        source_type_id: Some(TypeId::from_parts(Some(&chat_ns()), "message")),
        comment:        None,
    };
    stores.upsert_type(alias.clone()).unwrap();

    let loaded_alias = stores.get_type(&alias.type_id).unwrap().unwrap();
    assert_eq!(loaded_alias.kind, CatalogTypeKind::RowAlias);
    assert_eq!(loaded_alias.source_type_id.as_ref().map(TypeId::as_str), Some("chat.message"));
    let loaded_implicit = stores
        .get_type(&TypeId::from_parts(Some(&chat_ns()), "message"))
        .unwrap()
        .unwrap();
    assert_eq!(
        loaded_implicit.table_id.as_ref().map(TableId::full_name),
        Some("chat.messages".to_string())
    );
}

#[test]
fn nested_type_field_stores_type_id_not_a_struct_string() {
    let stores = stores();
    stores.upsert_type(address_type()).unwrap();
    stores.upsert_type_field(street_field()).unwrap();
    stores.upsert_type(implicit_row_type()).unwrap();
    stores.upsert_type_field(nested_location_field()).unwrap();

    let fields = stores
        .list_type_fields(&TypeId::from_parts(Some(&chat_ns()), "message"))
        .unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].field_type_id.as_ref().map(TypeId::as_str), Some("chat.address"));
    assert_eq!(fields[0].data_type, None);
    assert_ne!(fields[0].type_name, r#"STRUCT("street" TEXT)"#);

    let address_fields = stores
        .list_type_fields(&TypeId::from_parts(Some(&chat_ns()), "address"))
        .unwrap();
    assert_eq!(address_fields[0].data_type, Some(kalamdb_commons::KalamDataType::Text));
}

#[test]
fn additive_nullable_comment_decodes_as_null() {
    let stores = stores();
    let mut catalog_type = implicit_row_type();
    catalog_type.comment = None;
    stores.upsert_type(catalog_type.clone()).unwrap();

    let decoded = stores.get_type(&catalog_type.type_id).unwrap().unwrap();
    assert_eq!(decoded.comment, None);
    assert_eq!(decoded.type_id, catalog_type.type_id);

    let mut row = model_to_system_row(&implicit_row_type(), &CatalogType::definition()).unwrap();
    row.fields.values.remove("comment");
    let from_missing =
        system_row_to_model::<CatalogType>(&row, &CatalogType::definition()).unwrap();
    assert_eq!(from_missing.comment, None);
}

#[test]
fn routine_owner_security_and_grants_are_queryable() {
    let stores = stores();
    stores.upsert_type(implicit_row_type()).unwrap();
    stores.upsert_routine(create_message_routine()).unwrap();

    let param = CatalogRoutineParameter {
        parameter_id: RoutineParameterId::new(
            &RoutineId::from_parts(Some(&chat_ns()), "create_message"),
            0,
        )
        .unwrap(),
        routine_id:   RoutineId::from_parts(Some(&chat_ns()), "create_message"),
        name:         "payload".to_string(),
        ordinal:      0,
        type_id:      Some(TypeId::from_parts(Some(&chat_ns()), "message")),
        type_name:    "chat.message".to_string(),
        is_array:     false,
        not_null:     true,
        nonempty:     false,
        data_type:    None,
    };
    stores.upsert_parameter(param.clone()).unwrap();

    let grant = CatalogRoutineGrant {
        grant_id:   RoutineGrantId::new(
            &RoutineId::from_parts(Some(&chat_ns()), "create_message"),
            &RoutineGrantee::User,
        ),
        routine_id: RoutineId::from_parts(Some(&chat_ns()), "create_message"),
        grantee:    RoutineGrantee::User,
    };
    stores.upsert_grant(grant.clone()).unwrap();

    let loaded = stores
        .get_routine(&RoutineId::from_parts(Some(&chat_ns()), "create_message"))
        .unwrap()
        .unwrap();
    assert_eq!(loaded.owner, UserId::new("root"));
    assert_eq!(loaded.security, RoutineSecurityMode::Definer);
    assert_eq!(stores.list_grants(&loaded.routine_id).unwrap(), vec![grant]);
    assert_eq!(stores.list_parameters(&loaded.routine_id).unwrap(), vec![param]);
}

#[test]
fn drop_type_is_blocked_by_alias_nested_field_and_routine() {
    let stores = stores();
    stores.upsert_type(address_type()).unwrap();
    stores.upsert_type_field(street_field()).unwrap();
    stores.upsert_type(implicit_row_type()).unwrap();
    stores.upsert_type_field(nested_location_field()).unwrap();

    let err = stores.drop_type(&TypeId::from_parts(Some(&chat_ns()), "address")).unwrap_err();
    assert!(err.to_string().contains("referenced by chat.message.location"));

    stores
        .type_fields
        .delete(
            &TypeFieldId::new(&TypeId::from_parts(Some(&chat_ns()), "message"), "location")
                .unwrap(),
        )
        .unwrap();

    let alias = CatalogType {
        type_id:        TypeId::from_parts(Some(&chat_ns()), "Message"),
        namespace_id:   chat_ns(),
        name:           "Message".to_string(),
        kind:           CatalogTypeKind::RowAlias,
        table_id:       None,
        source_type_id: Some(TypeId::from_parts(Some(&chat_ns()), "message")),
        comment:        None,
    };
    stores.upsert_type(alias).unwrap();
    let err = stores.drop_type(&TypeId::from_parts(Some(&chat_ns()), "message")).unwrap_err();
    assert!(err.to_string().contains("row alias"));

    stores.drop_type(&TypeId::from_parts(Some(&chat_ns()), "Message")).unwrap();
    stores.upsert_routine(create_message_routine()).unwrap();
    let err = stores.drop_type(&TypeId::from_parts(Some(&chat_ns()), "message")).unwrap_err();
    assert!(err.to_string().contains("referenced by routine"));

    stores
        .drop_routine(&RoutineId::from_parts(Some(&chat_ns()), "create_message"))
        .unwrap();
    stores.drop_type(&TypeId::from_parts(Some(&chat_ns()), "message")).unwrap();
    stores.drop_type(&TypeId::from_parts(Some(&chat_ns()), "address")).unwrap();
    assert!(stores
        .get_type(&TypeId::from_parts(Some(&chat_ns()), "address"))
        .unwrap()
        .is_none());
}

#[test]
fn system_table_names_are_registered() {
    let provider = TypesTableProvider::new(Arc::new(InMemoryBackend::new()));
    assert_eq!(SystemTable::Types.table_name(), "types");
    assert_eq!(SystemTable::TypeFields.table_name(), "type_fields");
    assert_eq!(SystemTable::Routines.table_name(), "routines");
    assert_eq!(SystemTable::RoutineParameters.table_name(), "routine_parameters");
    assert_eq!(SystemTable::RoutineGrants.table_name(), "routine_grants");
    assert_eq!(SystemTable::FunctionModules.table_name(), "function_modules");
    assert_eq!(SystemTable::FunctionRevisions.table_name(), "function_revisions");
    assert_eq!(SystemTable::FunctionArtifacts.table_name(), "function_artifacts");
    assert!(SystemTable::Types.partition().is_some());
    provider.upsert_type(address_type()).unwrap();
    assert_eq!(provider.list_types().unwrap().len(), 1);
}

fn function_rows(
    label: &str,
) -> (CatalogFunctionModule, CatalogFunctionRevision, CatalogFunctionArtifact) {
    let artifact_id = ArtifactId::new(label);
    let module_id = FunctionModuleId::new("backend");
    let revision_id = FunctionRevisionId::from_module_artifact(&module_id, &artifact_id);
    let artifact = CatalogFunctionArtifact {
        artifact_id: artifact_id.clone(),
        size_bytes:  12,
        runtime:     FunctionRuntime::Typescript,
        created_at:  1,
    };
    let revision = CatalogFunctionRevision {
        revision_id: revision_id.clone(),
        module_id: module_id.clone(),
        artifact_id,
        contract_hash: format!("contract-{label}"),
        abi_version: 1,
        runtime: FunctionRuntime::Typescript,
        created_at: 1,
    };
    let module = CatalogFunctionModule {
        module_id,
        runtime: FunctionRuntime::Typescript,
        active_revision_id: Some(revision_id),
        contract_hash: Some(revision.contract_hash.clone()),
        abi_version: 1,
    };
    (module, revision, artifact)
}

#[test]
fn function_revision_cas_and_interruption_leave_old_active() {
    let stores = stores();
    let (module_v1, revision_v1, artifact_v1) = function_rows("aaa");
    let outcome = stores
        .activate_function_revision(module_v1.clone(), revision_v1, artifact_v1, None)
        .unwrap();
    assert_eq!(outcome, ActivateFunctionOutcome::Activated);

    let (module_v2, revision_v2, artifact_v2) = function_rows("bbb");
    stores.upsert_function_artifact(artifact_v2.clone()).unwrap();
    stores.upsert_function_revision(revision_v2.clone()).unwrap();
    let still_v1 = stores.get_function_module(&module_v1.module_id).unwrap().unwrap();
    assert_eq!(still_v1.active_revision_id, module_v1.active_revision_id);

    let err = stores
        .activate_function_revision(
            module_v2.clone(),
            revision_v2.clone(),
            artifact_v2.clone(),
            Some(&FunctionRevisionId::new("backend:missing")),
        )
        .unwrap_err();
    assert!(matches!(err, crate::error::SystemError::Conflict(_)));
    let still_v1 = stores.get_function_module(&module_v1.module_id).unwrap().unwrap();
    assert_eq!(still_v1.active_revision_id, module_v1.active_revision_id);

    let outcome = stores
        .activate_function_revision(
            module_v2.clone(),
            revision_v2,
            artifact_v2,
            module_v1.active_revision_id.as_ref(),
        )
        .unwrap();
    assert_eq!(outcome, ActivateFunctionOutcome::Activated);
    let loaded = stores.get_function_module(&module_v2.module_id).unwrap().unwrap();
    assert_eq!(loaded.active_revision_id, module_v2.active_revision_id);
}
