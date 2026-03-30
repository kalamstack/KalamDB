//! CREATE TOPIC handler

use kalamdb_core::app_context::AppContext;
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::{ExecutionContext, ExecutionResult, ScalarValue};
use kalamdb_core::sql::executor::handlers::TypedStatementHandler;
use kalamdb_commons::models::{NamespaceId, TopicId};
use kalamdb_sql::ddl::CreateTopicStatement;
use kalamdb_system::providers::topics::models::Topic;
use std::sync::Arc;

/// Handler for CREATE TOPIC statements
pub struct CreateTopicHandler {
    app_context: Arc<AppContext>,
}

impl CreateTopicHandler {
    pub fn new(app_context: Arc<AppContext>) -> Self {
        Self { app_context }
    }

    fn extract_namespace_id(topic_name: &str) -> Result<NamespaceId, KalamDbError> {
        let (namespace, topic_local_name) = topic_name.split_once('.').ok_or_else(|| {
            KalamDbError::InvalidOperation(
                "Topic name must be namespace-qualified: <namespace>.<topic>".to_string(),
            )
        })?;

        if namespace.is_empty() || topic_local_name.is_empty() {
            return Err(KalamDbError::InvalidOperation(
                "Topic name must be namespace-qualified: <namespace>.<topic>".to_string(),
            ));
        }

        Ok(NamespaceId::new(namespace))
    }
}

#[async_trait::async_trait]
impl TypedStatementHandler<CreateTopicStatement> for CreateTopicHandler {
    async fn execute(
        &self,
        statement: CreateTopicStatement,
        _params: Vec<ScalarValue>,
        _context: &ExecutionContext,
    ) -> Result<ExecutionResult, KalamDbError> {
        let namespace_id = Self::extract_namespace_id(&statement.topic_name)?;
        let namespaces_provider = self.app_context.system_tables().namespaces();
        if namespaces_provider.get_namespace_async(&namespace_id).await?.is_none() {
            return Err(KalamDbError::NotFound(format!(
                "Namespace '{}' does not exist",
                namespace_id
            )));
        }

        let topic_id = TopicId::new(&statement.topic_name);

        // Access topics provider from system_tables
        let topics_provider = self.app_context.system_tables().topics();

        // Check if topic already exists
        if topics_provider.get_topic_by_id_async(&topic_id).await?.is_some() {
            return Err(KalamDbError::AlreadyExists(format!(
                "Topic '{}' already exists",
                statement.topic_name
            )));
        }

        // Create topic with default settings
        let mut topic = Topic::new(topic_id.clone(), statement.topic_name.clone());
        topic.partitions = statement.partitions.unwrap_or(1); // Default to 1 partition
        topic.retention_seconds = Some(7 * 24 * 60 * 60); // Default 7 days
        topic.retention_max_bytes = Some(1024 * 1024 * 1024); // Default 1GB

        // Insert into system.topics
        topics_provider.create_topic_async(topic.clone()).await?;

        // Add to TopicPublisherService cache for immediate availability
        self.app_context.topic_publisher().add_topic(topic.clone());

        Ok(ExecutionResult::Success {
            message: format!(
                "Created topic '{}' with {} partition(s)",
                statement.topic_name, topic.partitions
            ),
        })
    }

    async fn check_authorization(
        &self,
        _statement: &CreateTopicStatement,
        context: &ExecutionContext,
    ) -> Result<(), KalamDbError> {
        use kalamdb_commons::Role;

        // Only DBA and System roles can create topics
        match context.user_role() {
            Role::Dba | Role::System => Ok(()),
            _ => Err(KalamDbError::PermissionDenied(
                "CREATE TOPIC requires DBA or System role".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kalamdb_core::test_helpers::{create_test_session_simple, test_app_context_simple};
    use kalamdb_commons::models::UserId;
    use kalamdb_commons::Role;
    use kalamdb_system::Namespace;

    fn test_context() -> ExecutionContext {
        ExecutionContext::new(UserId::from("test_user"), Role::Dba, create_test_session_simple())
    }

    #[tokio::test]
    async fn test_create_topic() {
        let app_ctx = test_app_context_simple();
        app_ctx
            .system_tables()
            .namespaces()
            .create_namespace(Namespace::new("test"))
            .expect("failed to seed namespace");
        let handler = CreateTopicHandler::new(app_ctx);
        let ctx = test_context();

        let stmt = CreateTopicStatement {
            topic_name: "test.test_events".to_string(),
            partitions: Some(4),
        };

        let result = handler.execute(stmt, vec![], &ctx).await;
        assert!(result.is_ok(), "Failed to create topic: {:?}", result.err());

        match result.unwrap() {
            ExecutionResult::Success { message } => {
                assert!(message.contains("test.test_events"));
                assert!(message.contains("4 partition"));
            },
            _ => panic!("Expected Success result"),
        }
    }

    #[tokio::test]
    async fn test_create_topic_duplicate() {
        let app_ctx = test_app_context_simple();
        app_ctx
            .system_tables()
            .namespaces()
            .create_namespace(Namespace::new("test"))
            .expect("failed to seed namespace");
        let handler = CreateTopicHandler::new(app_ctx);
        let ctx = test_context();

        let stmt = CreateTopicStatement {
            topic_name: "test.duplicate_topic".to_string(),
            partitions: None,
        };

        // First creation should succeed
        let result1 = handler.execute(stmt.clone(), vec![], &ctx).await;
        assert!(result1.is_ok());

        // Second creation should fail with AlreadyExists
        let result2 = handler.execute(stmt, vec![], &ctx).await;
        assert!(result2.is_err());
        assert!(matches!(result2.unwrap_err(), KalamDbError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_create_topic_requires_namespace_qualified_name() {
        let app_ctx = test_app_context_simple();
        let handler = CreateTopicHandler::new(app_ctx);
        let ctx = test_context();

        let stmt = CreateTopicStatement {
            topic_name: "topic_without_namespace".to_string(),
            partitions: None,
        };

        let result = handler.execute(stmt, vec![], &ctx).await;
        assert!(matches!(result, Err(KalamDbError::InvalidOperation(_))));
    }

    #[tokio::test]
    async fn test_create_topic_requires_existing_namespace() {
        let app_ctx = test_app_context_simple();
        let handler = CreateTopicHandler::new(app_ctx);
        let ctx = test_context();

        let stmt = CreateTopicStatement {
            topic_name: "missing_ns.events".to_string(),
            partitions: None,
        };

        let result = handler.execute(stmt, vec![], &ctx).await;
        assert!(matches!(result, Err(KalamDbError::NotFound(_))));
    }
}
