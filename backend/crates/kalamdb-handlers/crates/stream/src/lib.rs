pub mod result_rows;
pub mod topics;

use std::sync::Arc;

use kalamdb_core::{app_context::AppContext, sql::executor::handler_registry::HandlerRegistry};
use kalamdb_handlers_support::register_typed_handler;
use kalamdb_sql::{
    classifier::SqlStatementKind,
    ddl::{
        AckStatement, AddTopicSourceStatement, ClearTopicStatement, ConsumePosition,
        ConsumeStatement, CreateTopicStatement, DropTopicStatement,
    },
};

pub fn register_stream_handlers(registry: &HandlerRegistry, app_context: Arc<AppContext>) {
    use kalamdb_commons::models::{PayloadMode, TableId, TopicId};

    register_typed_handler!(
        registry,
        SqlStatementKind::CreateTopic(CreateTopicStatement {
            topic_name: "_placeholder".to_string(),
            partitions: None,
        }),
        topics::CreateTopicHandler::new(app_context.clone()),
        SqlStatementKind::CreateTopic,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::DropTopic(DropTopicStatement {
            topic_name: "_placeholder".to_string(),
        }),
        topics::DropTopicHandler::new(app_context.clone()),
        SqlStatementKind::DropTopic,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::ClearTopic(ClearTopicStatement {
            topic_id: TopicId::new("_placeholder"),
        }),
        topics::ClearTopicHandler::new(app_context.clone()),
        SqlStatementKind::ClearTopic,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::AddTopicSource(AddTopicSourceStatement {
            topic_name: "_placeholder".to_string(),
            table_id: TableId::from_strings("_placeholder", "_placeholder"),
            operation: kalamdb_commons::models::TopicOp::Insert,
            filter_expr: None,
            payload_mode: PayloadMode::Full,
        }),
        topics::AddTopicSourceHandler::new(app_context.clone()),
        SqlStatementKind::AddTopicSource,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::ConsumeTopic(ConsumeStatement {
            topic_name: "_placeholder".to_string(),
            group_id: None,
            position: ConsumePosition::Latest,
            limit: None,
        }),
        topics::ConsumeHandler::new(app_context.clone()),
        SqlStatementKind::ConsumeTopic,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::AckTopic(AckStatement {
            topic_name: "_placeholder".to_string(),
            group_id: "_placeholder".to_string(),
            partition_id: 0,
            upto_offset: 0,
        }),
        topics::AckHandler::new(app_context),
        SqlStatementKind::AckTopic,
    );
}
