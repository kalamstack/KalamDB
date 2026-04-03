pub mod helpers;
pub mod user;

use kalamdb_commons::AuthType;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::sql::executor::handler_registry::HandlerRegistry;
use kalamdb_handlers_support::register_typed_handler;
use kalamdb_sql::classifier::SqlStatementKind;
use kalamdb_sql::ddl::{
    AlterUserStatement, CreateUserStatement, DropUserStatement, UserModification,
};
use std::sync::Arc;

pub fn register_user_handlers(
    registry: &HandlerRegistry,
    app_context: Arc<AppContext>,
    enforce_password_complexity: bool,
) {
    register_typed_handler!(
        registry,
        SqlStatementKind::CreateUser(CreateUserStatement {
            username: "_placeholder".to_string(),
            auth_type: AuthType::Internal,
            role: kalamdb_commons::Role::User,
            email: None,
            password: None,
        }),
        user::CreateUserHandler::new(app_context.clone(), enforce_password_complexity),
        SqlStatementKind::CreateUser,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::AlterUser(AlterUserStatement {
            username: "_placeholder".to_string(),
            modification: UserModification::SetEmail("_placeholder".to_string()),
        }),
        user::AlterUserHandler::new(app_context.clone(), enforce_password_complexity),
        SqlStatementKind::AlterUser,
    );

    register_typed_handler!(
        registry,
        SqlStatementKind::DropUser(DropUserStatement {
            username: "_placeholder".to_string(),
            if_exists: false,
        }),
        user::DropUserHandler::new(app_context),
        SqlStatementKind::DropUser,
    );
}