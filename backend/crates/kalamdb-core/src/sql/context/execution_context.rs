use datafusion::prelude::SessionContext;
use kalamdb_commons::models::ReadContext;
use kalamdb_commons::{NamespaceId, Role, UserId};
use kalamdb_session::AuthSession;
use kalamdb_session_datafusion::SessionUserContext;
use once_cell::sync::OnceCell;
use std::sync::Arc;
use std::time::SystemTime;

/// Unified execution context for SQL queries
///
/// Combines authenticated session information with DataFusion session management
/// and query-specific state (namespace, caching).
#[derive(Clone)]
pub struct ExecutionContext {
    /// Authenticated session with user identity and metadata
    auth_session: AuthSession,
    /// Optional namespace for this query execution
    namespace_id: Option<NamespaceId>,
    /// Base SessionContext from AppContext (tables already registered)
    /// We extract SessionState from this and inject user_id to create per-request SessionContext
    base_session_context: Arc<SessionContext>,
    /// Cached per-request SessionState with user context injected
    ///
    /// This avoids repeated allocations when a single request needs multiple
    /// SessionContext instances (retries, planning fallbacks) while keeping
    /// user isolation intact.
    session_context_cache: Arc<OnceCell<SessionContext>>,
}

impl ExecutionContext {
    /// Create a new ExecutionContext with base SessionContext
    ///
    /// # Arguments
    /// * `user_id` - User ID executing the query
    /// * `user_role` - User's role for authorization
    /// * `base_session_context` - Base SessionContext from AppContext (tables already registered)
    ///
    /// # Note
    /// The base_session_context contains all registered table providers.
    /// When executing queries, call `create_session_with_user()` to get a
    /// SessionContext with user_id injected for per-user filtering.
    pub fn new(
        user_id: UserId,
        user_role: Role,
        base_session_context: Arc<SessionContext>,
    ) -> Self {
        Self {
            auth_session: AuthSession::new(user_id, user_role),
            namespace_id: None,
            base_session_context,
            session_context_cache: Arc::new(OnceCell::new()),
        }
    }

    /// Create ExecutionContext from an existing AuthSession
    pub fn from_session(
        auth_session: AuthSession,
        base_session_context: Arc<SessionContext>,
    ) -> Self {
        Self {
            auth_session,
            namespace_id: None,
            base_session_context,
            session_context_cache: Arc::new(OnceCell::new()),
        }
    }

    pub fn with_namespace(
        user_id: UserId,
        user_role: Role,
        namespace_id: NamespaceId,
        base_session_context: Arc<SessionContext>,
    ) -> Self {
        Self {
            auth_session: AuthSession::new(user_id, user_role),
            namespace_id: Some(namespace_id),
            base_session_context,
            session_context_cache: Arc::new(OnceCell::new()),
        }
    }

    #[inline]
    pub fn is_admin(&self) -> bool {
        self.auth_session.is_admin()
    }
    #[inline]
    pub fn is_system(&self) -> bool {
        self.auth_session.is_system()
    }

    /// Check if this is an anonymous user (not authenticated)
    ///
    /// Anonymous users have limited permissions:
    /// - Can only SELECT from public tables
    /// - Cannot CREATE, ALTER, DROP, INSERT, UPDATE, or DELETE
    #[inline]
    pub fn is_anonymous(&self) -> bool {
        self.auth_session.is_anonymous()
    }

    #[inline]
    pub fn user_id(&self) -> &UserId {
        self.auth_session.user_id()
    }
    #[inline]
    pub fn user_role(&self) -> Role {
        self.auth_session.role()
    }
    #[inline]
    pub fn username(&self) -> Option<&str> {
        self.auth_session.user_context().username.as_ref().map(|username| username.as_str())
    }
    #[inline]
    pub fn request_id(&self) -> Option<&str> {
        self.auth_session.request_id()
    }
    #[inline]
    pub fn ip_address(&self) -> Option<&str> {
        self.auth_session.ip_address()
    }
    #[inline]
    pub fn timestamp(&self) -> SystemTime {
        self.auth_session.timestamp()
    }

    // Builder methods for Phase 3
    pub fn with_request_id(mut self, request_id: String) -> Self {
        self.auth_session = self.auth_session.with_request_id(request_id);
        self
    }

    /// Set the namespace for this execution context
    pub fn with_namespace_id(mut self, namespace_id: NamespaceId) -> Self {
        self.namespace_id = Some(namespace_id);
        self.session_context_cache = Arc::new(OnceCell::new());
        self
    }

    /// Clone this context with a different effective identity while preserving
    /// namespace and request metadata.
    pub fn with_effective_identity(&self, user_id: UserId, role: Role) -> Self {
        let mut auth_session = self.auth_session.clone();
        auth_session.user_context.user_id = user_id;
        auth_session.user_context.role = role;

        Self {
            auth_session,
            namespace_id: self.namespace_id.clone(),
            base_session_context: Arc::clone(&self.base_session_context),
            session_context_cache: Arc::new(OnceCell::new()),
        }
    }

    /// Set the read context (client vs internal)
    ///
    /// Use `ReadContext::Internal` for WebSocket subscriptions on followers
    /// to bypass leader-only read checks while still applying RLS.
    pub fn with_read_context(mut self, read_context: ReadContext) -> Self {
        self.auth_session = self.auth_session.with_read_context_mode(read_context);
        self.session_context_cache = Arc::new(OnceCell::new());
        self
    }

    fn build_user_session_context(&self) -> SessionContext {
        // Clone SessionState to keep per-user options/extensions isolated
        // (shared internals like RuntimeEnv remain Arc-backed)
        let mut session_state = self.base_session_context.state().clone();

        // Inject current user_id, role, and read_context into session config extensions
        // TableProviders will read this during scan() for per-user filtering and leader check
        // Use the read_context from this ExecutionContext (defaults to Client)
        let session_user_context =
            if let Some(username) = self.auth_session.user_context().username.clone() {
                SessionUserContext::with_username(
                    self.auth_session.user_id().clone(),
                    username,
                    self.auth_session.role(),
                    self.auth_session.read_context(),
                )
            } else {
                SessionUserContext::new(
                    self.auth_session.user_id().clone(),
                    self.auth_session.role(),
                    self.auth_session.read_context(),
                )
            };

        session_state.config_mut().options_mut().extensions.insert(session_user_context);

        // Override default_schema if namespace_id is set on this context
        if let Some(ref ns) = self.namespace_id {
            session_state.config_mut().options_mut().catalog.default_schema =
                ns.as_str().to_string();
        }

        // Create SessionContext from the per-user state
        let ctx = SessionContext::new_with_state(session_state);

        ctx
    }

    fn build_effective_session_context(&self, user_id: UserId, role: Role) -> SessionContext {
        let mut session_state = self.base_session_context.state().clone();

        session_state
            .config_mut()
            .options_mut()
            .extensions
            .insert(SessionUserContext::new(
                user_id.clone(),
                role,
                self.auth_session.read_context(),
            ));

        if let Some(ref ns) = self.namespace_id {
            session_state.config_mut().options_mut().catalog.default_schema =
                ns.as_str().to_string();
        }

        let ctx = SessionContext::new_with_state(session_state);

        ctx
    }

    /// Create a per-request SessionContext with current user_id and role injected
    ///
    /// Clones the base SessionState and injects the current user_id and role into config.extensions.
    /// The clone is relatively cheap (~1-2μs) because most fields are Arc-wrapped.
    ///
    /// # What Gets Cloned
    /// - session_id: String (~50 bytes)
    /// - config: Arc<SessionConfig> (pointer copy)
    /// - runtime_env: Arc<RuntimeEnv> (pointer copy)
    /// - catalog_list: Arc<dyn CatalogList> (pointer copy)
    /// - scalar_functions: HashMap<String, Arc<ScalarUDF>> (HashMap clone, Arc values)
    /// - Total: ~1-2μs per request
    ///
    /// # Performance Impact
    /// - At 10,000 QPS: 10-20ms/sec = 1-2% CPU overhead
    /// - At 100,000 QPS: 100-200ms/sec = 10-20% CPU overhead
    /// - Acceptable trade-off for clean user isolation
    ///
    /// # User Isolation
    /// UserTableProvider and StreamTableProvider will read SessionUserContext from
    /// state.config().options().extensions during scan() to filter data by user.
    ///
    /// # Namespace Handling
    /// If `namespace_id` is set on this ExecutionContext, it will override the
    /// default_schema in the session config. This allows clients to specify the
    /// active namespace per-request.
    ///
    /// # Returns
    /// SessionContext with user_id and role injected, ready for query execution
    pub fn create_session_with_user(&self) -> SessionContext {
        let session = self.session_context_cache.get_or_init(|| self.build_user_session_context());

        session.clone()
    }

    /// Create a per-request SessionContext using an explicit effective identity.
    ///
    /// This bypasses the cached session and is used for impersonation reads where
    /// user_id/role must differ from the actor's authenticated session.
    pub fn create_session_with_effective_user(
        &self,
        user_id: &UserId,
        role: Role,
    ) -> SessionContext {
        self.build_effective_session_context(user_id.clone(), role)
    }

    /// Get the current default namespace (schema) from DataFusion session config
    ///
    /// This reads `datafusion.catalog.default_schema` from the session configuration.
    /// The default schema is set to "default" initially and can be changed using:
    /// - `USE namespace`
    /// - `USE NAMESPACE namespace`  
    /// - `SET NAMESPACE namespace`
    ///
    /// # Returns
    /// The current default namespace as a NamespaceId (defaults to "default")
    pub fn default_namespace(&self) -> NamespaceId {
        let state = self
            .session_context_cache
            .get()
            .map(|session| session.state())
            .unwrap_or_else(|| self.base_session_context.state());
        let default_schema = state.config().options().catalog.default_schema.clone();
        NamespaceId::new(default_schema)
    }

    #[inline]
    pub fn user_context(&self) -> &kalamdb_session::UserContext {
        self.auth_session.user_context()
    }

    /// Get the AuthSession reference (contains all session metadata)
    #[inline]
    pub fn auth_session(&self) -> &AuthSession {
        &self.auth_session
    }
}
