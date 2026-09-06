//! CALL origin, frames, and HTTP overrides.

use std::{collections::HashMap, sync::Arc};

use kalamdb_commons::{
    models::{FunctionRevisionId, RoutineId, UserId},
    Role, RoutineSecurityMode,
};
use parking_lot::Mutex;

#[derive(Debug, Clone)]
pub struct HttpResponseOverrides {
    pub status:  Option<u16>,
    pub headers: HashMap<String, String>,
}

impl Default for HttpResponseOverrides {
    fn default() -> Self {
        Self {
            status:  None,
            headers: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum FunctionCallOrigin {
    Sql,
    Http {
        headers:  HashMap<String, String>,
        response: Arc<Mutex<HttpResponseOverrides>>,
    },
    Topic {
        topic_name: String,
        event_id:   String,
        partition:  u32,
        offset:     u64,
        attempt:    u32,
    },
}

#[derive(Debug, Clone)]
#[allow(dead_code)] // revision_id/security are kept on the frame for nested INVOKER/DEFINER
pub struct ProcedureFrame {
    pub routine_id:     RoutineId,
    pub revision_id:    FunctionRevisionId,
    pub principal_user: UserId,
    pub principal_role: Role,
    pub security:       RoutineSecurityMode,
}

impl ProcedureFrame {
    pub fn stack_label(&self) -> String {
        self.routine_id.as_str().to_string()
    }
}

#[derive(Debug, Clone)]
pub struct FunctionCallResult {
    pub value:        kalamdb_functions::RoutineValue,
    pub http_status:  Option<u16>,
    pub http_headers: HashMap<String, String>,
}
