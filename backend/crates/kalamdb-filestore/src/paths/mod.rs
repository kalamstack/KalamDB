//! Path resolution for storage paths (StorageCached only).
//!
//! This module is intentionally scoped to StorageCached usage to avoid
//! duplicate path logic elsewhere in the crate.

use std::borrow::Cow;

use kalamdb_commons::models::{TableId, UserId};

/// Template resolution utilities for storage path templates.
pub(crate) struct TemplateResolver;

impl TemplateResolver {
    /// Resolve static placeholders in a storage path template.
    ///
    /// Called once at table creation time. Substitutes:
    /// - `{namespace}` → namespace ID
    /// - `{tableName}` → table name
    ///
    /// Leaves dynamic placeholders (`{userId}`, `{shard}`) for runtime resolution.
    pub(crate) fn resolve_static_placeholders(raw_template: &str, table_id: &TableId) -> String {
        // Normalize legacy placeholder variants first
        let canonical = Self::normalize_template_placeholders(raw_template);

        // Substitute static placeholders
        canonical
            .replace("{namespace}", table_id.namespace_id().as_str())
            .replace("{tableName}", table_id.table_name().as_str())
    }

    /// Resolve dynamic placeholders in a partially-resolved template.
    pub(crate) fn resolve_dynamic_placeholders<'a>(
        template: &'a str,
        user_id: &UserId,
    ) -> Cow<'a, str> {
        let needs_user_sub = template.contains("{userId}");

        if !needs_user_sub {
            return Cow::Borrowed(template);
        }

        let result = template.replace("{userId}", user_id.as_str());
        Cow::Owned(result)
    }

    /// Normalize legacy placeholder variants to canonical names.
    #[inline]
    pub(crate) fn normalize_template_placeholders(template: &str) -> Cow<'_, str> {
        let has_legacy = template.contains("{table_name}")
            || template.contains("{namespace_id}")
            || template.contains("{namespaceId}")
            || template.contains("{table-id}")
            || template.contains("{namespace-id}")
            || template.contains("{user_id}")
            || template.contains("{user-id}")
            || template.contains("{shard_id}")
            || template.contains("{shard-id}");

        if !has_legacy {
            return Cow::Borrowed(template);
        }

        Cow::Owned(
            template
                .replace("{table_name}", "{tableName}")
                .replace("{namespace_id}", "{namespace}")
                .replace("{namespaceId}", "{namespace}")
                .replace("{table-id}", "{tableName}")
                .replace("{namespace-id}", "{namespace}")
                .replace("{user_id}", "{userId}")
                .replace("{user-id}", "{userId}")
                .replace("{shard_id}", "{shard}")
                .replace("{shard-id}", "{shard}"),
        )
    }
}

/// Unified path resolver for StorageCached operations.
pub(crate) struct PathResolver;

impl PathResolver {
    /// Resolve the path prefix for cleanup operations.
    ///
    /// Returns the deepest directory path that does NOT contain unresolved
    /// dynamic placeholders (`{userId}`, `{shard}`). This ensures the result
    /// is a valid filesystem directory for `remove_dir_all`, even when the
    /// placeholder is embedded inside a path segment (e.g. `usr_{userId}`).
    ///
    /// Examples:
    /// - `ns_foo/tbl_bar/usr_{userId}` → `ns_foo/tbl_bar`
    /// - `ns_foo/tbl_bar/{userId}`     → `ns_foo/tbl_bar`
    /// - `{userId}`                    → `` (empty – root)
    /// - `ns_foo/table_bar`            → `ns_foo/table_bar` (no placeholder)
    pub(crate) fn resolve_cleanup_prefix(template: &str) -> Cow<'_, str> {
        // Find the earliest dynamic placeholder and strip back to the last
        // whole directory segment before it.
        let dynamic_pos =
            template.find("{userId}").into_iter().chain(template.find("{shard}")).min();

        if let Some(pos) = dynamic_pos {
            // Everything before the placeholder
            let before = &template[..pos];
            // Find the last '/' to get a clean directory boundary
            if let Some(slash_pos) = before.rfind('/') {
                return Cow::Owned(template[..slash_pos].to_string());
            }
            // Placeholder is in the root-level segment – cleanup from root
            return Cow::Owned(String::new());
        }

        Cow::Borrowed(template.trim_end_matches('/'))
    }
}

#[cfg(test)]
mod tests {
    use kalamdb_commons::models::{NamespaceId, TableName};

    use super::*;

    // ==================== TemplateResolver tests ====================

    #[test]
    fn test_resolve_static_placeholders() {
        let table_id = TableId::new(NamespaceId::new("myns"), TableName::new("mytable"));
        let result = TemplateResolver::resolve_static_placeholders(
            "ns_{namespace}/tbl_{tableName}",
            &table_id,
        );
        assert_eq!(result, "ns_myns/tbl_mytable");
    }

    #[test]
    fn test_resolve_static_leaves_dynamic() {
        let table_id = TableId::new(NamespaceId::new("myns"), TableName::new("mytable"));
        let result = TemplateResolver::resolve_static_placeholders(
            "ns_{namespace}/tbl_{tableName}/usr_{userId}",
            &table_id,
        );
        assert_eq!(result, "ns_myns/tbl_mytable/usr_{userId}");
    }

    #[test]
    fn test_resolve_dynamic_placeholders() {
        let uid = UserId::new("user123");
        let result = TemplateResolver::resolve_dynamic_placeholders(
            "ns_myns/tbl_mytable/usr_{userId}",
            &uid,
        );
        assert_eq!(result.as_ref(), "ns_myns/tbl_mytable/usr_user123");
    }

    #[test]
    fn test_resolve_dynamic_no_placeholder() {
        let uid = UserId::new("user123");
        let result = TemplateResolver::resolve_dynamic_placeholders("ns_myns/tbl_mytable", &uid);
        assert_eq!(result.as_ref(), "ns_myns/tbl_mytable");
        // Should be borrowed (no allocation)
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    // ==================== PathResolver::resolve_cleanup_prefix tests ====================

    #[test]
    fn test_cleanup_prefix_embedded_userid() {
        // Template: usr_{userId} is embedded in a path segment
        let result = PathResolver::resolve_cleanup_prefix("ns_myns/tbl_mytable/usr_{userId}");
        assert_eq!(result.as_ref(), "ns_myns/tbl_mytable");
    }

    #[test]
    fn test_cleanup_prefix_whole_segment_userid() {
        // Template: {userId} is the entire last segment
        let result = PathResolver::resolve_cleanup_prefix("ns_myns/tbl_mytable/{userId}");
        assert_eq!(result.as_ref(), "ns_myns/tbl_mytable");
    }

    #[test]
    fn test_cleanup_prefix_root_userid() {
        // Template: {userId} is at root
        let result = PathResolver::resolve_cleanup_prefix("{userId}");
        assert_eq!(result.as_ref(), "");
    }

    #[test]
    fn test_cleanup_prefix_no_dynamic() {
        // Shared table: no dynamic placeholders
        let result = PathResolver::resolve_cleanup_prefix("ns_myns/table_mytable");
        assert_eq!(result.as_ref(), "ns_myns/table_mytable");
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_cleanup_prefix_no_dynamic_trailing_slash() {
        let result = PathResolver::resolve_cleanup_prefix("ns_myns/table_mytable/");
        assert_eq!(result.as_ref(), "ns_myns/table_mytable");
    }

    #[test]
    fn test_cleanup_prefix_embedded_shard() {
        let result = PathResolver::resolve_cleanup_prefix("ns_myns/tbl_mytable/shard_{shard}");
        assert_eq!(result.as_ref(), "ns_myns/tbl_mytable");
    }

    #[test]
    fn test_cleanup_prefix_userid_before_shard() {
        // Both placeholders; should strip to the earliest one's parent
        let result = PathResolver::resolve_cleanup_prefix("ns_myns/{userId}/shard_{shard}");
        assert_eq!(result.as_ref(), "ns_myns");
    }
}
