//! User management SQL commands using sqlparser-rs for robust parsing
//!
//! This module provides SQL command parsing for user management:
//! - CREATE USER: Create a new user with authentication
//! - ALTER USER: Modify user properties (password, role, email)
//! - DROP USER: Soft delete a user account
//!
//! Uses sqlparser-rs tokenizer for consistent identifier and string handling.

use kalamdb_commons::{AuthType, Role};
use serde::{Deserialize, Serialize};
use sqlparser::{
    dialect::GenericDialect,
    tokenizer::{Token, Tokenizer},
};

/// Common error type for user command parsing
#[derive(Debug, Clone, PartialEq)]
pub struct UserCommandError {
    pub message: String,
    pub hint: Option<String>,
}

impl std::fmt::Display for UserCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)?;
        if let Some(hint) = &self.hint {
            write!(f, ". Hint: {}", hint)?;
        }
        Ok(())
    }
}

impl From<UserCommandError> for String {
    fn from(e: UserCommandError) -> String {
        e.to_string()
    }
}

/// Parse SQL role names to Role enum with helpful error messages
fn parse_role(role_str: &str) -> Result<Role, UserCommandError> {
    match role_str.to_lowercase().as_str() {
        "dba" | "admin" => Ok(Role::Dba),
        "developer" | "analyst" | "service" => Ok(Role::Service),
        "viewer" | "readonly" | "user" => Ok(Role::User),
        "system" => Ok(Role::System),
        _ => Err(UserCommandError {
            message: format!("Invalid role '{}'", role_str),
            hint: Some(
                "Valid roles: dba, admin, developer, analyst, viewer, user, service, system"
                    .to_string(),
            ),
        }),
    }
}

/// Extract identifier or string value from a token
fn extract_identifier(token: &Token) -> Option<String> {
    match token {
        Token::Word(w) => Some(w.value.clone()),
        Token::SingleQuotedString(s) => Some(s.clone()),
        Token::DoubleQuotedString(s) => Some(s.clone()),
        _ => None,
    }
}

/// Check if token matches a keyword (case-insensitive)
fn is_keyword(token: &Token, keyword: &str) -> bool {
    matches!(token, Token::Word(w) if w.value.to_uppercase() == keyword)
}

/// Tokenize SQL using sqlparser
fn tokenize(sql: &str) -> Result<Vec<Token>, UserCommandError> {
    let dialect = GenericDialect {};
    Tokenizer::new(&dialect, sql).tokenize().map_err(|e| UserCommandError {
        message: format!("Tokenization error: {}", e),
        hint: None,
    })
}

/// Filter out whitespace tokens
fn filter_tokens(tokens: Vec<Token>) -> Vec<Token> {
    tokens.into_iter().filter(|t| !matches!(t, Token::Whitespace(_))).collect()
}

// ============================================================================
// CREATE USER
// ============================================================================

/// CREATE USER command
///
/// Syntax:
/// ```sql
/// CREATE USER username WITH PASSWORD 'password' ROLE role_name [EMAIL 'email'];
/// CREATE USER username WITH OAUTH ROLE role_name [EMAIL 'email'];
/// CREATE USER username WITH INTERNAL ROLE role_name;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CreateUserStatement {
    pub username: String,
    pub auth_type: AuthType,
    pub role: Role,
    pub email: Option<String>,
    pub password: Option<String>,
}

impl CreateUserStatement {
    pub fn parse(sql: &str) -> Result<Self, String> {
        Self::parse_tokens(sql).map_err(|e| e.to_string())
    }

    fn parse_tokens(sql: &str) -> Result<Self, UserCommandError> {
        let tokens = filter_tokens(tokenize(sql)?);
        let mut iter = tokens.iter().peekable();

        // CREATE USER
        if !is_keyword(iter.next().unwrap_or(&Token::EOF), "CREATE") {
            return Err(UserCommandError {
                message: "Expected CREATE".to_string(),
                hint: Some(
                    "Syntax: CREATE USER username WITH PASSWORD 'pass' ROLE role".to_string(),
                ),
            });
        }
        if !is_keyword(iter.next().unwrap_or(&Token::EOF), "USER") {
            return Err(UserCommandError {
                message: "Expected USER after CREATE".to_string(),
                hint: Some(
                    "Syntax: CREATE USER username WITH PASSWORD 'pass' ROLE role".to_string(),
                ),
            });
        }

        // Username (identifier or quoted string)
        let username = extract_identifier(iter.next().unwrap_or(&Token::EOF)).ok_or_else(|| {
            UserCommandError {
                message: "Expected username after CREATE USER".to_string(),
                hint: Some("Username can be unquoted (alice) or quoted ('alice')".to_string()),
            }
        })?;

        // WITH keyword
        if !is_keyword(iter.next().unwrap_or(&Token::EOF), "WITH") {
            return Err(UserCommandError {
                message: "Expected WITH after username".to_string(),
                hint: Some(
                    "Syntax: CREATE USER username WITH PASSWORD|OAUTH|INTERNAL ...".to_string(),
                ),
            });
        }

        // Auth type: PASSWORD, OAUTH, or INTERNAL
        let auth_token = iter.next().unwrap_or(&Token::EOF);
        let (auth_type, password) = if is_keyword(auth_token, "PASSWORD") {
            let pwd = extract_identifier(iter.next().unwrap_or(&Token::EOF)).ok_or_else(|| {
                UserCommandError {
                    message: "Expected password value after PASSWORD".to_string(),
                    hint: Some("Password must be quoted: WITH PASSWORD 'secret'".to_string()),
                }
            })?;
            (AuthType::Password, Some(pwd))
        } else if is_keyword(auth_token, "OAUTH") {
            // Optional JSON payload
            let payload = if let Some(token) = iter.peek() {
                if matches!(token, Token::SingleQuotedString(_)) {
                    extract_identifier(iter.next().unwrap_or(&Token::EOF))
                } else {
                    None
                }
            } else {
                None
            };
            (AuthType::OAuth, payload)
        } else if is_keyword(auth_token, "INTERNAL") {
            (AuthType::Internal, None)
        } else {
            return Err(UserCommandError {
                message: "Expected PASSWORD, OAUTH, or INTERNAL after WITH".to_string(),
                hint: Some(
                    "Valid auth types: WITH PASSWORD 'pass', WITH OAUTH, WITH INTERNAL".to_string(),
                ),
            });
        };

        // ROLE keyword and value
        if !is_keyword(iter.next().unwrap_or(&Token::EOF), "ROLE") {
            return Err(UserCommandError {
                message: "Expected ROLE keyword".to_string(),
                hint: Some("ROLE is required: ... ROLE dba".to_string()),
            });
        }
        let role_str = extract_identifier(iter.next().unwrap_or(&Token::EOF)).ok_or_else(|| {
            UserCommandError {
                message: "Expected role name after ROLE".to_string(),
                hint: Some("Valid roles: dba, admin, developer, service, user, system".to_string()),
            }
        })?;
        let role = parse_role(&role_str)?;

        // Optional EMAIL
        let email = if is_keyword(iter.peek().unwrap_or(&&Token::EOF), "EMAIL") {
            iter.next(); // consume EMAIL
            Some(extract_identifier(iter.next().unwrap_or(&Token::EOF)).ok_or_else(|| {
                UserCommandError {
                    message: "Expected email address after EMAIL".to_string(),
                    hint: Some("Email must be quoted: EMAIL 'user@example.com'".to_string()),
                }
            })?)
        } else {
            None
        };

        Ok(CreateUserStatement {
            username,
            auth_type,
            role,
            email,
            password,
        })
    }
}

// ============================================================================
// ALTER USER
// ============================================================================

/// ALTER USER command
///
/// Syntax:
/// ```sql
/// ALTER USER username SET PASSWORD 'new_password';
/// ALTER USER username SET ROLE new_role;
/// ALTER USER username SET EMAIL 'new_email@example.com';
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AlterUserStatement {
    pub username: String,
    pub modification: UserModification,
}

/// Type of user modification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UserModification {
    SetPassword(String),
    SetRole(Role),
    SetEmail(String),
}

impl UserModification {
    /// Returns a sanitized string representation suitable for audit logs.
    /// Masks passwords to prevent credential leakage in audit logs.
    pub fn display_for_audit(&self) -> String {
        match self {
            UserModification::SetPassword(_) => "SetPassword([REDACTED])".to_string(),
            UserModification::SetRole(role) => format!("SetRole({:?})", role),
            UserModification::SetEmail(email) => format!("SetEmail({})", email),
        }
    }
}

impl AlterUserStatement {
    pub fn parse(sql: &str) -> Result<Self, String> {
        Self::parse_tokens(sql).map_err(|e| e.to_string())
    }

    fn parse_tokens(sql: &str) -> Result<Self, UserCommandError> {
        let tokens = filter_tokens(tokenize(sql)?);
        let mut iter = tokens.iter().peekable();

        // ALTER USER
        if !is_keyword(iter.next().unwrap_or(&Token::EOF), "ALTER") {
            return Err(UserCommandError {
                message: "Expected ALTER".to_string(),
                hint: Some("Syntax: ALTER USER username SET PASSWORD|ROLE|EMAIL ...".to_string()),
            });
        }
        if !is_keyword(iter.next().unwrap_or(&Token::EOF), "USER") {
            return Err(UserCommandError {
                message: "Expected USER after ALTER".to_string(),
                hint: Some("Syntax: ALTER USER username SET PASSWORD|ROLE|EMAIL ...".to_string()),
            });
        }

        // Username (identifier or quoted string)
        let username = extract_identifier(iter.next().unwrap_or(&Token::EOF)).ok_or_else(|| {
            UserCommandError {
                message: "Expected username after ALTER USER".to_string(),
                hint: Some("Username can be unquoted (root) or quoted ('root')".to_string()),
            }
        })?;

        // SET keyword
        if !is_keyword(iter.next().unwrap_or(&Token::EOF), "SET") {
            return Err(UserCommandError {
                message: "Expected SET after username".to_string(),
                hint: Some("Syntax: ALTER USER username SET PASSWORD|ROLE|EMAIL ...".to_string()),
            });
        }

        // Modification type: PASSWORD, ROLE, or EMAIL
        let mod_token = iter.next().unwrap_or(&Token::EOF);
        let modification = if is_keyword(mod_token, "PASSWORD") {
            let pwd = extract_identifier(iter.next().unwrap_or(&Token::EOF)).ok_or_else(|| {
                UserCommandError {
                    message: "Expected password value after SET PASSWORD".to_string(),
                    hint: Some("Password must be quoted: SET PASSWORD 'newsecret'".to_string()),
                }
            })?;
            UserModification::SetPassword(pwd)
        } else if is_keyword(mod_token, "ROLE") {
            let role_str =
                extract_identifier(iter.next().unwrap_or(&Token::EOF)).ok_or_else(|| {
                    UserCommandError {
                        message: "Expected role name after SET ROLE".to_string(),
                        hint: Some(
                            "Valid roles: dba, admin, developer, service, user, system".to_string(),
                        ),
                    }
                })?;
            let role = parse_role(&role_str)?;
            UserModification::SetRole(role)
        } else if is_keyword(mod_token, "EMAIL") {
            let email =
                extract_identifier(iter.next().unwrap_or(&Token::EOF)).ok_or_else(|| {
                    UserCommandError {
                        message: "Expected email address after SET EMAIL".to_string(),
                        hint: Some(
                            "Email must be quoted: SET EMAIL 'user@example.com'".to_string(),
                        ),
                    }
                })?;
            UserModification::SetEmail(email)
        } else {
            return Err(UserCommandError {
                message: "Expected PASSWORD, ROLE, or EMAIL after SET".to_string(),
                hint: Some(
                    "Valid modifications: SET PASSWORD 'pass', SET ROLE admin, SET EMAIL 'x@y.com'"
                        .to_string(),
                ),
            });
        };

        Ok(AlterUserStatement {
            username,
            modification,
        })
    }
}

// ============================================================================
// DROP USER
// ============================================================================

/// DROP USER command
///
/// Syntax:
/// ```sql
/// DROP USER username;
/// DROP USER IF EXISTS username;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DropUserStatement {
    pub username: String,
    pub if_exists: bool,
}

impl DropUserStatement {
    pub fn parse(sql: &str) -> Result<Self, String> {
        Self::parse_tokens(sql).map_err(|e| e.to_string())
    }

    fn parse_tokens(sql: &str) -> Result<Self, UserCommandError> {
        let tokens = filter_tokens(tokenize(sql)?);
        let mut iter = tokens.iter().peekable();

        // DROP USER
        if !is_keyword(iter.next().unwrap_or(&Token::EOF), "DROP") {
            return Err(UserCommandError {
                message: "Expected DROP".to_string(),
                hint: Some("Syntax: DROP USER [IF EXISTS] username".to_string()),
            });
        }
        if !is_keyword(iter.next().unwrap_or(&Token::EOF), "USER") {
            return Err(UserCommandError {
                message: "Expected USER after DROP".to_string(),
                hint: Some("Syntax: DROP USER [IF EXISTS] username".to_string()),
            });
        }

        // Optional IF EXISTS
        let if_exists = if is_keyword(iter.peek().unwrap_or(&&Token::EOF), "IF") {
            iter.next(); // consume IF
            if !is_keyword(iter.next().unwrap_or(&Token::EOF), "EXISTS") {
                return Err(UserCommandError {
                    message: "Expected EXISTS after IF".to_string(),
                    hint: Some("Syntax: DROP USER IF EXISTS username".to_string()),
                });
            }
            true
        } else {
            false
        };

        // Username (identifier or quoted string)
        let username = extract_identifier(iter.next().unwrap_or(&Token::EOF)).ok_or_else(|| {
            UserCommandError {
                message: "Expected username".to_string(),
                hint: Some("Username can be unquoted (alice) or quoted ('alice')".to_string()),
            }
        })?;

        Ok(DropUserStatement {
            username,
            if_exists,
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // CREATE USER tests
    #[test]
    fn test_create_user_with_password_quoted() {
        let sql = "CREATE USER 'alice' WITH PASSWORD 'secure123' ROLE developer EMAIL \
                   'alice@example.com'";
        let stmt = CreateUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "alice");
        assert_eq!(stmt.auth_type, AuthType::Password);
        assert_eq!(stmt.password, Some("secure123".to_string()));
        assert_eq!(stmt.role, Role::Service);
        assert_eq!(stmt.email, Some("alice@example.com".to_string()));
    }

    #[test]
    fn test_create_user_with_password_unquoted() {
        let sql = "CREATE USER alice WITH PASSWORD 'secure123' ROLE developer";
        let stmt = CreateUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "alice");
        assert_eq!(stmt.auth_type, AuthType::Password);
    }

    #[test]
    fn test_create_user_with_oauth() {
        let sql = "CREATE USER 'oauth_user' WITH OAUTH ROLE viewer EMAIL 'user@example.com'";
        let stmt = CreateUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "oauth_user");
        assert_eq!(stmt.auth_type, AuthType::OAuth);
        assert_eq!(stmt.password, None);
        assert_eq!(stmt.role, Role::User);
    }

    #[test]
    fn test_create_user_with_internal() {
        let sql = "CREATE USER 'service_account' WITH INTERNAL ROLE system";
        let stmt = CreateUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "service_account");
        assert_eq!(stmt.auth_type, AuthType::Internal);
        assert_eq!(stmt.role, Role::System);
    }

    #[test]
    fn test_create_user_missing_auth() {
        let sql = "CREATE USER alice ROLE developer";
        let result = CreateUserStatement::parse(sql);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("WITH"));
    }

    #[test]
    fn test_create_user_invalid_role() {
        let sql = "CREATE USER alice WITH PASSWORD 'pass' ROLE invalid_role";
        let result = CreateUserStatement::parse(sql);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid role"));
    }

    // ALTER USER tests
    #[test]
    fn test_alter_user_set_password_quoted() {
        let sql = "ALTER USER 'alice' SET PASSWORD 'newsecure456'";
        let stmt = AlterUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "alice");
        if let UserModification::SetPassword(pw) = stmt.modification {
            assert_eq!(pw, "newsecure456");
        } else {
            panic!("Expected SetPassword");
        }
    }

    #[test]
    fn test_alter_user_set_password_unquoted() {
        // This is the bug case: ALTER USER root SET PASSWORD 'test666'
        let sql = "ALTER USER root SET PASSWORD 'test666'";
        let stmt = AlterUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "root");
        if let UserModification::SetPassword(pw) = stmt.modification {
            assert_eq!(pw, "test666");
        } else {
            panic!("Expected SetPassword");
        }
    }

    #[test]
    fn test_alter_user_set_role_unquoted() {
        let sql = "ALTER USER admin SET ROLE dba";
        let stmt = AlterUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "admin");
        if let UserModification::SetRole(role) = stmt.modification {
            assert_eq!(role, Role::Dba);
        } else {
            panic!("Expected SetRole");
        }
    }

    #[test]
    fn test_alter_user_set_role_quoted() {
        let sql = "ALTER USER 'alice' SET ROLE admin";
        let stmt = AlterUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "alice");
        if let UserModification::SetRole(role) = stmt.modification {
            assert_eq!(role, Role::Dba);
        } else {
            panic!("Expected SetRole");
        }
    }

    #[test]
    fn test_alter_user_set_email() {
        let sql = "ALTER USER bob SET EMAIL 'bob@new.com'";
        let stmt = AlterUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "bob");
        if let UserModification::SetEmail(email) = stmt.modification {
            assert_eq!(email, "bob@new.com");
        } else {
            panic!("Expected SetEmail");
        }
    }

    #[test]
    fn test_alter_user_invalid_modification() {
        let sql = "ALTER USER alice SET UNKNOWN 'value'";
        let result = AlterUserStatement::parse(sql);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("PASSWORD, ROLE, or EMAIL"));
    }

    // DROP USER tests
    #[test]
    fn test_drop_user_quoted() {
        let sql = "DROP USER 'alice'";
        let stmt = DropUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "alice");
        assert!(!stmt.if_exists);
    }

    #[test]
    fn test_drop_user_unquoted() {
        let sql = "DROP USER alice";
        let stmt = DropUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "alice");
        assert!(!stmt.if_exists);
    }

    #[test]
    fn test_drop_user_if_exists_quoted() {
        let sql = "DROP USER IF EXISTS 'bob'";
        let stmt = DropUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "bob");
        assert!(stmt.if_exists);
    }

    #[test]
    fn test_drop_user_if_exists_unquoted() {
        let sql = "DROP USER IF EXISTS bob";
        let stmt = DropUserStatement::parse(sql).unwrap();
        assert_eq!(stmt.username, "bob");
        assert!(stmt.if_exists);
    }

    #[test]
    fn test_drop_user_missing_username() {
        let sql = "DROP USER";
        let result = DropUserStatement::parse(sql);
        assert!(result.is_err());
    }

    // UserModification display_for_audit tests
    #[test]
    fn test_user_modification_display_for_audit_password() {
        let modification = UserModification::SetPassword("SuperSecret123!".to_string());
        let display = modification.display_for_audit();

        // Should contain [REDACTED]
        assert!(display.contains("[REDACTED]"), "Expected [REDACTED] in: {}", display);

        // Should NOT contain the actual password
        assert!(
            !display.contains("SuperSecret123!"),
            "Password should be masked in: {}",
            display
        );
    }

    #[test]
    fn test_user_modification_display_for_audit_role() {
        let modification = UserModification::SetRole(Role::Dba);
        let display = modification.display_for_audit();

        assert_eq!(display, "SetRole(Dba)");
    }

    #[test]
    fn test_user_modification_display_for_audit_email() {
        let modification = UserModification::SetEmail("alice@example.com".to_string());
        let display = modification.display_for_audit();

        assert_eq!(display, "SetEmail(alice@example.com)");
    }
}
