//! Error types for kalam-cli
//!
//! **Implements T090**: CLI-specific error types for terminal client
//!
//! Provides user-friendly error messages and context for common CLI failures.

use std::fmt;

use kalam_client::KalamLinkError;

/// Result type for CLI operations
pub type Result<T> = std::result::Result<T, CLIError>;

/// Errors that can occur in the CLI
#[derive(Debug)]
pub enum CLIError {
    /// Error from the kalam-client library
    LinkError(KalamLinkError),

    /// Configuration file error
    ConfigurationError(String),

    /// File I/O error
    FileError(String),

    /// Invalid command syntax
    ParseError(String),

    /// User cancelled operation
    Cancelled,

    /// Readline error
    ReadlineError(String),

    /// History file error
    HistoryError(String),

    /// Format error
    FormatError(String),

    /// Subscription error
    SubscriptionError(String),

    /// Server requires initial setup
    SetupRequired(String),
}

// Allow dead code for error variants that will be used in future features
#[allow(dead_code)]
impl CLIError {
    /// Create a subscription error
    pub fn subscription_error(msg: impl Into<String>) -> Self {
        CLIError::SubscriptionError(msg.into())
    }

    fn format_link_error(err: &KalamLinkError) -> String {
        match err {
            KalamLinkError::NetworkError(msg) => Self::clean_nested_message(msg),
            KalamLinkError::AuthenticationError(msg) => msg.clone(),
            KalamLinkError::SetupRequired(msg) => msg.clone(),
            KalamLinkError::ConfigurationError(msg) => msg.clone(),
            KalamLinkError::TimeoutError(msg) => msg.clone(),
            KalamLinkError::SerializationError(msg) => msg.clone(),
            KalamLinkError::WebSocketError(msg) => msg.clone(),
            KalamLinkError::ServerError {
                status_code,
                message,
            } => {
                // Try to parse JSON and extract error.message
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(message) {
                    if let Some(error_obj) = json.get("error") {
                        if let Some(error_msg) = error_obj.get("message").and_then(|v| v.as_str()) {
                            return format!("Server error ({}): {}", status_code, error_msg);
                        }
                    }
                }
                // Fallback to full message if not JSON or no error.message field
                format!("Server error ({}): {}", status_code, message)
            },
            KalamLinkError::Cancelled => "Operation cancelled".to_string(),
        }
    }

    fn clean_nested_message(message: &str) -> String {
        let mut cleaned = message.trim();
        let prefixes = [
            "Connection failed:",
            "connection failed:",
            "Network error:",
            "network error:",
        ];

        loop {
            let mut stripped = false;
            for prefix in &prefixes {
                if let Some(rest) = cleaned.strip_prefix(prefix) {
                    cleaned = rest.trim_start();
                    stripped = true;
                    break;
                }
            }

            if !stripped {
                break;
            }
        }

        cleaned.to_string()
    }
}

impl fmt::Display for CLIError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CLIError::LinkError(e) => write!(f, "{}", Self::format_link_error(e)),
            CLIError::ConfigurationError(msg) => write!(f, "Configuration error: {}", msg),
            CLIError::FileError(msg) => write!(f, "File error: {}", msg),
            CLIError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            CLIError::Cancelled => write!(f, "Operation cancelled"),
            CLIError::ReadlineError(msg) => write!(f, "Input error: {}", msg),
            CLIError::HistoryError(msg) => write!(f, "History error: {}", msg),
            CLIError::FormatError(msg) => write!(f, "Format error: {}", msg),
            CLIError::SubscriptionError(msg) => write!(f, "Subscription error: {}", msg),
            CLIError::SetupRequired(msg) => write!(f, "Server setup required: {}", msg),
        }
    }
}

impl std::error::Error for CLIError {}

impl From<KalamLinkError> for CLIError {
    fn from(err: KalamLinkError) -> Self {
        CLIError::LinkError(err)
    }
}

impl From<rustyline::error::ReadlineError> for CLIError {
    fn from(err: rustyline::error::ReadlineError) -> Self {
        match err {
            rustyline::error::ReadlineError::Interrupted => CLIError::Cancelled,
            rustyline::error::ReadlineError::Eof => CLIError::Cancelled,
            e => CLIError::ReadlineError(e.to_string()),
        }
    }
}

impl From<std::io::Error> for CLIError {
    fn from(err: std::io::Error) -> Self {
        CLIError::FileError(err.to_string())
    }
}

impl From<toml::de::Error> for CLIError {
    fn from(err: toml::de::Error) -> Self {
        CLIError::ConfigurationError(format!("TOML parse error: {}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = CLIError::ParseError("Invalid SQL".into());
        assert_eq!(err.to_string(), "Parse error: Invalid SQL");

        let err = CLIError::Cancelled;
        assert_eq!(err.to_string(), "Operation cancelled");
    }
}
