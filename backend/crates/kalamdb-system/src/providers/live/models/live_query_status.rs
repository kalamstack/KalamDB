use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Status of a live query subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LiveQueryStatus {
    /// Active and receiving updates
    Active,
    /// Paused by user or system
    Paused,
    /// Completed normally (e.g., connection closed gracefully)
    Completed,
    /// Error state (e.g., connection lost, query failed)
    Error,
}

impl LiveQueryStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            LiveQueryStatus::Active => "active",
            LiveQueryStatus::Paused => "paused",
            LiveQueryStatus::Completed => "completed",
            LiveQueryStatus::Error => "error",
        }
    }
}

impl fmt::Display for LiveQueryStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for LiveQueryStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "active" => Ok(LiveQueryStatus::Active),
            "paused" => Ok(LiveQueryStatus::Paused),
            "completed" => Ok(LiveQueryStatus::Completed),
            "error" => Ok(LiveQueryStatus::Error),
            _ => Err(format!(
                "Invalid live query status: '{}'. Expected: active, paused, completed, error",
                s
            )),
        }
    }
}

impl From<LiveQueryStatus> for String {
    fn from(status: LiveQueryStatus) -> String {
        status.as_str().to_string()
    }
}

impl From<&LiveQueryStatus> for String {
    fn from(status: &LiveQueryStatus) -> String {
        status.as_str().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_as_str() {
        assert_eq!(LiveQueryStatus::Active.as_str(), "active");
        assert_eq!(LiveQueryStatus::Paused.as_str(), "paused");
        assert_eq!(LiveQueryStatus::Completed.as_str(), "completed");
        assert_eq!(LiveQueryStatus::Error.as_str(), "error");
    }

    #[test]
    fn test_status_display() {
        assert_eq!(format!("{}", LiveQueryStatus::Active), "active");
        assert_eq!(format!("{}", LiveQueryStatus::Paused), "paused");
        assert_eq!(format!("{}", LiveQueryStatus::Completed), "completed");
        assert_eq!(format!("{}", LiveQueryStatus::Error), "error");
    }

    #[test]
    fn test_status_from_str() {
        assert_eq!("active".parse::<LiveQueryStatus>().unwrap(), LiveQueryStatus::Active);
        assert_eq!("paused".parse::<LiveQueryStatus>().unwrap(), LiveQueryStatus::Paused);
        assert_eq!("completed".parse::<LiveQueryStatus>().unwrap(), LiveQueryStatus::Completed);
        assert_eq!("error".parse::<LiveQueryStatus>().unwrap(), LiveQueryStatus::Error);

        assert_eq!("ACTIVE".parse::<LiveQueryStatus>().unwrap(), LiveQueryStatus::Active);
        assert_eq!("Paused".parse::<LiveQueryStatus>().unwrap(), LiveQueryStatus::Paused);

        assert!("invalid".parse::<LiveQueryStatus>().is_err());
    }

    #[test]
    fn test_status_to_string() {
        assert_eq!(String::from(LiveQueryStatus::Active), "active");
        assert_eq!(String::from(LiveQueryStatus::Paused), "paused");
        assert_eq!(String::from(&LiveQueryStatus::Completed), "completed");
        assert_eq!(String::from(&LiveQueryStatus::Error), "error");
    }
}
