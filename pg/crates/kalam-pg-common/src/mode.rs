/// Backend mode for the PostgreSQL extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendMode {
    Remote,
}

impl BackendMode {
    /// Returns the active backend mode (always remote).
    pub fn current() -> Self {
        Self::Remote
    }
}
