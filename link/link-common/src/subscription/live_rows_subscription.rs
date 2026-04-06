use crate::error::Result;

use super::{LiveRowsConfig, LiveRowsEvent, LiveRowsMaterializer, SubscriptionManager};

/// High-level subscription that yields materialized live-query row snapshots.
pub struct LiveRowsSubscription {
    inner: SubscriptionManager,
    materializer: LiveRowsMaterializer,
}

impl LiveRowsSubscription {
    pub(crate) fn new(inner: SubscriptionManager, config: LiveRowsConfig) -> Self {
        Self {
            inner,
            materializer: LiveRowsMaterializer::new(config),
        }
    }

    /// Receive the next materialized live-query event.
    pub async fn next(&mut self) -> Option<Result<LiveRowsEvent>> {
        loop {
            let event = self.inner.next().await?;
            match event {
                Ok(change) => {
                    if let Some(update) = self.materializer.apply(change) {
                        return Some(Ok(update));
                    }
                },
                Err(error) => return Some(Err(error)),
            }
        }
    }

    /// Close the underlying subscription.
    pub async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }

    /// Get the server-assigned subscription ID.
    pub fn subscription_id(&self) -> &str {
        self.inner.subscription_id()
    }
}
