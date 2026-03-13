use crate::error::{DbaError, Result};
use crate::mapping::model_to_row;
use crate::models::StatsRow;
use chrono::Utc;
use kalamdb_core::app_context::AppContext;
use kalamdb_core::providers::SharedTableProvider;
use kalamdb_tables::utils::row_utils::system_user_id;
use kalamdb_tables::BaseTableProvider;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::{interval_at, Instant, MissedTickBehavior};

const DEFAULT_STATS_RECORD_INTERVAL: Duration = Duration::from_secs(30);
const DBA_STATS_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(12 * 60 * 60);
const DELETE_BATCH_SIZE: usize = 1_000;

pub async fn start_stats_recorder(app_context: Arc<AppContext>) -> Result<JoinHandle<()>> {
    let recorded = record_stats_snapshot(app_context.clone()).await?;
    log::debug!("Recorded {} startup system.stats samples into dba.stats", recorded);

    if let Err(error) = prune_expired_stats(app_context.clone()).await {
        log::warn!("Failed to prune expired dba.stats samples during startup: {}", error);
    }

    Ok(tokio::spawn(async move {
        let mut ticker = interval_at(
            Instant::now() + DEFAULT_STATS_RECORD_INTERVAL,
            DEFAULT_STATS_RECORD_INTERVAL,
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut next_maintenance_run = Instant::now() + DBA_STATS_MAINTENANCE_INTERVAL;

        loop {
            ticker.tick().await;
            match record_stats_snapshot(app_context.clone()).await {
                Ok(recorded) => {
                    log::debug!(
                        "Recorded {} periodic system.stats samples into dba.stats",
                        recorded
                    );
                },
                Err(error) => {
                    log::warn!("Failed to record dba.stats snapshot: {}", error);
                },
            }

            if Instant::now() >= next_maintenance_run {
                if let Err(error) = prune_expired_stats(app_context.clone()).await {
                    log::warn!("Failed to prune expired dba.stats samples: {}", error);
                }
                next_maintenance_run = Instant::now() + DBA_STATS_MAINTENANCE_INTERVAL;
            }
        }
    }))
}

pub async fn record_stats_snapshot(app_context: Arc<AppContext>) -> Result<usize> {
    let sampled_at = Utc::now().timestamp_millis();
    let node_id = app_context.node_id().to_string();

    let mut rows = Vec::new();
    for (metric_name, metric_value_text) in app_context.compute_metrics() {
        let Some(metric_value) = parse_numeric_metric(&metric_value_text) else {
            continue;
        };

        rows.push(StatsRow {
            id: StatsRow::sample_id(&node_id, &metric_name, sampled_at),
            node_id: node_id.clone(),
            metric_name: metric_name.clone(),
            metric_value,
            metric_unit: infer_metric_unit(&metric_name),
            sampled_at,
        });
    }

    let recorded = rows.len();
    insert_stats_rows_local(app_context.as_ref(), rows).await?;

    Ok(recorded)
}

async fn prune_expired_stats(app_context: Arc<AppContext>) -> Result<usize> {
    let retention_days = app_context.config().retention.dba_stats_retention_days;
    if retention_days == 0 {
        return Ok(0);
    }

    let retention_ms = Duration::from_secs(retention_days * 24 * 60 * 60).as_millis() as i64;
    let cutoff_ms = Utc::now().timestamp_millis() - retention_ms;
    let cutoff_id = StatsRow::cutoff_id(cutoff_ms);
    let table_id = StatsRow::table_id();
    let provider = app_context
        .schema_registry()
        .get_provider(&table_id)
        .ok_or_else(|| DbaError::ProviderMismatch(table_id.to_string()))?;
    let shared_provider = provider
        .as_any()
        .downcast_ref::<SharedTableProvider>()
        .ok_or_else(|| DbaError::ProviderMismatch(table_id.to_string()))?;

    let mut pruned = 0usize;
    loop {
        let expired_ids = shared_provider
            .collect_live_string_primary_keys_before_async(cutoff_id.clone(), DELETE_BATCH_SIZE)
            .await?;
        if expired_ids.is_empty() {
            break;
        }

        let batch_size = expired_ids.len();
        delete_stats_rows_local(shared_provider, expired_ids).await?;
        pruned += batch_size;

        if batch_size < DELETE_BATCH_SIZE {
            break;
        }
    }

    if pruned == 0 {
        return Ok(0);
    }

    log::debug!(
        "Pruned {} expired dba.stats samples older than {} day(s)",
        pruned,
        retention_days
    );

    Ok(pruned)
}

async fn insert_stats_rows_local(app_context: &AppContext, models: Vec<StatsRow>) -> Result<()> {
    if models.is_empty() {
        return Ok(());
    }

    let table_id = StatsRow::table_id();
    let table_def = app_context
        .schema_registry()
        .get_table_if_exists(&table_id)?
        .ok_or_else(|| DbaError::ProviderMismatch(table_id.to_string()))?;
    let provider = app_context
        .schema_registry()
        .get_provider(&table_id)
        .ok_or_else(|| DbaError::ProviderMismatch(table_id.to_string()))?;
    let shared_provider = provider
        .as_any()
        .downcast_ref::<SharedTableProvider>()
        .ok_or_else(|| DbaError::ProviderMismatch(table_id.to_string()))?;
    let rows = models
        .iter()
        .map(|model| model_to_row(model, table_def.as_ref()))
        .collect::<Result<Vec<_>>>()?;

    shared_provider
        .insert_batch(system_user_id(), rows)
        .await
        .map(|_| ())
        .map_err(DbaError::from)
}

async fn delete_stats_rows_local(
    shared_provider: &SharedTableProvider,
    ids: Vec<String>,
) -> Result<()> {
    shared_provider.hard_delete_string_primary_keys_async(ids).await?;
    Ok(())
}

fn parse_numeric_metric(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    trimmed.parse::<f64>().ok().filter(|parsed| parsed.is_finite())
}

fn infer_metric_unit(metric_name: &str) -> Option<String> {
    let unit = if metric_name.ends_with("_mb") {
        "MB"
    } else if metric_name.ends_with("_percent") {
        "percent"
    } else if metric_name.ends_with("_seconds") {
        "seconds"
    } else if metric_name.ends_with("_bytes") {
        "bytes"
    } else if metric_name.ends_with("_count")
        || metric_name.starts_with("total_")
        || metric_name.starts_with("jobs_")
        || metric_name.starts_with("open_files_")
        || metric_name.starts_with("active_")
    {
        "count"
    } else {
        return None;
    };

    Some(unit.to_string())
}
