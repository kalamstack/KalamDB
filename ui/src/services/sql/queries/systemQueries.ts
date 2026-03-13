export const SYSTEM_SETTINGS_QUERY = `
  SELECT name, value, description, category
  FROM system.settings
  ORDER BY category, name
`;

export const SYSTEM_USERS_QUERY = `
  SELECT user_id, username, role, email, auth_type, auth_data, storage_mode, storage_id,
         failed_login_attempts, locked_until, last_login_at, last_seen,
         created_at, updated_at, deleted_at
  FROM system.users
  WHERE deleted_at IS NULL
  ORDER BY username
`;

export const SYSTEM_STATS_QUERY = `
  SELECT metric_name, metric_value
  FROM system.stats
`;

export const getDbaStatsQuery = () => `
  SELECT sampled_at, metric_name, metric_value
  FROM dba.stats
  WHERE metric_name IN (
    'active_connections',
    'active_subscriptions',
    'memory_usage_mb',
    'cpu_usage_percent',
    'total_jobs',
    'jobs_running',
    'jobs_queued',
    'total_tables',
    'total_namespaces',
    'open_files_total',
    'open_files_regular'
  )
  ORDER BY sampled_at DESC
  LIMIT 5000
`;
