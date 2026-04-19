import { bigint, boolean, doublePrecision, integer, jsonb, pgTable, text } from 'drizzle-orm/pg-core';
import { sql } from 'drizzle-orm';

export const dba_favorites = pgTable('dba.favorites', {
  id: text('id').notNull(),
  payload: jsonb('payload'),
});

export const dba_notifications = pgTable('dba.notifications', {
  id: text('id').notNull(),
  user_id: text('user_id').notNull(),
  title: text('title').notNull(),
  body: text('body'),
  is_read: boolean('is_read').default(sql``).notNull(),
  created_at: bigint('created_at', { mode: 'number' }).notNull(),
  updated_at: bigint('updated_at', { mode: 'number' }).notNull(),
});

export const dba_stats = pgTable('dba.stats', {
  id: text('id').notNull(),
  node_id: text('node_id').notNull(),
  metric_name: text('metric_name').notNull(),
  metric_value: doublePrecision('metric_value').notNull(),
  metric_unit: text('metric_unit'),
  sampled_at: bigint('sampled_at', { mode: 'number' }).notNull(),
});

export const system_audit_log = pgTable('system.audit_log', {
  audit_id: text('audit_id').notNull(),
  timestamp: bigint('timestamp', { mode: 'number' }).notNull(),
  actor_user_id: text('actor_user_id').notNull(),
  actor_username: text('actor_username').notNull(),
  action: text('action').notNull(),
  target: text('target').notNull(),
  details: text('details'),
  ip_address: text('ip_address'),
  subject_user_id: text('subject_user_id'),
});

export const system_job_nodes = pgTable('system.job_nodes', {
  job_id: text('job_id').notNull(),
  node_id: text('node_id').notNull(),
  status: text('status').notNull(),
  error_message: text('error_message'),
  created_at: bigint('created_at', { mode: 'number' }).default(sql``).notNull(),
  started_at: bigint('started_at', { mode: 'number' }),
  finished_at: bigint('finished_at', { mode: 'number' }),
  updated_at: bigint('updated_at', { mode: 'number' }).default(sql``).notNull(),
});

export const system_jobs = pgTable('system.jobs', {
  job_id: text('job_id').notNull(),
  job_type: text('job_type').notNull(),
  status: text('status').notNull(),
  leader_status: text('leader_status'),
  parameters: jsonb('parameters'),
  message: text('message'),
  exception_trace: text('exception_trace'),
  idempotency_key: text('idempotency_key'),
  queue: text('queue'),
  priority: integer('priority'),
  retry_count: text('retry_count').notNull(),
  max_retries: text('max_retries').notNull(),
  memory_used: text('memory_used'),
  cpu_used: text('cpu_used'),
  created_at: bigint('created_at', { mode: 'number' }).notNull(),
  updated_at: bigint('updated_at', { mode: 'number' }).notNull(),
  started_at: bigint('started_at', { mode: 'number' }),
  finished_at: bigint('finished_at', { mode: 'number' }),
  node_id: text('node_id').notNull(),
  leader_node_id: text('leader_node_id'),
});

export const system_manifest = pgTable('system.manifest', {
  cache_key: text('cache_key').notNull(),
  namespace_id: text('namespace_id').notNull(),
  table_name: text('table_name').notNull(),
  scope: text('scope').notNull(),
  etag: text('etag'),
  last_refreshed: bigint('last_refreshed', { mode: 'number' }).notNull(),
  last_accessed: bigint('last_accessed', { mode: 'number' }).notNull(),
  in_memory: boolean('in_memory').notNull(),
  sync_state: text('sync_state').notNull(),
  manifest_json: jsonb('manifest_json').notNull(),
});

export const system_namespaces = pgTable('system.namespaces', {
  namespace_id: text('namespace_id').notNull(),
  name: text('name').notNull(),
  created_at: bigint('created_at', { mode: 'number' }).notNull(),
  options: jsonb('options'),
  table_count: integer('table_count').notNull(),
});

export const system_schemas = pgTable('system.schemas', {
  table_id: text('table_id').notNull(),
  table_name: text('table_name').notNull(),
  namespace_id: text('namespace_id').notNull(),
  table_type: text('table_type').notNull(),
  created_at: bigint('created_at', { mode: 'number' }).notNull(),
  schema_version: integer('schema_version').notNull(),
  columns: jsonb('columns').notNull(),
  table_comment: text('table_comment'),
  updated_at: bigint('updated_at', { mode: 'number' }).notNull(),
  options: jsonb('options'),
  access_level: text('access_level'),
  is_latest: boolean('is_latest').notNull(),
  storage_id: text('storage_id'),
  use_user_storage: boolean('use_user_storage'),
});

export const system_storages = pgTable('system.storages', {
  storage_id: text('storage_id').notNull(),
  storage_name: text('storage_name').notNull(),
  description: text('description'),
  storage_type: text('storage_type').notNull(),
  base_directory: text('base_directory').notNull(),
  credentials: jsonb('credentials'),
  config_json: jsonb('config_json'),
  shared_tables_template: text('shared_tables_template').notNull(),
  user_tables_template: text('user_tables_template').notNull(),
  created_at: bigint('created_at', { mode: 'number' }).notNull(),
  updated_at: bigint('updated_at', { mode: 'number' }).notNull(),
});

export const system_topic_offsets = pgTable('system.topic_offsets', {
  topic_id: text('topic_id').notNull(),
  group_id: text('group_id').notNull(),
  partition_id: integer('partition_id').notNull(),
  last_acked_offset: text('last_acked_offset').notNull(),
  updated_at: bigint('updated_at', { mode: 'number' }).notNull(),
});

export const system_topics = pgTable('system.topics', {
  topic_id: text('topic_id').notNull(),
  name: text('name').notNull(),
  alias: text('alias'),
  partitions: integer('partitions').notNull(),
  retention_seconds: text('retention_seconds'),
  retention_max_bytes: text('retention_max_bytes'),
  routes: jsonb('routes').notNull(),
  created_at: bigint('created_at', { mode: 'number' }).notNull(),
  updated_at: bigint('updated_at', { mode: 'number' }).notNull(),
});

export const system_users = pgTable('system.users', {
  user_id: text('user_id').notNull(),
  username: text('username').notNull(),
  password_hash: text('password_hash').notNull(),
  role: text('role').notNull(),
  email: text('email'),
  auth_type: text('auth_type').notNull(),
  auth_data: jsonb('auth_data'),
  storage_mode: text('storage_mode').notNull(),
  storage_id: text('storage_id'),
  created_at: bigint('created_at', { mode: 'number' }).notNull(),
  updated_at: bigint('updated_at', { mode: 'number' }).notNull(),
  last_seen: bigint('last_seen', { mode: 'number' }),
  deleted_at: bigint('deleted_at', { mode: 'number' }),
  failed_login_attempts: integer('failed_login_attempts').notNull(),
  locked_until: bigint('locked_until', { mode: 'number' }),
  last_login_at: bigint('last_login_at', { mode: 'number' }),
});

export const system_live = pgTable('system.live', {
  live_id: text('live_id').notNull(),
  connection_id: text('connection_id').notNull(),
  subscription_id: text('subscription_id').notNull(),
  namespace_id: text('namespace_id').notNull(),
  table_name: text('table_name').notNull(),
  user_id: text('user_id').notNull(),
  query: text('query').notNull(),
  options: text('options'),
  status: text('status').notNull(),
  created_at: bigint('created_at', { mode: 'number' }).notNull(),
  last_update: bigint('last_update', { mode: 'number' }).notNull(),
  changes: text('changes').notNull(),
  node_id: text('node_id').notNull(),
  last_ping_at: bigint('last_ping_at', { mode: 'number' }).notNull(),
});

export const system_server_logs = pgTable('system.server_logs', {
  timestamp: text('timestamp').notNull(),
  level: text('level').notNull(),
  thread: text('thread'),
  target: text('target'),
  line: text('line'),
  message: text('message').notNull(),
});

export const system_cluster = pgTable('system.cluster', {
  cluster_id: text('cluster_id').notNull(),
  node_id: text('node_id').notNull(),
  role: text('role').notNull(),
  status: text('status').notNull(),
  rpc_addr: text('rpc_addr').notNull(),
  api_addr: text('api_addr').notNull(),
  is_self: boolean('is_self').notNull(),
  is_leader: boolean('is_leader').notNull(),
  groups_leading: integer('groups_leading').notNull(),
  total_groups: integer('total_groups').notNull(),
  current_term: text('current_term'),
  last_applied_log: text('last_applied_log'),
  leader_last_log_index: text('leader_last_log_index'),
  snapshot_index: text('snapshot_index'),
  catchup_progress_pct: text('catchup_progress_pct'),
  replication_lag: text('replication_lag'),
  hostname: text('hostname'),
  version: text('version'),
  memory_mb: text('memory_mb'),
  memory_usage_mb: text('memory_usage_mb'),
  cpu_usage_percent: text('cpu_usage_percent'),
  uptime_seconds: text('uptime_seconds'),
  uptime_human: text('uptime_human'),
  os: text('os'),
  arch: text('arch'),
});

export const system_settings = pgTable('system.settings', {
  name: text('name').notNull(),
  value: text('value').notNull(),
  description: text('description').notNull(),
  category: text('category').notNull(),
});

export const system_stats = pgTable('system.stats', {
  metric_name: text('metric_name').notNull(),
  metric_value: text('metric_value').notNull(),
});
