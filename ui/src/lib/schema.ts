import { boolean, doublePrecision, integer, jsonb, pgTable, text, timestamp } from 'drizzle-orm/pg-core';
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
  created_at: timestamp('created_at', { mode: 'string' }).notNull(),
  updated_at: timestamp('updated_at', { mode: 'string' }).notNull(),
});

export const dba_stats = pgTable('dba.stats', {
  id: text('id').notNull(),
  node_id: text('node_id').notNull(),
  metric_name: text('metric_name').notNull(),
  metric_value: doublePrecision('metric_value').notNull(),
  metric_unit: text('metric_unit'),
  sampled_at: timestamp('sampled_at', { mode: 'string' }).notNull(),
});

export const dt_ns_mo64yit3_ia9_0_table1_mo64yit3_ia9_0 = pgTable('dt_ns_mo64yit3_ia9_0.table1_mo64yit3_ia9_0', {
  id: text('id').notNull(),
  name: text('name'),
});

export const dt_ns_mo64yit3_ia9_0_table2_mo64yit3_ia9_1 = pgTable('dt_ns_mo64yit3_ia9_0.table2_mo64yit3_ia9_1', {
  id: text('id').notNull(),
  name: text('name'),
});

export const flush_error_ns_mo64yext_i5e_0_stream_no_flush_mo64yext_i5e_0 = pgTable('flush_error_ns_mo64yext_i5e_0.stream_no_flush_mo64yext_i5e_0', {
  event_id: text('event_id').default(sql``).notNull(),
  event_type: text('event_type'),
});

export const flush_manifest_ns_mo64yexv_i5f_0_double_flush_test_mo64yexv_i5f_0 = pgTable('flush_manifest_ns_mo64yexv_i5f_0.double_flush_test_mo64yexv_i5f_0', {
  id: text('id').default(sql``).notNull(),
  data: text('data').notNull(),
});

export const flush_manifest_ns_mo64yexw_i5g_0_shared_flush_test_mo64yexw_i5g_0 = pgTable('flush_manifest_ns_mo64yexw_i5g_0.shared_flush_test_mo64yexw_i5g_0', {
  id: text('id').default(sql``).notNull(),
  config_key: text('config_key').notNull(),
  config_value: text('config_value'),
});

export const flush_manifest_ns_mo64yf1x_i5h_0_user_flush_test_mo64yf1x_i5h_0 = pgTable('flush_manifest_ns_mo64yf1x_i5h_0.user_flush_test_mo64yf1x_i5h_0', {
  id: text('id').default(sql``).notNull(),
  content: text('content').notNull(),
  created_at: timestamp('created_at', { mode: 'string' }).default(sql``),
});

export const live_ns_mo64yipj_ia8_0_messages_mo64yipj_ia8_0 = pgTable('live_ns_mo64yipj_ia8_0.messages_mo64yipj_ia8_0', {
  id: text('id').default(sql``).notNull(),
  content: text('content').notNull(),
});

export const repro_dupe_ns_mo64yb1x_hy1_0_dupe_table_mo64yb1x_hy1_0 = pgTable('repro_dupe_ns_mo64yb1x_hy1_0.dupe_table_mo64yb1x_hy1_0', {
  id: text('id').notNull(),
  name: text('name'),
  col1: text('col1'),
});

export const snowflake_ns_mo64yddc_i3m_0_snowflake_test_mo64yddc_i3m_0 = pgTable('snowflake_ns_mo64yddc_i3m_0.snowflake_test_mo64yddc_i3m_0', {
  id: text('id').default(sql``).notNull(),
  content: text('content').notNull(),
  created_at: timestamp('created_at', { mode: 'string' }).default(sql``),
});

export const sys_tables_ns_mo64yit8_iab_0_shared_tbl_mo64yit8_iab_1 = pgTable('sys_tables_ns_mo64yit8_iab_0.shared_tbl_mo64yit8_iab_1', {
  id: text('id').default(sql``).notNull(),
  config_key: text('config_key').notNull(),
  config_value: text('config_value'),
});

export const sys_tables_ns_mo64yit8_iab_0_stream_tbl_mo64yit8_iab_2 = pgTable('sys_tables_ns_mo64yit8_iab_0.stream_tbl_mo64yit8_iab_2', {
  event_id: text('event_id').default(sql``).notNull(),
  event_type: text('event_type'),
  payload: text('payload'),
});

export const sys_tables_ns_mo64yit8_iab_0_user_tbl_mo64yit8_iab_0 = pgTable('sys_tables_ns_mo64yit8_iab_0.user_tbl_mo64yit8_iab_0', {
  id: text('id').default(sql``).notNull(),
  name: text('name').notNull(),
});

export const system_audit_log = pgTable('system.audit_log', {
  audit_id: text('audit_id').notNull(),
  timestamp: timestamp('timestamp', { mode: 'string' }).notNull(),
  actor_user_id: text('actor_user_id').notNull(),
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
  created_at: timestamp('created_at', { mode: 'string' }).default(sql``).notNull(),
  started_at: timestamp('started_at', { mode: 'string' }),
  finished_at: timestamp('finished_at', { mode: 'string' }),
  updated_at: timestamp('updated_at', { mode: 'string' }).default(sql``).notNull(),
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
  created_at: timestamp('created_at', { mode: 'string' }).notNull(),
  updated_at: timestamp('updated_at', { mode: 'string' }).notNull(),
  started_at: timestamp('started_at', { mode: 'string' }),
  finished_at: timestamp('finished_at', { mode: 'string' }),
  node_id: text('node_id').notNull(),
  leader_node_id: text('leader_node_id'),
});

export const system_manifest = pgTable('system.manifest', {
  cache_key: text('cache_key').notNull(),
  namespace_id: text('namespace_id').notNull(),
  table_name: text('table_name').notNull(),
  scope: text('scope').notNull(),
  etag: text('etag'),
  last_refreshed: timestamp('last_refreshed', { mode: 'string' }).notNull(),
  last_accessed: timestamp('last_accessed', { mode: 'string' }).notNull(),
  in_memory: boolean('in_memory').notNull(),
  sync_state: text('sync_state').notNull(),
  manifest_json: jsonb('manifest_json').notNull(),
});

export const system_namespaces = pgTable('system.namespaces', {
  namespace_id: text('namespace_id').notNull(),
  name: text('name').notNull(),
  created_at: timestamp('created_at', { mode: 'string' }).notNull(),
  options: jsonb('options'),
  table_count: integer('table_count').notNull(),
});

export const system_schemas = pgTable('system.schemas', {
  table_id: text('table_id').notNull(),
  table_name: text('table_name').notNull(),
  namespace_id: text('namespace_id').notNull(),
  table_type: text('table_type').notNull(),
  created_at: timestamp('created_at', { mode: 'string' }).notNull(),
  schema_version: integer('schema_version').notNull(),
  columns: jsonb('columns').notNull(),
  table_comment: text('table_comment'),
  updated_at: timestamp('updated_at', { mode: 'string' }).notNull(),
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
  created_at: timestamp('created_at', { mode: 'string' }).notNull(),
  updated_at: timestamp('updated_at', { mode: 'string' }).notNull(),
});

export const system_topic_offsets = pgTable('system.topic_offsets', {
  topic_id: text('topic_id').notNull(),
  group_id: text('group_id').notNull(),
  partition_id: integer('partition_id').notNull(),
  last_acked_offset: text('last_acked_offset').notNull(),
  updated_at: timestamp('updated_at', { mode: 'string' }).notNull(),
});

export const system_topics = pgTable('system.topics', {
  topic_id: text('topic_id').notNull(),
  name: text('name').notNull(),
  alias: text('alias'),
  partitions: integer('partitions').notNull(),
  retention_seconds: text('retention_seconds'),
  retention_max_bytes: text('retention_max_bytes'),
  routes: jsonb('routes').notNull(),
  created_at: timestamp('created_at', { mode: 'string' }).notNull(),
  updated_at: timestamp('updated_at', { mode: 'string' }).notNull(),
});

export const system_users = pgTable('system.users', {
  user_id: text('user_id').notNull(),
  password_hash: text('password_hash').notNull(),
  role: text('role').notNull(),
  email: text('email'),
  auth_type: text('auth_type').notNull(),
  auth_data: jsonb('auth_data'),
  storage_mode: text('storage_mode').notNull(),
  storage_id: text('storage_id'),
  failed_login_attempts: integer('failed_login_attempts').notNull(),
  locked_until: timestamp('locked_until', { mode: 'string' }),
  last_login_at: timestamp('last_login_at', { mode: 'string' }),
  created_at: timestamp('created_at', { mode: 'string' }).notNull(),
  updated_at: timestamp('updated_at', { mode: 'string' }).notNull(),
  last_seen: timestamp('last_seen', { mode: 'string' }),
  deleted_at: timestamp('deleted_at', { mode: 'string' }),
});

export const ulid_ns_mo64ydh9_i3n_0_ulid_test_mo64ydh9_i3n_0 = pgTable('ulid_ns_mo64ydh9_i3n_0.ulid_test_mo64ydh9_i3n_0', {
  event_id: text('event_id').default(sql``).notNull(),
  event_type: text('event_type').notNull(),
  user_id: text('user_id'),
  payload: text('payload'),
  created_at: timestamp('created_at', { mode: 'string' }).default(sql``),
});

export const uuid_ns_mo64ydkv_i3o_0_uuid_test_mo64ydkv_i3o_0 = pgTable('uuid_ns_mo64ydkv_i3o_0.uuid_test_mo64ydkv_i3o_0', {
  session_id: text('session_id').default(sql``).notNull(),
  user_id: text('user_id').notNull(),
  ip_address: text('ip_address'),
  created_at: timestamp('created_at', { mode: 'string' }).default(sql``),
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
  created_at: timestamp('created_at', { mode: 'string' }).notNull(),
  last_update: timestamp('last_update', { mode: 'string' }).notNull(),
  changes: text('changes').notNull(),
  node_id: text('node_id').notNull(),
  last_ping_at: timestamp('last_ping_at', { mode: 'string' }).notNull(),
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
