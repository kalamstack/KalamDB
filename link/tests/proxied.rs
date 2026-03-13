//! Proxy-based integration tests that simulate real network outages
//! by routing the client through a local TCP proxy.
//!
//! **IMPORTANT**: These tests require a running KalamDB server (auto-started).

mod common;

#[path = "proxied/ack_before_first_batch.rs"]
mod ack_before_first_batch;
#[path = "proxied/double_outage.rs"]
mod double_outage;
#[path = "proxied/helpers.rs"]
mod helpers;
#[path = "proxied/large_snapshot_repeated_outages.rs"]
mod large_snapshot_repeated_outages;
#[path = "proxied/live_updates_resume.rs"]
mod live_updates_resume;
#[path = "proxied/loading_resume_with_live_writes.rs"]
mod loading_resume_with_live_writes;
#[path = "proxied/mixed_stage_recovery.rs"]
mod mixed_stage_recovery;
#[path = "proxied/multi_sub_bounce.rs"]
mod multi_sub_bounce;
#[path = "proxied/server_down_connecting.rs"]
mod server_down_connecting;
#[path = "proxied/server_down_initial_load.rs"]
mod server_down_initial_load;
#[path = "proxied/socket_drop_resume.rs"]
mod socket_drop_resume;
#[path = "proxied/staggered_outages.rs"]
mod staggered_outages;
#[path = "proxied/update_delete_resume.rs"]
mod update_delete_resume;
