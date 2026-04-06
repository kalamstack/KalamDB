//! Proxy-based integration tests that simulate real network outages
//! by routing the client through a local TCP proxy.
//!
//! **IMPORTANT**: These tests require a running KalamDB server (auto-started).

mod common;

#[path = "proxied/ack_before_first_batch.rs"]
mod ack_before_first_batch;
#[path = "proxied/blackhole_during_subscribe.rs"]
mod blackhole_during_subscribe;
#[path = "proxied/double_outage.rs"]
mod double_outage;
#[path = "proxied/event_counter_integrity.rs"]
mod event_counter_integrity;
#[path = "proxied/gradual_degradation.rs"]
mod gradual_degradation;
#[path = "proxied/heavy_write_burst_recovery.rs"]
mod heavy_write_burst_recovery;
#[path = "proxied/helpers.rs"]
mod helpers;
#[path = "proxied/large_snapshot_repeated_outages.rs"]
mod large_snapshot_repeated_outages;
#[path = "proxied/latency_during_snapshot.rs"]
mod latency_during_snapshot;
#[path = "proxied/live_updates_resume.rs"]
mod live_updates_resume;
#[path = "proxied/loading_resume_with_live_writes.rs"]
mod loading_resume_with_live_writes;
#[path = "proxied/mixed_stage_recovery.rs"]
mod mixed_stage_recovery;
#[path = "proxied/multi_sub_bounce.rs"]
mod multi_sub_bounce;
#[path = "proxied/rapid_flap.rs"]
mod rapid_flap;
#[path = "proxied/server_down_connecting.rs"]
mod server_down_connecting;
#[path = "proxied/server_down_initial_load.rs"]
mod server_down_initial_load;
#[path = "proxied/socket_drop_resume.rs"]
mod socket_drop_resume;
#[path = "proxied/staggered_outages.rs"]
mod staggered_outages;
#[path = "proxied/subscribe_during_reconnect.rs"]
mod subscribe_during_reconnect;
#[path = "proxied/transport_impairments.rs"]
mod transport_impairments;
#[path = "proxied/unsubscribe_during_outage.rs"]
mod unsubscribe_during_outage;
#[path = "proxied/update_delete_resume.rs"]
mod update_delete_resume;
