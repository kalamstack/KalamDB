use kalamdb_dba::{
    models::{NotificationRow, StatsRow},
    NotificationsRepository, SharedTableRepository, StatsRepository,
};

#[test]
fn shared_repository_api_is_exposed() {
    let _shared_stats_ctor = SharedTableRepository::<StatsRow>::new;
    let _shared_notifications_ctor = SharedTableRepository::<NotificationRow>::new;
    let _stats_ctor = StatsRepository::new;
    let _notifications_ctor = NotificationsRepository::new;
}
