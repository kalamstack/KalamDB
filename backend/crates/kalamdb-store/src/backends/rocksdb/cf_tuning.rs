use kalamdb_commons::system_tables::{classify_column_family_name, ColumnFamilyProfile};
use kalamdb_configs::{RocksDbCfProfileSettings, RocksDbSettings};
use rocksdb::Options;

pub(crate) fn apply_db_settings(db_opts: &mut Options, settings: &RocksDbSettings) {
    let default_cf = &settings.cf_profiles.system_meta;
    db_opts.set_write_buffer_size(default_cf.write_buffer_size);
    db_opts.set_max_write_buffer_number(default_cf.max_write_buffers);
    db_opts.set_max_background_jobs(settings.max_background_jobs);
    db_opts.increase_parallelism(settings.max_background_jobs);
    db_opts.set_max_open_files(settings.max_open_files);
}

pub(crate) fn apply_cf_settings(cf_opts: &mut Options, settings: &RocksDbSettings, cf_name: &str) {
    let profile_settings = profile_settings(settings, classify_column_family_name(cf_name));
    cf_opts.set_write_buffer_size(profile_settings.write_buffer_size);
    cf_opts.set_max_write_buffer_number(profile_settings.max_write_buffers);
}

fn profile_settings(
    settings: &RocksDbSettings,
    profile: ColumnFamilyProfile,
) -> &RocksDbCfProfileSettings {
    match profile {
        ColumnFamilyProfile::SystemMeta => &settings.cf_profiles.system_meta,
        ColumnFamilyProfile::SystemIndex => &settings.cf_profiles.system_index,
        ColumnFamilyProfile::HotData => &settings.cf_profiles.hot_data,
        ColumnFamilyProfile::HotIndex => &settings.cf_profiles.hot_index,
        ColumnFamilyProfile::Raft => &settings.cf_profiles.raft,
    }
}
