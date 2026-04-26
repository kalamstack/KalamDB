use kalamdb_commons::system_tables::{classify_column_family_name, ColumnFamilyProfile};

pub(crate) const SYSTEM_META_CF: &str = "system_meta";
pub(crate) const SYSTEM_INDEX_CF: &str = "system_index";
pub(crate) const HOT_DATA_CF: &str = "hot_data";
pub(crate) const HOT_INDEX_CF: &str = "hot_index";
pub(crate) const RAFT_CF: &str = "raft_data";

const PARTITION_KEY_PREFIX_TAG: u8 = 0x70;
const LOGICAL_PARTITION_REGISTRY_TAG: u8 = 0x71;

pub(crate) fn fixed_column_families() -> &'static [&'static str] {
    &[
        "default",
        SYSTEM_META_CF,
        SYSTEM_INDEX_CF,
        HOT_DATA_CF,
        HOT_INDEX_CF,
        RAFT_CF,
    ]
}

pub(crate) fn physical_cf_for_partition(partition_name: &str) -> &'static str {
    match classify_column_family_name(partition_name) {
        ColumnFamilyProfile::SystemMeta => SYSTEM_META_CF,
        ColumnFamilyProfile::SystemIndex => SYSTEM_INDEX_CF,
        ColumnFamilyProfile::HotData => HOT_DATA_CF,
        ColumnFamilyProfile::HotIndex => HOT_INDEX_CF,
        ColumnFamilyProfile::Raft => RAFT_CF,
    }
}

pub(crate) fn partition_key_prefix(partition_name: &str) -> Vec<u8> {
    let name = partition_name.as_bytes();
    let mut prefix = Vec::with_capacity(1 + std::mem::size_of::<u32>() + name.len());
    prefix.push(PARTITION_KEY_PREFIX_TAG);
    prefix.extend_from_slice(&(name.len() as u32).to_be_bytes());
    prefix.extend_from_slice(name);
    prefix
}

pub(crate) fn physical_key(partition_name: &str, user_key: &[u8]) -> Vec<u8> {
    let mut key = partition_key_prefix(partition_name);
    key.extend_from_slice(user_key);
    key
}

pub(crate) fn logical_partition_registry_prefix() -> Vec<u8> {
    vec![LOGICAL_PARTITION_REGISTRY_TAG]
}

pub(crate) fn logical_partition_registry_key(partition_name: &str) -> Vec<u8> {
    let name = partition_name.as_bytes();
    let mut key = Vec::with_capacity(1 + std::mem::size_of::<u32>() + name.len());
    key.push(LOGICAL_PARTITION_REGISTRY_TAG);
    key.extend_from_slice(&(name.len() as u32).to_be_bytes());
    key.extend_from_slice(name);
    key
}

pub(crate) fn decode_logical_partition_registry_key(key: &[u8]) -> Option<&str> {
    if key.len() < 1 + std::mem::size_of::<u32>() || key[0] != LOGICAL_PARTITION_REGISTRY_TAG {
        return None;
    }

    let len_start = 1;
    let len_end = len_start + std::mem::size_of::<u32>();
    let name_len = u32::from_be_bytes(key[len_start..len_end].try_into().ok()?) as usize;
    let name_end = len_end.checked_add(name_len)?;
    if key.len() != name_end {
        return None;
    }

    std::str::from_utf8(&key[len_end..name_end]).ok()
}

pub(crate) fn next_prefix_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();
    for index in (0..bound.len()).rev() {
        if bound[index] != u8::MAX {
            bound[index] += 1;
            bound.truncate(index + 1);
            return Some(bound);
        }
    }
    None
}
