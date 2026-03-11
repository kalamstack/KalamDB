use kalamdb_system::VectorMetric;

const VIX_MAGIC: &[u8; 5] = b"KVIX1";
const VIX_CODEC_VERSION: u8 = 2;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VixSnapshotEntry {
    pub key: u64,
    pub pk: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VixSnapshotFile {
    pub table_id: String,
    pub column_name: String,
    pub dimensions: u32,
    pub metric: VectorMetric,
    pub generated_at: i64,
    pub last_applied_seq: i64,
    pub next_key: u64,
    pub entries: Vec<VixSnapshotEntry>,
    pub index_blob: Vec<u8>,
}

pub(crate) fn encode_snapshot(snapshot: &VixSnapshotFile) -> Result<Vec<u8>, String> {
    let mut output = Vec::with_capacity(
        256 + snapshot.entries.len().saturating_mul(24) + snapshot.index_blob.len(),
    );
    output.extend_from_slice(VIX_MAGIC);
    output.push(VIX_CODEC_VERSION);

    write_string(&mut output, &snapshot.table_id)?;
    write_string(&mut output, &snapshot.column_name)?;
    write_u32(&mut output, snapshot.dimensions);
    write_u8(&mut output, encode_metric(snapshot.metric)?);
    write_i64(&mut output, snapshot.generated_at);
    write_i64(&mut output, snapshot.last_applied_seq);
    write_u64(&mut output, snapshot.next_key);
    write_u32(
        &mut output,
        u32::try_from(snapshot.entries.len())
            .map_err(|_| "Too many key entries in vector snapshot".to_string())?,
    );
    for entry in &snapshot.entries {
        write_u64(&mut output, entry.key);
        write_string(&mut output, &entry.pk)?;
    }

    write_u32(
        &mut output,
        u32::try_from(snapshot.index_blob.len())
            .map_err(|_| "Index blob too large to encode in vector snapshot".to_string())?,
    );
    output.extend_from_slice(&snapshot.index_blob);

    Ok(output)
}

pub(crate) fn decode_snapshot(input: &[u8]) -> Result<VixSnapshotFile, String> {
    let mut cursor = 0usize;

    let magic = read_bytes(input, &mut cursor, VIX_MAGIC.len())?;
    if magic != VIX_MAGIC {
        return Err("Invalid .vix snapshot magic header".to_string());
    }

    let version = read_u8(input, &mut cursor)?;
    if version != VIX_CODEC_VERSION {
        return Err(format!(
            "Unsupported .vix snapshot codec version {}",
            version
        ));
    }

    let table_id = read_string(input, &mut cursor)?;
    let column_name = read_string(input, &mut cursor)?;
    let dimensions = read_u32(input, &mut cursor)?;
    let metric = decode_metric(read_u8(input, &mut cursor)?)?;
    let generated_at = read_i64(input, &mut cursor)?;
    let last_applied_seq = read_i64(input, &mut cursor)?;
    let next_key = read_u64(input, &mut cursor)?;

    let entries_count = read_u32(input, &mut cursor)?;
    let mut entries = Vec::with_capacity(entries_count as usize);
    for _ in 0..entries_count {
        let key = read_u64(input, &mut cursor)?;
        let pk = read_string(input, &mut cursor)?;
        entries.push(VixSnapshotEntry { key, pk });
    }

    let blob_len = read_u32(input, &mut cursor)? as usize;
    let index_blob = read_bytes(input, &mut cursor, blob_len)?.to_vec();

    if cursor != input.len() {
        return Err("Vector snapshot has trailing bytes".to_string());
    }

    Ok(VixSnapshotFile {
        table_id,
        column_name,
        dimensions,
        metric,
        generated_at,
        last_applied_seq,
        next_key,
        entries,
        index_blob,
    })
}

fn encode_metric(metric: VectorMetric) -> Result<u8, String> {
    Ok(match metric {
        VectorMetric::Cosine => 0,
        VectorMetric::L2 => 1,
        VectorMetric::Dot => 2,
    })
}

fn decode_metric(code: u8) -> Result<VectorMetric, String> {
    match code {
        0 => Ok(VectorMetric::Cosine),
        1 => Ok(VectorMetric::L2),
        2 => Ok(VectorMetric::Dot),
        _ => Err(format!("Unknown vector metric code {}", code)),
    }
}

fn write_u8(output: &mut Vec<u8>, value: u8) {
    output.push(value);
}

fn write_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn write_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn write_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&value.to_le_bytes());
}

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<(), String> {
    write_u32(
        output,
        u32::try_from(value.len())
            .map_err(|_| "String too large to encode in vector snapshot".to_string())?,
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_bytes<'a>(input: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8], String> {
    let start = *cursor;
    let end = start.saturating_add(len);
    if end > input.len() {
        return Err("Unexpected EOF while decoding vector snapshot".to_string());
    }
    *cursor = end;
    Ok(&input[start..end])
}

fn read_u8(input: &[u8], cursor: &mut usize) -> Result<u8, String> {
    Ok(read_bytes(input, cursor, 1)?[0])
}

fn read_u32(input: &[u8], cursor: &mut usize) -> Result<u32, String> {
    let bytes = read_bytes(input, cursor, 4)?;
    let array: [u8; 4] = bytes
        .try_into()
        .map_err(|_| "Failed to decode u32 from vector snapshot".to_string())?;
    Ok(u32::from_le_bytes(array))
}

fn read_u64(input: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let bytes = read_bytes(input, cursor, 8)?;
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| "Failed to decode u64 from vector snapshot".to_string())?;
    Ok(u64::from_le_bytes(array))
}

fn read_i64(input: &[u8], cursor: &mut usize) -> Result<i64, String> {
    let bytes = read_bytes(input, cursor, 8)?;
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| "Failed to decode i64 from vector snapshot".to_string())?;
    Ok(i64::from_le_bytes(array))
}

fn read_string(input: &[u8], cursor: &mut usize) -> Result<String, String> {
    let len = read_u32(input, cursor)? as usize;
    let bytes = read_bytes(input, cursor, len)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|e| format!("Invalid UTF-8 string in vector snapshot: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_codec_roundtrip() {
        let snapshot = VixSnapshotFile {
            table_id: "app.docs".to_string(),
            column_name: "embedding".to_string(),
            dimensions: 3,
            metric: VectorMetric::Cosine,
            generated_at: 12345,
            last_applied_seq: 99,
            next_key: 1000,
            entries: vec![
                VixSnapshotEntry {
                    key: 1,
                    pk: "a".to_string(),
                },
                VixSnapshotEntry {
                    key: 2,
                    pk: "b".to_string(),
                },
            ],
            index_blob: vec![10, 20, 30, 40],
        };

        let encoded = encode_snapshot(&snapshot).expect("encode");
        let decoded = decode_snapshot(&encoded).expect("decode");
        assert_eq!(decoded, snapshot);
    }
}

