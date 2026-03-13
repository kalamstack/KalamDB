use kalamdb_system::VectorMetric;
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

pub(crate) fn create_index(
    dimensions: u32,
    metric: VectorMetric,
    capacity: usize,
) -> Result<Index, String> {
    let mut options = IndexOptions::default();
    options.dimensions = dimensions as usize;
    options.metric = metric_to_usearch(metric);
    options.quantization = ScalarKind::F32;

    let index =
        Index::new(&options).map_err(|e| format!("Failed to create usearch index: {}", e))?;
    if capacity > 0 {
        index
            .reserve(capacity)
            .map_err(|e| format!("Failed to reserve usearch index capacity: {}", e))?;
    }
    Ok(index)
}

pub(crate) fn serialize_index(index: &Index) -> Result<Vec<u8>, String> {
    let serialized_length = index.serialized_length();
    let mut buffer = vec![0u8; serialized_length];
    index
        .save_to_buffer(&mut buffer)
        .map_err(|e| format!("Failed to serialize usearch index: {}", e))?;
    Ok(buffer)
}

pub(crate) fn load_index(
    dimensions: u32,
    metric: VectorMetric,
    blob: &[u8],
) -> Result<Index, String> {
    let index = create_index(dimensions, metric, 0)?;
    index
        .load_from_buffer(blob)
        .map_err(|e| format!("Failed to load usearch index from buffer: {}", e))?;
    Ok(index)
}

pub(crate) fn add_vector(index: &Index, key: u64, vector: &[f32]) -> Result<(), String> {
    index
        .add(key, vector)
        .map_err(|e| format!("Failed to add vector to usearch index: {}", e))
}

pub(crate) fn search_index(
    index: &Index,
    query: &[f32],
    top_k: usize,
) -> Result<Vec<(u64, f32)>, String> {
    let matches = index
        .search(query, top_k)
        .map_err(|e| format!("Failed to search usearch index: {}", e))?;
    Ok(matches.keys.into_iter().zip(matches.distances).collect())
}

pub(crate) fn export_vector(
    index: &Index,
    key: u64,
    dimensions: u32,
) -> Result<Option<Vec<f32>>, String> {
    let mut buffer = Vec::<f32>::new();
    let match_count = index
        .export(key, &mut buffer)
        .map_err(|e| format!("Failed to export vector from usearch index: {}", e))?;
    if match_count == 0 {
        return Ok(None);
    }

    let dims = dimensions as usize;
    if buffer.len() < dims {
        return Err(format!(
            "Exported vector for key {} has {} values, expected at least {}",
            key,
            buffer.len(),
            dims
        ));
    }

    Ok(Some(buffer.into_iter().take(dims).collect()))
}

pub(crate) fn metric_to_usearch(metric: VectorMetric) -> MetricKind {
    match metric {
        VectorMetric::Cosine => MetricKind::Cos,
        VectorMetric::L2 => MetricKind::L2sq,
        VectorMetric::Dot => MetricKind::IP,
    }
}
