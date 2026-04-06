//! Cluster test server for testing multiple node scenarios
use super::http_server::HttpTestServer;
use anyhow::Result;
use kalam_client::models::QueryResponse;
use rand::RngExt;
use tokio::sync::Mutex;

/// A test cluster with 3 nodes for testing replication and consistency.
pub struct ClusterTestServer {
    /// Three independent server instances that form a cluster
    pub nodes: Vec<HttpTestServer>,
    /// Track which nodes are currently online (for offline testing)
    node_states: std::sync::Arc<Mutex<Vec<bool>>>,
}

impl ClusterTestServer {
    /// Get a reference to a node in the cluster (0, 1, or 2)
    pub fn get_node(&self, index: usize) -> Result<&HttpTestServer> {
        self.nodes
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("Node index {} out of range (0-2)", index))
    }

    /// Check if a node is currently online
    pub async fn is_node_online(&self, index: usize) -> Result<bool> {
        let states = self.node_states.lock().await;
        states
            .get(index)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Node index {} out of range (0-2)", index))
    }

    /// Wait for a node to be online (polls every 100ms, max 10 seconds)
    pub async fn wait_for_node_online(&self, index: usize) -> Result<()> {
        let max_attempts = 100; // 10 seconds
        for attempt in 0..max_attempts {
            let online = self.is_node_online(index).await?;
            if online {
                eprintln!("✅ Node {} is online (attempt {})", index, attempt + 1);
                return Ok(());
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        Err(anyhow::anyhow!("Node {} did not come online after 10 seconds", index))
    }

    /// Take a node offline (simulates network failure/shutdown)
    pub async fn take_node_offline(&self, index: usize) -> Result<()> {
        let mut states = self.node_states.lock().await;
        if let Some(state) = states.get_mut(index) {
            *state = false;
            eprintln!("🔴 Node {} is now OFFLINE", index);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Node index {} out of range (0-2)", index))
        }
    }

    /// Bring a node back online
    pub async fn bring_node_online(&self, index: usize) -> Result<()> {
        let mut states = self.node_states.lock().await;
        if let Some(state) = states.get_mut(index) {
            *state = true;
            eprintln!("🟢 Node {} is now ONLINE", index);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Node index {} out of range (0-2)", index))
        }
    }

    /// Execute SQL on a random online node in the cluster
    pub async fn execute_sql_on_random(&self, sql: &str) -> Result<QueryResponse> {
        let states = self.node_states.lock().await;
        let mut rng = rand::rng();

        // Find all online nodes
        let online_indices: Vec<usize> = states
            .iter()
            .enumerate()
            .filter_map(|(i, &online)| if online { Some(i) } else { None })
            .collect();

        drop(states); // Release lock before executing SQL

        if online_indices.is_empty() {
            return Err(anyhow::anyhow!("No online nodes available in cluster"));
        }

        let index = online_indices[rng.random_range(0..online_indices.len())];
        eprintln!("   📍 Executing on random node {}", index);
        self.nodes[index].execute_sql(sql).await
    }

    /// Execute SQL on all online nodes and return results
    pub async fn execute_sql_on_all(&self, sql: &str) -> Result<Vec<QueryResponse>> {
        let states = self.node_states.lock().await;
        let mut results = Vec::new();
        for (i, node) in self.nodes.iter().enumerate() {
            if states[i] {
                results.push(node.execute_sql(sql).await?);
            }
        }
        drop(states);
        Ok(results)
    }

    /// Execute SQL on all online nodes and return results with node indices
    pub async fn execute_sql_on_all_with_indices(
        &self,
        sql: &str,
    ) -> Result<Vec<(usize, QueryResponse)>> {
        let states = self.node_states.lock().await;
        let mut results = Vec::new();
        for (i, node) in self.nodes.iter().enumerate() {
            if states[i] {
                results.push((i, node.execute_sql(sql).await?));
            }
        }
        drop(states);
        Ok(results)
    }

    /// Check data consistency across all online nodes for a query
    pub async fn verify_data_consistency(&self, query: &str) -> Result<bool> {
        let results = self.execute_sql_on_all(query).await?;

        if results.is_empty() {
            return Ok(true);
        }

        // Check if all nodes returned the same data
        let first_result = &results[0];
        for (i, result) in results.iter().enumerate().skip(1) {
            if result.results.len() != first_result.results.len() {
                eprintln!(
                    "❌ Node consistency failed: Node 0 has {} result sets, Node {} has {}",
                    first_result.results.len(),
                    i,
                    result.results.len()
                );
                return Ok(false);
            }

            for (j, (first_rows, result_rows)) in
                first_result.results.iter().zip(result.results.iter()).enumerate()
            {
                let first_maps = first_rows.rows_as_maps();
                let result_maps = result_rows.rows_as_maps();

                if first_maps.len() != result_maps.len() {
                    eprintln!(
                        "❌ Node consistency failed: Node 0 result set {} has {} rows, Node {} has {}",
                        j,
                        first_maps.len(),
                        i,
                        result_maps.len()
                    );
                    return Ok(false);
                }

                // Basic row comparison (convert to strings for comparison)
                for (k, (first_map, result_map)) in
                    first_maps.iter().zip(result_maps.iter()).enumerate()
                {
                    let first_str = format!("{:?}", first_map);
                    let result_str = format!("{:?}", result_map);

                    if first_str != result_str {
                        eprintln!(
                            "❌ Node consistency failed: Node 0 row {} differs from Node {} row {}",
                            k, i, k
                        );
                        eprintln!("   Node 0: {}", first_str);
                        eprintln!("   Node {}: {}", i, result_str);
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }

    /// Create a new ClusterTestServer from nodes
    pub fn new(nodes: Vec<HttpTestServer>) -> Self {
        ClusterTestServer {
            nodes,
            node_states: std::sync::Arc::new(Mutex::new(vec![true, true, true])),
        }
    }
}
