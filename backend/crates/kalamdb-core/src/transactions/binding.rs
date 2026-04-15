use kalamdb_commons::models::NodeId;
use kalamdb_sharding::GroupId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionRaftBinding {
    LocalSingleNode,
    UnboundCluster,
    BoundCluster {
        group_id: GroupId,
        leader_node_id: NodeId,
    },
}
