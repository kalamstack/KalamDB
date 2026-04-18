#!/bin/bash
# KalamDB Cluster Management Script
#
# This script manages a 3-node KalamDB cluster in either local or Docker mode.
#
# Usage:
#   ./scripts/cluster.sh [--docker] <command>
#
# Modes:
#   --local   Run cluster locally (default, native processes)
#   --docker  Run cluster in Docker containers
#
# Commands:
#   start      Start the 3-node cluster
#   stop       Stop all nodes
#   restart    Restart the cluster
#   status     Check cluster status
#   logs [N]   View logs from node N (1, 2, or 3) or all nodes
#   clean      Stop and remove all data
#   build      Build the server binary (local) or Docker image (docker)
#   test       Run cluster tests (cli/tests/cluster)
#   smoke      Run smoke tests against leader (local mode only)
#   smoke-all  Run smoke tests against all nodes (local mode only)
#   verify     Run consistency verification tests (local mode only)
#   full       Run complete test suite (local mode only)
#   shell N    Open shell in node N (docker mode only)
#
# Examples:
#   ./scripts/cluster.sh start           # Start local cluster
#   ./scripts/cluster.sh --docker start  # Start Docker cluster
#   ./scripts/cluster.sh logs 1          # View node 1 logs
#   ./scripts/cluster.sh --docker shell 2 # Open shell in Docker node 2
#
# Prerequisites (local mode):
#   - Rust toolchain installed (cargo, rustc)
#   - Port 8081, 8082, 8083 available (HTTP)
#   - Port 9081, 9082, 9083 available (Raft RPC)
#
# Prerequisites (docker mode):
#   - Docker and Docker Compose installed
#   - Docker daemon running

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse mode flag
MODE="local"
if [[ "$1" == "--docker" ]]; then
    MODE="docker"
    shift
elif [[ "$1" == "--local" ]]; then
    MODE="local"
    shift
fi

CLUSTER_DATA_DIR="$PROJECT_ROOT/.cluster-local"
DOCKER_CLUSTER_DIR="$PROJECT_ROOT/docker/run/cluster"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Node configuration
NODE1_HTTP=8081
NODE1_RPC=9081
NODE1_ID=1

NODE2_HTTP=8082
NODE2_RPC=9082
NODE2_ID=2

NODE3_HTTP=8083
NODE3_RPC=9083
NODE3_ID=3

# Cluster bootstrap always provisions the root account up front.
# Override for local development with KALAMDB_ROOT_PASSWORD=... ./scripts/cluster.sh start
ROOT_PASSWORD="${KALAMDB_ROOT_PASSWORD:-kalamdb123}"
ADMIN_PASSWORD="${KALAMDB_ADMIN_PASSWORD:-$ROOT_PASSWORD}"
CLUSTER_URLS="http://127.0.0.1:$NODE1_HTTP,http://127.0.0.1:$NODE2_HTTP,http://127.0.0.1:$NODE3_HTTP"

print_header() {
    echo ""
    echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════════╗${NC}"
    if [ "$MODE" = "docker" ]; then
        echo -e "${BLUE}║        KalamDB Cluster (Docker Mode)                              ║${NC}"
    else
        echo -e "${BLUE}║        KalamDB Cluster (Local Mode)                               ║${NC}"
    fi
    echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

check_rust() {
    if ! command -v cargo &> /dev/null; then
        echo -e "${RED}Error: Cargo is not installed${NC}"
        echo "Please install Rust: https://rustup.rs/"
        exit 1
    fi
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Error: Docker is not installed${NC}"
        echo "Install Docker: https://docs.docker.com/get-docker/"
        exit 1
    fi
    if ! docker info &> /dev/null; then
        echo -e "${RED}Error: Docker daemon is not running${NC}"
        exit 1
    fi
}

# ===================================================================
# DOCKER MODE FUNCTIONS
# ===================================================================

docker_build_image() {
    print_header
    echo -e "${YELLOW}Building KalamDB Docker image from local source...${NC}"
    echo ""
    
    echo "Project root: $PROJECT_ROOT"
    echo "Building image (this may take several minutes)..."
    docker build -f "$PROJECT_ROOT/docker/build/Dockerfile" -t jamals86/kalamdb:latest "$PROJECT_ROOT"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Image built successfully${NC}"
        echo ""
        echo "Image: jamals86/kalamdb:latest"
        docker images jamals86/kalamdb:latest --format "Size: {{.Size}}, Created: {{.CreatedSince}}"
    else
        echo -e "${RED}✗ Image build failed${NC}"
        exit 1
    fi
}

docker_start_cluster() {
    print_header
    echo -e "${GREEN}Starting 3-node KalamDB cluster in Docker...${NC}"
    echo ""
    
    # Check if image exists
    echo "Checking for kalamdb image..."
    if ! docker image inspect jamals86/kalamdb:latest &> /dev/null; then
        echo -e "${YELLOW}Image not found locally. Pulling...${NC}"
        docker pull jamals86/kalamdb:latest || {
            echo -e "${RED}Failed to pull image. Try building locally: $0 --docker build${NC}"
            exit 1
        }
    fi
    
    # Start cluster
    cd "$DOCKER_CLUSTER_DIR"
    docker compose up -d
    
    echo ""
    echo -e "${GREEN}Cluster starting...${NC}"
    echo "Waiting for nodes to be healthy..."
    
    # Wait for HTTP health checks (process up)
    for i in {1..15}; do
        node1_ok=$(curl -sf http://localhost:$NODE1_HTTP/v1/api/healthcheck 2>/dev/null && echo "1" || echo "0")
        node2_ok=$(curl -sf http://localhost:$NODE2_HTTP/v1/api/healthcheck 2>/dev/null && echo "1" || echo "0")
        node3_ok=$(curl -sf http://localhost:$NODE3_HTTP/v1/api/healthcheck 2>/dev/null && echo "1" || echo "0")
        
        if [[ "$node1_ok" == "1" && "$node2_ok" == "1" && "$node3_ok" == "1" ]]; then
            echo ""
            echo -e "${GREEN}✓ All nodes are responding (HTTP up)${NC}"
            break
        fi
        
        echo -n "."
        sleep 1
    done
    
    echo ""

    echo "Waiting for cluster to become Raft-ready (leader elected, node active)..."
    for i in {1..30}; do
        if curl -sf "http://localhost:${NODE1_HTTP}/v1/api/cluster/health" 2>/dev/null | grep -q '"status":"healthy"'; then
            echo -e "${GREEN}✓ Cluster is healthy${NC}"
            break
        fi
        echo -n "."
        sleep 1
    done

    if ! ensure_admin_user "http://127.0.0.1:$NODE1_HTTP"; then
        echo -e "${RED}Failed to provision default admin user on Docker cluster startup.${NC}"
        exit 1
    fi

    docker_show_status
}

docker_stop_cluster() {
    print_header
    echo -e "${YELLOW}Stopping Docker cluster (data preserved)...${NC}"
    cd "$DOCKER_CLUSTER_DIR"
    docker compose down
    echo -e "${GREEN}✓ Cluster stopped${NC}"
}

docker_restart_cluster() {
    print_header
    echo -e "${YELLOW}Restarting Docker cluster...${NC}"
    cd "$DOCKER_CLUSTER_DIR"
    docker compose restart
    echo -e "${GREEN}✓ Cluster restarted${NC}"
    sleep 5
    docker_show_status
}

docker_show_status() {
    print_header
    echo -e "${BLUE}Cluster Status (Docker):${NC}"
    echo ""
    
    # Check each node
    for node in 1 2 3; do
        port=$((8080 + node))
        container="kalamdb-node${node}"
        
        # Check if container is running
        if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
            # Check HTTP health (process up)
            if curl -sf "http://localhost:${port}/v1/api/healthcheck" &> /dev/null; then
                echo -e "  Node $node (port $port): ${GREEN}● HTTP up${NC}"
            else
                echo -e "  Node $node (port $port): ${YELLOW}○ Starting...${NC}"
            fi
        else
            echo -e "  Node $node (port $port): ${RED}✗ Not running${NC}"
        fi
    done
    
    echo ""
    echo -e "${BLUE}Docker containers:${NC}"
    cd "$DOCKER_CLUSTER_DIR"
    docker compose ps
    echo ""
    echo -e "${BLUE}Connection URLs:${NC}"
    echo "  Node 1: http://localhost:$NODE1_HTTP"
    echo "  Node 2: http://localhost:$NODE2_HTTP"
    echo "  Node 3: http://localhost:$NODE3_HTTP"
    echo ""
}

docker_show_logs() {
    local node=$1
    cd "$DOCKER_CLUSTER_DIR"
    if [[ -n "$node" ]]; then
        echo -e "${BLUE}Logs for Node $node:${NC}"
        docker compose logs -f "kalamdb-node${node}"
    else
        echo -e "${BLUE}Logs for all nodes:${NC}"
        docker compose logs -f
    fi
}

docker_clean_cluster() {
    print_header
    echo -e "${RED}WARNING: This will delete all Docker cluster data!${NC}"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cd "$DOCKER_CLUSTER_DIR"
        docker compose down -v
        echo -e "${GREEN}✓ Cluster stopped and data removed${NC}"
    else
        echo "Cancelled"
    fi
}

docker_shell() {
    local node=$1
    if [[ -z "$node" || ! "$node" =~ ^[123]$ ]]; then
        echo -e "${RED}Usage: $0 --docker shell <1|2|3>${NC}"
        exit 1
    fi
    echo -e "${BLUE}Opening shell in Node $node...${NC}"
    docker exec -it "kalamdb-node${node}" /bin/bash
}

# ===================================================================
# LOCAL MODE FUNCTIONS (existing functions preserved)
# ==================================================================

build_server() {
    print_header
    echo -e "${YELLOW}Building KalamDB server...${NC}"
    cd "$PROJECT_ROOT/backend"
    cargo build --release
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Server built successfully${NC}"
    else
        echo -e "${RED}✗ Build failed${NC}"
        exit 1
    fi
}

# ===================================================================
# LOCAL MODE CLUSTER OPERATIONS
# ===================================================================

create_node_config() {
    local node_id=$1
    local http_port=$2
    local rpc_port=$3
    local data_dir="$CLUSTER_DATA_DIR/node$node_id"
    local config_file="$data_dir/server.toml"

    mkdir -p "$data_dir"
    mkdir -p "$data_dir/data"
    mkdir -p "$data_dir/logs"

    # Build peer list (all nodes except self)
    local peer_blocks=""
    if [ "$node_id" != "1" ]; then
        peer_blocks="${peer_blocks}
[[cluster.peers]]
node_id = 1
rpc_addr = \"127.0.0.1:$NODE1_RPC\"
api_addr = \"http://127.0.0.1:$NODE1_HTTP\"
"
    fi
    if [ "$node_id" != "2" ]; then
        peer_blocks="${peer_blocks}
[[cluster.peers]]
node_id = 2
rpc_addr = \"127.0.0.1:$NODE2_RPC\"
api_addr = \"http://127.0.0.1:$NODE2_HTTP\"
"
    fi
    if [ "$node_id" != "3" ]; then
        peer_blocks="${peer_blocks}
[[cluster.peers]]
node_id = 3
rpc_addr = \"127.0.0.1:$NODE3_RPC\"
api_addr = \"http://127.0.0.1:$NODE3_HTTP\"
"
    fi

    cat > "$config_file" << EOF
# KalamDB Node $node_id Configuration
# Auto-generated by cluster.sh

[server]
host = "127.0.0.1"
port = $http_port
workers = 0
api_version = "v1"

[storage]
rocksdb_path = "$data_dir/data/rocksdb"
default_storage_path = "$data_dir/data/storage"
shared_tables_template = "{namespace}/{tableName}"
user_tables_template = "{namespace}/{tableName}/{userId}"

[limits]
max_message_size = 1048576
max_query_limit = 1000
default_query_limit = 50

[logging]
level = "info"
logs_path = "$data_dir/logs"
format = "json"

[performance]
request_timeout = 30
keepalive_timeout = 75
max_connections = 25000
backlog = 2048
worker_max_blocking_threads = 512
client_request_timeout = 5
client_disconnect_timeout = 2
max_header_size = 16384

[rate_limit]
max_queries_per_sec = 10000
max_messages_per_sec = 1000
max_subscriptions_per_user = 1000

[cluster]
enabled = true
cluster_id = "local-cluster"
node_id = $node_id
rpc_addr = "127.0.0.1:$rpc_port"
api_addr = "http://127.0.0.1:$http_port"
heartbeat_interval_ms = 150
election_timeout_ms = [300, 500]
replication_timeout_ms = 5000

# Note: Raft uses quorum by design; there is no min_replication_nodes setting.

$peer_blocks

[auth]
enabled = true
root_password = "$ROOT_PASSWORD"
jwt_secret = "local-cluster-jwt-secret-key-for-testing-only"
jwt_expiry_hours = 24
EOF

    echo "$config_file"
}

start_node() {
    local node_id=$1
    local http_port=$2
    local rpc_port=$3

    echo -e "${YELLOW}Starting node $node_id...${NC}"

    local config_file=$(create_node_config $node_id $http_port $rpc_port)
    local data_dir="$CLUSTER_DATA_DIR/node$node_id"
    local pid_file="$data_dir/server.pid"
    local log_file="$data_dir/logs/stdout.log"

    # Check if already running
    if [ -f "$pid_file" ]; then
        local old_pid=$(cat "$pid_file")
        if kill -0 "$old_pid" 2>/dev/null; then
            echo -e "${YELLOW}  Node $node_id already running (PID: $old_pid)${NC}"
            return 0
        fi
        rm -f "$pid_file"
    fi

    # Start the server
    local binary="$PROJECT_ROOT/target/release/kalamdb-server"
    if [ ! -f "$binary" ]; then
        binary="$PROJECT_ROOT/target/debug/kalamdb-server"
    fi

    if [ ! -f "$binary" ]; then
        echo -e "${RED}  Server binary not found. Run './scripts/cluster.sh build' first.${NC}"
        exit 1
    fi

    (
        cd "$data_dir"
        KALAMDB_ROOT_PASSWORD="$ROOT_PASSWORD" nohup "$binary" > "$log_file" 2>&1 &
        echo $! > "$pid_file"
    )
    local pid=$(cat "$pid_file")

    # Wait for server to start
    sleep 2

    if kill -0 "$pid" 2>/dev/null; then
        echo -e "${GREEN}  ✓ Node $node_id started (PID: $pid, HTTP: $http_port, RPC: $rpc_port)${NC}"
    else
        echo -e "${RED}  ✗ Node $node_id failed to start. Check $log_file${NC}"
        return 1
    fi
}

stop_node() {
    local node_id=$1
    local data_dir="$CLUSTER_DATA_DIR/node$node_id"
    local pid_file="$data_dir/server.pid"
    local http_port=$((8080 + node_id))
    local rpc_port=$((9080 + node_id))
    local listener_pids=""

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${YELLOW}Stopping node $node_id (PID: $pid)...${NC}"
            kill "$pid" 2>/dev/null || true
            sleep 1
            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
        rm -f "$pid_file"
        echo -e "${GREEN}  ✓ Node $node_id stopped${NC}"
    else
        echo -e "${YELLOW}  Node $node_id not running${NC}"
    fi

    listener_pids=$( (
        lsof -tiTCP:"$http_port" -sTCP:LISTEN 2>/dev/null || true
        lsof -tiTCP:"$rpc_port" -sTCP:LISTEN 2>/dev/null || true
    ) | sort -u )

    if [ -n "$listener_pids" ]; then
        echo -e "${YELLOW}  Cleaning up leftover listener processes for node $node_id...${NC}"
        while IFS= read -r listener_pid; do
            [ -z "$listener_pid" ] && continue
            kill "$listener_pid" 2>/dev/null || true
            sleep 1
            if kill -0 "$listener_pid" 2>/dev/null; then
                kill -9 "$listener_pid" 2>/dev/null || true
            fi
        done <<< "$listener_pids"
        echo -e "${GREEN}  ✓ Freed ports $http_port and $rpc_port${NC}"
    fi
}

check_node_health() {
    local http_port=$1
    curl -sf "http://127.0.0.1:$http_port/v1/api/healthcheck" >/dev/null 2>&1
}

check_cluster_ready() {
    # Cluster is considered ready when cluster health reports status=healthy.
    # This implies a leader is known and this node is active.
    local http_port=$1
    curl -sf "http://127.0.0.1:$http_port/v1/api/cluster/health" 2>/dev/null | grep -q '"status":"healthy"'
}

start_cluster() {
    print_header
    echo -e "${GREEN}Starting 3-node local cluster...${NC}"
    echo ""

    # Check if binary exists
    local binary="$PROJECT_ROOT/target/release/kalamdb-server"
    if [ ! -f "$binary" ]; then
        binary="$PROJECT_ROOT/target/debug/kalamdb-server"
    fi
    if [ ! -f "$binary" ]; then
        echo -e "${YELLOW}Server binary not found. Building...${NC}"
        build_server
    fi

    # Create data directory
    mkdir -p "$CLUSTER_DATA_DIR"

    # Start all nodes
    start_node $NODE1_ID $NODE1_HTTP $NODE1_RPC
    start_node $NODE2_ID $NODE2_HTTP $NODE2_RPC
    start_node $NODE3_ID $NODE3_HTTP $NODE3_RPC

    echo ""
    echo -e "${YELLOW}Waiting for processes to come up...${NC}"

    # Wait for all nodes to be HTTP healthy
    local healthy=0
    for i in {1..30}; do
        local count=0
        check_node_health $NODE1_HTTP && ((count++)) || true
        check_node_health $NODE2_HTTP && ((count++)) || true
        check_node_health $NODE3_HTTP && ((count++)) || true

        if [ "$count" -eq 3 ]; then
            healthy=1
            break
        fi
        sleep 1
    done

    echo ""
    if [ "$healthy" -eq 1 ]; then
        echo -e "${YELLOW}Waiting for cluster to become Raft-ready (leader elected, node active)...${NC}"
        local raft_ready=0
        for i in {1..60}; do
            if check_cluster_ready $NODE1_HTTP; then
                raft_ready=1
                break
            fi
            sleep 1
        done

        if [ "$raft_ready" -ne 1 ]; then
            echo -e "${YELLOW}Cluster is responding but not healthy yet (see /v1/api/cluster/health).${NC}"
        fi

        if ! ensure_admin_user "http://127.0.0.1:$NODE1_HTTP"; then
            echo -e "${RED}Failed to provision default admin user on cluster startup.${NC}"
            exit 1
        fi

        echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║                    Cluster Ready!                                 ║${NC}"
        echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════════╝${NC}"
        echo ""
        echo "Node 1: http://127.0.0.1:$NODE1_HTTP"
        echo "Node 2: http://127.0.0.1:$NODE2_HTTP"
        echo "Node 3: http://127.0.0.1:$NODE3_HTTP"
        echo ""
        echo "Admin UI: http://127.0.0.1:$NODE1_HTTP/ui"
        echo ""
        echo "Authentication bootstrap: cluster.sh already configured the root account"
        echo "Username: root"
        echo "Root password: $ROOT_PASSWORD"
        echo ""
        echo "Setup note: the setup wizard is skipped for clusters started with this script"
        echo ""
        echo "Connect with CLI:"
        echo "  kalam --url http://127.0.0.1:$NODE1_HTTP --user root --password \"$ROOT_PASSWORD\" --save-credentials"
        echo ""
        echo "Run cluster tests:"
        echo "  cd cli && cargo test --test cluster"
    else
        echo -e "${RED}Cluster failed to initialize. Check logs:${NC}"
        echo "  $CLUSTER_DATA_DIR/node1/logs/stdout.log"
        echo "  $CLUSTER_DATA_DIR/node2/logs/stdout.log"
        echo "  $CLUSTER_DATA_DIR/node3/logs/stdout.log"
        exit 1
    fi
}

stop_cluster() {
    print_header
    echo -e "${YELLOW}Stopping cluster...${NC}"
    echo ""

    stop_node 1
    stop_node 2
    stop_node 3

    echo ""
    echo -e "${GREEN}Cluster stopped.${NC}"
}

show_status() {
    print_header
    echo -e "${BLUE}Cluster Status:${NC}"
    echo ""

    for node_id in 1 2 3; do
        local data_dir="$CLUSTER_DATA_DIR/node$node_id"
        local pid_file="$data_dir/server.pid"
        local http_port=$((8080 + node_id))

        echo -n "  Node $node_id: "
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                if check_node_health $http_port; then
                    echo -e "${GREEN}Running (PID: $pid, healthy)${NC}"
                else
                    echo -e "${YELLOW}Running (PID: $pid, not healthy yet)${NC}"
                fi
            else
                echo -e "${RED}Dead (stale PID file)${NC}"
            fi
        else
            echo -e "${RED}Not running${NC}"
        fi
    done

    echo ""
}

show_logs() {
    local node_id=$1
    local data_dir="$CLUSTER_DATA_DIR/node$node_id"
    local log_file="$data_dir/logs/stdout.log"

    if [ -f "$log_file" ]; then
        echo -e "${BLUE}=== Node $node_id Logs ===${NC}"
        tail -100 "$log_file"
    else
        echo -e "${RED}No log file found for node $node_id${NC}"
    fi
}

clean_cluster() {
    print_header
    echo -e "${YELLOW}Cleaning up cluster data...${NC}"

    stop_cluster

    if [ -d "$CLUSTER_DATA_DIR" ]; then
        rm -rf "$CLUSTER_DATA_DIR"
        echo -e "${GREEN}✓ Removed $CLUSTER_DATA_DIR${NC}"
    fi

    echo -e "${GREEN}Cleanup complete.${NC}"
}

ensure_cluster_healthy() {
    local count=0
    check_node_health $NODE1_HTTP && ((count++)) || true
    check_node_health $NODE2_HTTP && ((count++)) || true
    check_node_health $NODE3_HTTP && ((count++)) || true

    if [ "$count" -lt 3 ]; then
        echo -e "${RED}Cluster is not healthy (healthy nodes: $count). Start the cluster first.${NC}"
        exit 1
    fi
}

get_access_token() {
    local base_url="${1:-http://127.0.0.1:$NODE1_HTTP}"
    local response

    response=$(curl -fsS -X POST "$base_url/v1/api/auth/login" \
        -H "Content-Type: application/json" \
        -d "{\"user\":\"root\",\"password\":\"$ROOT_PASSWORD\"}") || return 1

    python3 - "$response" << 'PY'
import json
import sys

if len(sys.argv) < 2:
    sys.exit(1)

try:
    data = json.loads(sys.argv[1])
except json.JSONDecodeError:
    sys.exit(1)

token = data.get("access_token")
if token:
    print(token)
    sys.exit(0)

sys.exit(1)
PY
}

sql_escape() {
    printf '%s' "$1" | sed "s/'/''/g"
}

ensure_admin_user() {
    local base_url="${1:-http://127.0.0.1:$NODE1_HTTP}"
    local access_token
    local user_check
    local admin_password_sql

    access_token=$(get_access_token "$base_url") || return 1
    admin_password_sql=$(sql_escape "$ADMIN_PASSWORD")

    user_check=$(
        curl -fsS \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $access_token" \
            -d '{"sql":"SELECT username FROM system.users WHERE username = '\''admin'\'' LIMIT 1"}' \
            "$base_url/v1/api/sql"
    ) || return 1

    if echo "$user_check" | grep -q '"row_count":1'; then
        curl -fsS \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $access_token" \
            -d "{\"sql\":\"ALTER USER admin SET PASSWORD '$admin_password_sql'\"}" \
            "$base_url/v1/api/sql" >/dev/null || return 1

        curl -fsS \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $access_token" \
            -d '{"sql":"ALTER USER admin SET ROLE '\''dba'\''"}' \
            "$base_url/v1/api/sql" >/dev/null || return 1
    else
        curl -fsS \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $access_token" \
            -d "{\"sql\":\"CREATE USER admin WITH PASSWORD '$admin_password_sql' ROLE 'dba'\"}" \
            "$base_url/v1/api/sql" >/dev/null || return 1
    fi
}

run_cluster_tests() {
    print_header
    ensure_cluster_healthy
    ensure_admin_user "http://127.0.0.1:$NODE1_HTTP"
    echo -e "${YELLOW}Running cluster tests...${NC}"
    echo ""

    cd "$PROJECT_ROOT/cli"
    KALAMDB_SERVER_TYPE="cluster" \
    KALAMDB_SERVER_URL="http://127.0.0.1:$NODE1_HTTP" \
    KALAMDB_CLUSTER_URLS="$CLUSTER_URLS" \
    KALAMDB_ROOT_PASSWORD="$ROOT_PASSWORD" \
    KALAMDB_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
    RUST_TEST_THREADS=1 \
    cargo test --features e2e-tests --test cluster -- --nocapture
}

run_smoke_tests() {
    print_header
    ensure_cluster_healthy
    ensure_admin_user "http://127.0.0.1:$NODE1_HTTP"
    echo -e "${YELLOW}Detecting cluster leader for smoke tests...${NC}"
    echo ""

    cd "$PROJECT_ROOT/cli"

    local leader_url
    leader_url=$(detect_leader_url || true)
    if [ -z "$leader_url" ]; then
        leader_url="http://127.0.0.1:$NODE1_HTTP"
        echo -e "${YELLOW}⚠️  Could not detect leader. Falling back to node 1: $leader_url${NC}"
    else
        echo -e "${GREEN}✓ Leader detected: $leader_url${NC}"
    fi

    KALAMDB_SERVER_URL="$leader_url" \
    KALAMDB_SERVER_TYPE="cluster" \
    KALAMDB_CLUSTER_URLS="$CLUSTER_URLS" \
    KALAMDB_ROOT_PASSWORD="$ROOT_PASSWORD" \
    KALAMDB_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
    RUST_TEST_THREADS=1 \
        cargo test --features e2e-tests --test smoke -- --nocapture
}

run_smoke_tests_all_nodes() {
    print_header
    ensure_cluster_healthy
    ensure_admin_user "http://127.0.0.1:$NODE1_HTTP"
    echo -e "${YELLOW}Running smoke tests against ALL nodes...${NC}"
    echo ""

    cd "$PROJECT_ROOT/cli"

    local nodes=("http://127.0.0.1:$NODE1_HTTP" "http://127.0.0.1:$NODE2_HTTP" "http://127.0.0.1:$NODE3_HTTP")
    local passed=0
    local failed=0

    for node_url in "${nodes[@]}"; do
        echo ""
        echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
        echo -e "${BLUE}Testing node: $node_url${NC}"
        echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
        echo ""

        if KALAMDB_SERVER_URL="$node_url" \
              KALAMDB_SERVER_TYPE="cluster" \
              KALAMDB_CLUSTER_URLS="$CLUSTER_URLS" \
           KALAMDB_ROOT_PASSWORD="$ROOT_PASSWORD" \
              KALAMDB_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
           RUST_TEST_THREADS=1 \
           cargo test --features e2e-tests --test smoke smoke_test_core_operations -- --nocapture; then
            echo -e "${GREEN}✓ Core operations passed on $node_url${NC}"
            ((passed++))
        else
            echo -e "${RED}✗ Core operations failed on $node_url${NC}"
            ((failed++))
        fi
    done

    echo ""
    echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                    Smoke Tests Complete                           ║${NC}"
    echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Passed: $passed / ${#nodes[@]}"
    echo "Failed: $failed / ${#nodes[@]}"

    if [ "$failed" -gt 0 ]; then
        exit 1
    fi
}

run_verification_tests() {
    print_header
    ensure_cluster_healthy
    ensure_admin_user "http://127.0.0.1:$NODE1_HTTP"
    echo -e "${YELLOW}Running consistency verification tests...${NC}"
    echo ""

    cd "$PROJECT_ROOT/cli"

    echo -e "${BLUE}Running system tables replication tests...${NC}"
    KALAMDB_SERVER_TYPE="cluster" \
    KALAMDB_SERVER_URL="http://127.0.0.1:$NODE1_HTTP" \
    KALAMDB_CLUSTER_URLS="$CLUSTER_URLS" \
    KALAMDB_ROOT_PASSWORD="$ROOT_PASSWORD" \
    KALAMDB_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
    RUST_TEST_THREADS=1 \
        cargo test --features e2e-tests --test cluster cluster_test_system_tables -- --nocapture

    echo ""
    echo -e "${BLUE}Running subscription tests...${NC}"
    KALAMDB_SERVER_TYPE="cluster" \
    KALAMDB_SERVER_URL="http://127.0.0.1:$NODE1_HTTP" \
    KALAMDB_CLUSTER_URLS="$CLUSTER_URLS" \
    KALAMDB_ROOT_PASSWORD="$ROOT_PASSWORD" \
    KALAMDB_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
    RUST_TEST_THREADS=1 \
        cargo test --features e2e-tests --test cluster cluster_test_subscription -- --nocapture

    echo ""
    echo -e "${BLUE}Running table identity tests...${NC}"
    KALAMDB_SERVER_TYPE="cluster" \
    KALAMDB_SERVER_URL="http://127.0.0.1:$NODE1_HTTP" \
    KALAMDB_CLUSTER_URLS="$CLUSTER_URLS" \
    KALAMDB_ROOT_PASSWORD="$ROOT_PASSWORD" \
    KALAMDB_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
    RUST_TEST_THREADS=1 \
        cargo test --features e2e-tests --test cluster cluster_test_table_identity -- --nocapture

    echo ""
    echo -e "${BLUE}Running final consistency tests...${NC}"
    KALAMDB_SERVER_TYPE="cluster" \
    KALAMDB_SERVER_URL="http://127.0.0.1:$NODE1_HTTP" \
    KALAMDB_CLUSTER_URLS="$CLUSTER_URLS" \
    KALAMDB_ROOT_PASSWORD="$ROOT_PASSWORD" \
    KALAMDB_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
    RUST_TEST_THREADS=1 \
        cargo test --features e2e-tests --test cluster cluster_test_final -- --nocapture

    echo ""
    echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║               Verification Tests Complete                         ║${NC}"
    echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════════╝${NC}"
}

run_full_test_suite() {
    print_header
    ensure_cluster_healthy
    echo -e "${YELLOW}Running FULL cluster test suite...${NC}"
    echo ""
    echo "This includes:"
    echo "  1. All cluster replication tests"
    echo "  2. Smoke tests on all nodes"
    echo "  3. Consistency verification"
    echo ""

    local start_time=$(date +%s)

    # Run cluster tests
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}PHASE 1: Cluster Tests${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
    run_cluster_tests

    # Run smoke tests on all nodes
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}PHASE 2: Multi-Node Smoke Tests${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
    run_smoke_tests_all_nodes

    # Run verification tests
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}PHASE 3: Consistency Verification${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
    run_verification_tests

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    echo ""
    echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                 FULL TEST SUITE COMPLETE                          ║${NC}"
    echo -e "${GREEN}╠═══════════════════════════════════════════════════════════════════╣${NC}"
    echo -e "${GREEN}║  Duration: ${duration}s                                                     ${NC}"
    echo -e "${GREEN}║  All tests passed!                                                ║${NC}"
    echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════════╝${NC}"
}

detect_leader_url() {
    local query="SELECT api_addr, is_leader FROM system.cluster"
    local response
    local access_token

    access_token=$(get_access_token) || return 1

    response=$(
        curl -fsS \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $access_token" \
            -d "{\"sql\":\"$query\"}" \
            "http://127.0.0.1:$NODE1_HTTP/v1/api/sql"
    ) || return 1

    if [ -z "$response" ]; then
        return 1
    fi

    python3 - "$response" << 'PY'
import json
import sys

if len(sys.argv) < 2:
    sys.exit(1)

try:
    data = json.loads(sys.argv[1])
except json.JSONDecodeError:
    sys.exit(1)

results = data.get("results") or []
if not results:
    sys.exit(1)

rows = results[0].get("rows") or []
for row in rows:
    if len(row) >= 2 and row[1] is True:
        api_addr = row[0]
        if api_addr:
            print(api_addr)
            sys.exit(0)

sys.exit(1)
PY
}

# ===================================================================
# MAIN COMMAND HANDLER
# ===================================================================

show_help() {
    print_header
    echo "Usage: $0 [--local|--docker] <command> [options]"
    echo ""
    echo "Modes:"
    echo "  --local   Run cluster locally (default, native processes)"
    echo "  --docker  Run cluster in Docker containers"
    echo ""
    echo "Commands:"
    echo "  build        Build server binary (local) or Docker image (docker)"
    echo "  start        Start the 3-node cluster"
    echo "  stop         Stop all nodes"
    echo "  restart      Restart the cluster"
    echo "  status       Show cluster status"
    echo "  logs [N]     View logs from node N (1, 2, or 3) or all nodes"
    echo "  clean        Stop cluster and remove all data"
    echo "  test         Run cluster tests (cli/tests/cluster)"
    echo "  smoke        Run smoke tests against leader (local only)"
    echo "  smoke-all    Run smoke tests against all nodes (local only)"
    echo "  verify       Run consistency verification tests (local only)"
    echo "  full         Run complete test suite (local only)"
    echo "  shell N      Open shell in node N (docker only)"
    echo ""
    echo "Examples:"
    echo "  $0 start                    # Start local cluster"
    echo "  $0 --docker start           # Start Docker cluster"
    echo "  $0 --docker build           # Build Docker image"
    echo "  $0 logs 1                   # View node 1 logs"
    echo "  $0 --docker shell 2         # Open shell in Docker node 2"
    echo "  $0 test                     # Run tests against running cluster"
    echo ""
}

# Main command handler
if [ "$MODE" = "docker" ]; then
    check_docker
    
    case "${1:-}" in
        start)
            docker_start_cluster
            ;;
        stop)
            docker_stop_cluster
            ;;
        restart)
            docker_restart_cluster
            ;;
        status)
            docker_show_status
            ;;
        logs)
            docker_show_logs "$2"
            ;;
        clean)
            docker_clean_cluster
            ;;
        build)
            docker_build_image
            ;;
        shell)
            docker_shell "$2"
            ;;
        test)
            echo -e "${YELLOW}Note: Tests run against any available cluster (Docker or local)${NC}"
            echo ""
            run_cluster_tests
            ;;
        help|--help|-h|"")
            show_help
            ;;
        smoke|smoke-all|verify|full)
            echo -e "${RED}Error: '$1' command is only available in local mode${NC}"
            echo "Use: $0 $1 (without --docker flag)"
            exit 1
            ;;
        *)
            echo -e "${RED}Unknown command: $1${NC}"
            show_help
            exit 1
            ;;
    esac
else
    # Local mode
    case "${1:-}" in
        start)
            start_cluster
            ;;
        stop)
            stop_cluster
            ;;
        restart)
            stop_cluster
            sleep 2
            start_cluster
            ;;
        status)
            show_status
            ;;
        logs)
            if [ -z "${2:-}" ]; then
                echo "Usage: $0 logs <node_number>"
                echo "  node_number: 1, 2, or 3"
                exit 1
            fi
            show_logs "$2"
            ;;
        clean)
            clean_cluster
            ;;
        build)
            build_server
            ;;
        test)
            run_cluster_tests
            ;;
        smoke)
            run_smoke_tests
            ;;
        smoke-all)
            run_smoke_tests_all_nodes
            ;;
        verify)
            run_verification_tests
            ;;
        full)
            run_full_test_suite
            ;;
        shell)
            echo -e "${RED}Error: 'shell' command is only available in Docker mode${NC}"
            echo "Use: $0 --docker shell <node_number>"
            exit 1
            ;;
        help|--help|-h|"")
            show_help
            ;;
        *)
            echo -e "${RED}Unknown command: $1${NC}"
            show_help
            exit 1
            ;;
    esac
fi
