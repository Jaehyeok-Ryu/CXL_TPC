#!/bin/bash

# ==============================================================================
# run_citus_cluster.sh
# ==============================================================================
# This script spawns a highly-optimized Citus PostgreSQL cluster on a 2-socket server
# with CPU and Memory pinning (DDR, CXL, Interleave, Weighted Interleave).
#
# Topology:
#   - Coordinator: Socket 0 DDR-only (for transactional orchestration)
#   - Worker 1: Pinned to Socket 0 (CPU 0-15,32-47) | Node 0 (DDR) & Node 2 (CXL)
#   - Worker 2: Pinned to Socket 1 (CPU 16-31,48-63) | Node 1 (DDR) & Node 3 (CXL)
# ==============================================================================

set -e

# Default values
POLICY="interleave"
WEIGHT_RATIO="3:1" # DDR:CXL weight ratio
SCRIPT_DIR="/home/sawi/cxl_TPC"
IMAGE_NAME="citusdata/citus:12.1"
DATA_DIR="${SCRIPT_DIR}/data"

show_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --policy <ddr-only | cxl-only | interleave | weighted>  (default: interleave)"
    echo "      - ddr-only: Use only local DDR memory on each socket"
    echo "      - cxl-only: Use only direct CXL memory on each socket"
    echo "      - interleave: Allocate pages 1:1 between local DDR and CXL"
    echo "      - weighted: Cross-allocate pages based on weight ratio (--weight-ratio)"
    echo ""
    echo "  --weight-ratio <DDR_WEIGHT:CXL_WEIGHT>                 (default: 3:1)"
    echo "      - Weight ratio applied when using 'weighted' policy (e.g. 3:1, 4:1)"
    echo ""
    echo "  -h, --help                                             Show help"
    exit 0
}

# Parse parameters
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --policy) POLICY="$2"; shift ;;
        --weight-ratio) WEIGHT_RATIO="$2"; shift ;;
        -h|--help) show_help ;;
        *) echo "[ERROR] Unknown option: $1"; show_help ;;
    esac
    shift
done

echo "===================================================================="
echo "🚀 Spawning High-Performance Citus TPC-H Cluster"
echo "   - Memory Policy : ${POLICY^^}"
if [ "$POLICY" = "weighted" ]; then
echo "   - Weight Ratio  : System-Configured Default (all)"
fi
echo "   - Base Image    : $IMAGE_NAME"
echo "===================================================================="

# 1. Hardware topology configuration
# Socket 0
SOCKET0_CPUS="0-15,32-47"
SOCKET0_DDR_NODE=0
SOCKET0_CXL_NODE=2

# Socket 1
SOCKET1_CPUS="16-31,48-63"
SOCKET1_DDR_NODE=1
SOCKET1_CXL_NODE=3

# Configure mempolicy parameters for numactl
case $POLICY in
    ddr-only)
        MEM_NODE_0="0"
        MEM_NODE_1="1"
        NUMA_FLAG_0="--membind=0"
        NUMA_FLAG_1="--membind=1"
        ;;
    cxl-only)
        MEM_NODE_0="2"
        MEM_NODE_1="3"
        NUMA_FLAG_0="--membind=2"
        NUMA_FLAG_1="--membind=3"
        ;;
    interleave)
        MEM_NODE_0="all"
        MEM_NODE_1="all"
        NUMA_FLAG_0="--interleave=all"
        NUMA_FLAG_1="--interleave=all"
        ;;
    weighted)
        MEM_NODE_0="all"
        MEM_NODE_1="all"
        NUMA_FLAG_0="--weighted-interleave=all"
        NUMA_FLAG_1="--weighted-interleave=all"
        echo "[INFO] Running under numactl --weighted-interleave using pre-configured system/HMSDK weights (all)."
        ;;
    *)
        echo "[ERROR] Unknown policy: $POLICY"
        exit 1
        ;;
esac

# 2. Cleanup existing containers and networks
CONTAINER_COORD="citus_coordinator"
CONTAINER_WORKER1="citus_worker_1"
CONTAINER_WORKER2="citus_worker_2"

echo "[INFO] Cleaning up old cluster containers..."
docker rm -f "$CONTAINER_COORD" "$CONTAINER_WORKER1" "$CONTAINER_WORKER2" >/dev/null 2>&1 || true

echo "[INFO] Creating custom Docker bridge network..."
docker network rm citus-net >/dev/null 2>&1 || true
docker network create citus-net

# 3. Clean and recreate isolated storage directories inside workspace
echo "[INFO] Preparing storage volumes..."
if [ -d "${DATA_DIR}" ]; then
    docker run --rm -v "${DATA_DIR}":/data "$IMAGE_NAME" rm -rf /data/coordinator /data/worker1 /data/worker2 || true
fi
mkdir -p "${DATA_DIR}/coordinator" "${DATA_DIR}/worker1" "${DATA_DIR}/worker2"
# Fix local permissions to let PostgreSQL daemon inside container write to it
chmod -R 777 "${DATA_DIR}"

# 4. PostgreSQL Tuning Parameters (Optimized for pure in-memory workload and extreme bandwidth saturation)
# Disabling fsync/synchronous_commit, enabling aggressive LLVM JIT compile, and raising workers to maximum 
# limits forces PostgreSQL to scan RAM at its absolute hardware throughput ceiling.
PG_COMMON_OPTS="-c fsync=off -c synchronous_commit=off -c checkpoint_timeout=1h -c max_wal_size=100GB -c min_wal_size=20GB -c autovacuum=off -c max_worker_processes=256 -c jit=on -c jit_above_cost=0 -c jit_inline_above_cost=0 -c jit_optimize_above_cost=0"

# 5. Start Citus Coordinator (Runs on Socket 0 DDR for quick orchestration)
echo "[INFO] Starting Citus Coordinator on host port 5432..."
docker run -d \
    --name "$CONTAINER_COORD" \
    --network citus-net \
    --cpuset-cpus="$SOCKET0_CPUS" \
    --cpuset-mems="0" \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=postgres \
    -v "${DATA_DIR}/coordinator":/var/lib/postgresql/data \
    "$IMAGE_NAME" \
    postgres -c shared_buffers=8GB -c work_mem=2GB -c citus.max_adaptive_executor_pool_size=128 -c citus.max_shared_pool_size=512 $PG_COMMON_OPTS

# 6. Start Worker 1 (Socket 0 - bound with customized memory policy)
echo "[INFO] Starting Worker 1 (Socket 0) on host port 5433..."
echo "       - CPU Pinning: $SOCKET0_CPUS"
echo "       - Memory policy: numactl $NUMA_FLAG_0"
docker run -d \
    --name "$CONTAINER_WORKER1" \
    --network citus-net \
    --privileged \
    -p 5433:5432 \
    -e POSTGRES_PASSWORD=postgres \
    -v /usr/local/bin/numactl:/usr/local/bin/numactl \
    -v /usr/local/lib/libnuma.so.1:/usr/lib/x86_64-linux-gnu/libnuma.so.1 \
    -v "${DATA_DIR}/worker1":/var/lib/postgresql/data \
    --entrypoint /usr/local/bin/numactl \
    "$IMAGE_NAME" \
    --physcpubind="$SOCKET0_CPUS" \
    $NUMA_FLAG_0 \
    docker-entrypoint.sh postgres -c shared_buffers=32GB -c work_mem=3GB -c max_parallel_workers_per_gather=16 -c max_parallel_workers=64 -c max_worker_processes=128 $PG_COMMON_OPTS

# 7. Start Worker 2 (Socket 1 - bound with customized memory policy)
echo "[INFO] Starting Worker 2 (Socket 1) on host port 5434..."
echo "       - CPU Pinning: $SOCKET1_CPUS"
echo "       - Memory policy: numactl $NUMA_FLAG_1"
docker run -d \
    --name "$CONTAINER_WORKER2" \
    --network citus-net \
    --privileged \
    -p 5434:5432 \
    -e POSTGRES_PASSWORD=postgres \
    -v /usr/local/bin/numactl:/usr/local/bin/numactl \
    -v /usr/local/lib/libnuma.so.1:/usr/lib/x86_64-linux-gnu/libnuma.so.1 \
    -v "${DATA_DIR}/worker2":/var/lib/postgresql/data \
    --entrypoint /usr/local/bin/numactl \
    "$IMAGE_NAME" \
    --physcpubind="$SOCKET1_CPUS" \
    $NUMA_FLAG_1 \
    docker-entrypoint.sh postgres -c shared_buffers=32GB -c work_mem=3GB -c max_parallel_workers_per_gather=16 -c max_parallel_workers=64 -c max_worker_processes=128 $PG_COMMON_OPTS

# 8. Wait for Coordinator and Workers to become healthy
echo "[INFO] Waiting for database services to initialize..."
for i in {1..30}; do
    READY_COORD=0
    READY_W1=0
    READY_W2=0
    
    if docker exec "$CONTAINER_COORD" pg_isready -U postgres >/dev/null 2>&1; then
        if docker exec "$CONTAINER_COORD" psql -U postgres -d postgres -t -A -c "SELECT 1 FROM pg_extension WHERE extname = 'citus';" 2>/dev/null | grep -q "1"; then
            READY_COORD=1
        fi
    fi
    if docker exec "$CONTAINER_WORKER1" pg_isready -U postgres >/dev/null 2>&1; then
        if docker exec "$CONTAINER_WORKER1" psql -U postgres -d postgres -t -A -c "SELECT 1 FROM pg_extension WHERE extname = 'citus';" 2>/dev/null | grep -q "1"; then
            READY_W1=1
        fi
    fi
    if docker exec "$CONTAINER_WORKER2" pg_isready -U postgres >/dev/null 2>&1; then
        if docker exec "$CONTAINER_WORKER2" psql -U postgres -d postgres -t -A -c "SELECT 1 FROM pg_extension WHERE extname = 'citus';" 2>/dev/null | grep -q "1"; then
            READY_W2=1
        fi
    fi
    
    if [ $READY_COORD -eq 1 ] && [ $READY_W1 -eq 1 ] && [ $READY_W2 -eq 1 ]; then
        echo "[SUCCESS] All PostgreSQL/Citus instances are fully operational!"
        break
    fi
    
    if [ $i -eq 30 ]; then
        echo "[ERROR] Database startup timeout. Printing container states:"
        docker ps -a
        docker logs "$CONTAINER_COORD" | tail -n 20
        docker logs "$CONTAINER_WORKER1" | tail -n 20
        docker logs "$CONTAINER_WORKER2" | tail -n 20
        exit 1
    fi
    sleep 2
done

# 9. Form the Citus Cluster by registering worker nodes
echo "[INFO] Registering Workers on Coordinator..."
docker exec -i "$CONTAINER_COORD" psql -U postgres -d postgres -c "SELECT citus_add_node('citus_worker_1', 5432);" >/dev/null
docker exec -i "$CONTAINER_COORD" psql -U postgres -d postgres -c "SELECT citus_add_node('citus_worker_2', 5432);" >/dev/null

echo "===================================================================="
echo "🎉 Citus 2-Socket Isolated Cluster Active!"
echo "===================================================================="
echo "👉 Coordinator (Socket 0 DDR Only)"
echo "   - Container: $CONTAINER_COORD | Port: 5432"
echo "👉 Worker 1 (Socket 0 - CPUs $SOCKET0_CPUS | Memory nodes $MEM_NODE_0)"
echo "   - Container: $CONTAINER_WORKER1 | Port: 5433"
echo "👉 Worker 2 (Socket 1 - CPUs $SOCKET1_CPUS | Memory nodes $MEM_NODE_1)"
echo "   - Container: $CONTAINER_WORKER2 | Port: 5434"
echo "===================================================================="
echo "💡 To connect: pgcli -h localhost -p 5432 -U postgres -d postgres"
echo "===================================================================="
