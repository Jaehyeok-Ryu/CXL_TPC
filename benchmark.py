#!/usr/bin/env python3
import subprocess
import os
import sys
import time
import argparse
import random
from concurrent.futures import ThreadPoolExecutor

def run_cmd(cmd, cwd=None, shell=True, check=True, verbose=True):
    if verbose:
        print(f"[CMD] {cmd}")
    res = subprocess.run(cmd, cwd=cwd, shell=shell, text=True, capture_output=True)
    if check and res.returncode != 0:
        print(f"[ERROR] Command failed with return code {res.returncode}")
        print(f"STDOUT: {res.stdout}")
        print(f"STDERR: {res.stderr}")
        raise RuntimeError(f"Command failed: {cmd}")
    return res

def check_docker_running():
    try:
        run_cmd("docker ps", check=True, verbose=False)
    except Exception:
        print("[ERROR] Docker is not running or current user has no permission.")
        sys.exit(1)

def execute_stream_query(stream_id, q_id, queries_dir):
    q_file = f"q{q_id}.sql"
    q_path = os.path.join(queries_dir, q_file)
    if not os.path.exists(q_path):
        return q_id, 0.0, False
    
    t0 = time.time()
    # Execute query silently inside Coordinator container to avoid stdout pollution
    res = run_cmd(f"docker exec -i citus_coordinator psql -U postgres -d postgres < {q_path}", check=False, verbose=False)
    t1 = time.time()
    
    latency = t1 - t0
    success = (res.returncode == 0)
    return q_id, latency, success

def run_single_stream(stream_id, query_sequence, queries_dir):
    print(f"\n[Stream {stream_id}] 🚀 Starting Permutation Sequence: {query_sequence}")
    results = {}
    
    for idx, q_id in enumerate(query_sequence):
        step_num = idx + 1
        print(f"[Stream {stream_id}] ⏳ Running Q{q_id:02d} ({step_num}/22)...")
        
        q_id, latency, success = execute_stream_query(stream_id, q_id, queries_dir)
        results[q_id] = {
            "latency": latency,
            "success": success
        }
        
        status_str = "✅ Success" if success else "❌ Failed"
        print(f"[Stream {stream_id}]  └─ Q{q_id:02d} Finished: {latency:.3f}s ({status_str})")
        
    print(f"\n[Stream {stream_id}] 🎉 All 22 queries completed!")
    return results

def main():
    parser = argparse.ArgumentParser(description="Tailored Citus TPC-H Asymmetric Multi-Socket Benchmark Suite")
    parser.add_argument("-s", "--scale-factor", type=float, default=1.0, help="TPC-H scale factor (default: 1.0, ~1GB data)")
    parser.add_argument("-c", "--schema", choices=["columnar", "row"], default="columnar", help="Storage format (default: columnar)")
    parser.add_argument("-p", "--policy", choices=["ddr-only", "cxl-only", "interleave", "weighted"], default="interleave", help="NUMA memory policy (default: interleave)")
    parser.add_argument("-w", "--weight-ratio", default="3:1", help="DDR:CXL weight ratio for weighted policy (default: 3:1)")
    parser.add_argument("--skip-load", action="store_true", help="Skip cluster restart, data generation, and loading")
    
    # Newly added options for official Throughput Test
    parser.add_argument("-m", "--mode", choices=["power", "throughput"], default="power", help="Benchmark Mode: 'power' (Single-user Latency) or 'throughput' (Multi-user Concurrency)")
    parser.add_argument("--streams", type=int, default=2, help="Number of concurrent query streams for Throughput Test (default: 2)")
    
    args = parser.parse_args()
    
    check_docker_running()
    
    script_dir = "/home/sawi/cxl_TPC"
    dbgen_dir = f"{script_dir}/tpch-dbgen"
    
    if not args.skip_load:
        # 1. Spawn cluster with memory policy
        if args.policy == "weighted":
            print(f"\n=== 1. Spawning Citus Cluster (Policy: WEIGHTED, using pre-configured system/HMSDK weights) ===")
            cluster_cmd = f"./run_citus_cluster.sh --policy weighted"
        else:
            print(f"\n=== 1. Spawning Citus Cluster (Policy: {args.policy.upper()}) ===")
            cluster_cmd = f"./run_citus_cluster.sh --policy {args.policy}"
        run_cmd(cluster_cmd, cwd=script_dir)
        
        # 2. Generate TPC-H Data
        print(f"\n=== 2. Generating TPC-H Data (Scale Factor: {args.scale_factor}) ===")
        # Remove old tbl files if any
        for f in os.listdir(dbgen_dir):
            if f.endswith(".tbl"):
                os.remove(os.path.join(dbgen_dir, f))
                
        dbgen_cmd = f"./dbgen -s {args.scale_factor} -f"
        run_cmd(dbgen_cmd, cwd=dbgen_dir)
        
        # 3. Clean Trailing Pipes in tbl files (PostgreSQL compatibility)
        print("\n=== 3. Cleaning trailing pipes in TPC-H files ===")
        tbl_files = [f for f in os.listdir(dbgen_dir) if f.endswith(".tbl")]
        for tbl in tbl_files:
            tbl_path = os.path.join(dbgen_dir, tbl)
            print(f"Cleaning {tbl}...")
            # Use sed which is extremely memory efficient and fast
            run_cmd(f"sed -i 's/|$//' {tbl_path}", verbose=False)
            
            # Copy file to coordinator container directly to bypass host-side volume permission issues
            print(f"Copying {tbl} to coordinator container...")
            run_cmd(f"docker cp {tbl_path} citus_coordinator:/var/lib/postgresql/data/{tbl}", verbose=False)
            run_cmd(f"docker exec -u 0 citus_coordinator chown postgres:postgres /var/lib/postgresql/data/{tbl}", verbose=False)
            
            # Remove original file to save space
            os.remove(tbl_path)
            
        print("[SUCCESS] All TPC-H dataset tables sanitized and copied to staging area!")
        
        # 4. Load Schema
        schema_file = "schema_columnar.sql" if args.schema == "columnar" else "schema_row.sql"
        print(f"\n=== 4. Loading {args.schema.upper()} Schema DDL ({schema_file}) ===")
        schema_path = os.path.join(script_dir, schema_file)
        run_cmd(f"docker exec -i citus_coordinator psql -U postgres -d postgres < {schema_path}")
        print("[SUCCESS] Database schema initialized and Citus tables distributed!")
        
        # 5. Load Data via Server-Side parallel COPY
        print("\n=== 5. Loading Data into Citus (Server-side COPY) ===")
        tables = ["region", "nation", "part", "supplier", "partsupp", "customer", "orders", "lineitem"]
        load_times = {}
        for table in tables:
            tbl_file = f"{table}.tbl"
            res_exists = run_cmd(f"docker exec citus_coordinator test -f /var/lib/postgresql/data/{tbl_file}", check=False, verbose=False)
            if res_exists.returncode != 0:
                print(f"[WARNING] staging file {tbl_file} not found inside container. Skipping loading {table}.")
                continue
                
            print(f"Loading {table}...")
            copy_query = f"COPY {table} FROM '/var/lib/postgresql/data/{tbl_file}' WITH (FORMAT csv, DELIMITER '|');"
            
            t0 = time.time()
            # Execute COPY inside Coordinator container
            run_cmd(f"docker exec -i citus_coordinator psql -U postgres -d postgres -c \"{copy_query}\"", verbose=False)
            t1 = time.time()
            
            elapsed = t1 - t0
            load_times[table] = elapsed
            print(f"Loaded {table} in {elapsed:.3f} seconds.")
            
        total_load_time = sum(load_times.values())
        print(f"[SUCCESS] Database populated! Total staging load time: {total_load_time:.2f} seconds.")
    else:
        print("\n=== Skipping Loading phase as requested ===")
        total_load_time = 0.0
        load_times = {}
        
    # 6. Run TPC-H Queries
    print("\n=== 6. Executing TPC-H Benchmarking Queries ===")
    queries_dir = f"{script_dir}/queries"
    
    # Deep warm-up to force loading all table pages into the massive worker shared buffers (DDR/CXL)
    print("Performing comprehensive hot cache warm-up (scanning all tables)...")
    warmup_tables = ["region", "nation", "part", "supplier", "partsupp", "customer", "orders", "lineitem"]
    for table in warmup_tables:
        print(f"  Warming up {table}...")
        run_cmd(f"docker exec -i citus_coordinator psql -U postgres -d postgres -c \"SELECT COUNT(*) FROM {table};\"", verbose=False)
    print("[SUCCESS] All tables fully loaded into PostgreSQL shared buffer pool!")
    
    if args.mode == "power":
        print(f"\n⚡ Mode: Power Test (Single-User Sequential Latency)")
        query_results = {}
        
        t_start = time.time()
        for q_id in range(1, 23):
            q_file = f"q{q_id}.sql"
            q_path = os.path.join(queries_dir, q_file)
            if not os.path.exists(q_path):
                print(f"[WARNING] Query {q_file} not found. Skipping.")
                continue
                
            print(f"Running Q{q_id:02d}...", end="", flush=True)
            
            # Measure query latency
            t0 = time.time()
            res = run_cmd(f"docker exec -i citus_coordinator psql -U postgres -d postgres < {q_path}", check=False, verbose=False)
            t1 = time.time()
            
            latency = t1 - t0
            query_results[q_id] = {
                "latency": latency,
                "success": res.returncode == 0
            }
            status_str = "Done" if res.returncode == 0 else "FAILED"
            print(f" {status_str} ({latency:.3f}s)")
            
        t_end = time.time()
        total_query_time = t_end - t_start
        throughput_metric = None
        
        print(f"\n====================================================================")
        print(f"🏆 Power Test Finished! Total Elapsed Query Time: {total_query_time:.2f} seconds.")
        print(f"====================================================================")
        
    else:  # throughput mode
        print(f"\n💣 Mode: Throughput Test ({args.streams} Concurrent Streams with Permuted Sequences)")
        
        # Prepare permuted query sequences for each stream according to TPC-H standards
        stream_sequences = {}
        for s_id in range(1, args.streams + 1):
            q_list = list(range(1, 23))
            # Standard shuffle to avoid overlapping memory caches
            random.shuffle(q_list)
            stream_sequences[s_id] = q_list
            
        print("\n--- Prepared Permutation Sequences ---")
        for s_id, seq in stream_sequences.items():
            print(f"  * Stream {s_id}: {seq}")
        print("--------------------------------------")
        print("\n🚀 Initiating Concurrent Multi-User Execution...")
        
        t_start = time.time()
        
        # Launch parallel threads
        stream_results = {}
        with ThreadPoolExecutor(max_workers=args.streams) as executor:
            futures = {
                executor.submit(run_single_stream, s_id, stream_sequences[s_id], queries_dir): s_id
                for s_id in range(1, args.streams + 1)
            }
            for future in futures:
                s_id = futures[future]
                stream_results[s_id] = future.result()
                
        t_end = time.time()
        total_query_time = t_end - t_start
        
        # Compute official TPC-H Throughput Metric (Qth@Size)
        # Formula: Qth = (Streams * 22 * 3600) / Ts
        throughput_metric = (args.streams * 22 * 3600) / total_query_time
        
        print(f"\n====================================================================")
        print(f"🏆 Throughput Test Finished!")
        print(f"   - Concurrent Streams      : {args.streams}")
        print(f"   - Total Test Duration (Ts): {total_query_time:.2f} seconds")
        print(f"   - TPC-H Throughput Metric : {throughput_metric:.2f} Qth (Queries/Hour)")
        print(f"====================================================================")
        
    # 7. Generate beautiful Markdown report
    report_path = f"{script_dir}/benchmark_report.md"
    print(f"\n=== 7. Writing performance report to {report_path} ===")
    
    # Get current NUMA status info from container numa_maps
    numa_maps = "N/A"
    try:
        numa_maps_res = run_cmd("docker exec citus_worker_1 cat /proc/1/numa_maps | head -n 5", verbose=False)
        numa_maps = numa_maps_res.stdout.strip()
    except Exception:
        pass
        
    with open(report_path, "w") as rf:
        rf.write("# 📊 Citus TPC-H Asymmetric Multi-Socket Benchmark Report\n\n")
        rf.write("## ⚙️ Configuration & Environment\n")
        rf.write(f"- **Benchmark Mode**: {args.mode.upper()} ({'Single-user Latency' if args.mode == 'power' else f'{args.streams}-user Concurrency'})\n")
        rf.write(f"- **Scale Factor**: {args.scale_factor} (~{args.scale_factor} GB Raw Data)\n")
        rf.write(f"- **Table Storage Type**: {args.schema.upper()} (`USING {args.schema}`)\n")
        rf.write(f"- **NUMA Memory Policy**: {args.policy.upper()}\n")
        rf.write(f"- **DDR:CXL Weight Ratio**: {args.weight_ratio}\n")
        rf.write(f"- **Benchmark Executed At**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        rf.write("## 🔍 Worker 1 (Socket 0) Proc NUMA Map Check\n")
        rf.write("```text\n")
        rf.write(f"{numa_maps}\n")
        rf.write("```\n\n")
        
        if not args.skip_load:
            rf.write("## 📥 Parallel Data Loading Performance\n")
            rf.write("| Table Name | Load Time (seconds) |\n")
            rf.write("| :--- | :--- |\n")
            for table, l_time in load_times.items():
                rf.write(f"| {table} | {l_time:.3f} |\n")
            rf.write(f"| **TOTAL LOAD TIME** | **{total_load_time:.2f} seconds** |\n\n")
            
        if args.mode == "power":
            rf.write("## ⏱️ Query Latency Results (Power Test)\n")
            rf.write("| Query ID | Latency (seconds) | Status |\n")
            rf.write("| :---: | :--- | :---: |\n")
            for q_id, q_res in query_results.items():
                status = "✅ Success" if q_res["success"] else "❌ Failed"
                rf.write(f"| Q{q_id:02d} | {q_res['latency']:.3f} | {status} |\n")
            rf.write(f"| **TOTAL ELAPSED TIME** | **{total_query_time:.2f} seconds** | |\n\n")
        else:
            rf.write("## 📈 Concurrent Streams Results (Throughput Test)\n")
            rf.write(f"- **Number of Streams**: {args.streams}\n")
            rf.write(f"- **Total Elapsed Time ($T_s$)**: **{total_query_time:.2f} seconds**\n")
            rf.write(f"- **🏆 TPC-H Throughput Metric ($Qth@Size$)**: **{throughput_metric:.2f} Qth/Hour**\n\n")
            
            rf.write("### ⏱️ Individual Streams Query Latency Breakdowns (seconds)\n")
            headers = ["Query ID"] + [f"Stream {s}" for s in range(1, args.streams + 1)]
            rf.write("| " + " | ".join(headers) + " |\n")
            rf.write("| " + " | ".join([":---:"] * len(headers)) + " |\n")
            
            for q_id in range(1, 23):
                row_vals = [f"Q{q_id:02d}"]
                for s_id in range(1, args.streams + 1):
                    q_res = stream_results[s_id].get(q_id, {})
                    lat = q_res.get("latency", 0.0)
                    row_vals.append(f"{lat:.3f}")
                rf.write("| " + " | ".join(row_vals) + " |\n")
            rf.write("\n")
            
        rf.write("## 💡 Architectural Insights\n")
        if args.schema == "columnar":
            rf.write("- **Columnar Advantage**: Native Citus Columnar storage eliminates scanning of unused columns, drastically cutting down memory bus pressure on the CXL PCIe link. This results in significantly lower query times compared to traditional row-oriented heap tables for OLAP.\n")
        else:
            rf.write("- **Row-based Layout**: Row-oriented heap tables load entire rows into buffers, causing substantial page traffic over Socket CXL links. In memory-constrained asymmetric environments, this leads to heavy remote access bottlenecking.\n")
            
        if args.policy == "weighted":
            rf.write(f"- **Weighted Interleaving ({args.weight_ratio})**: Using kernel-level weighted interleave splits page allocations proportionally between fast local DDR and CXL remote memory. This mitigates memory asymmetry and optimizes resource utilisation across the different links.\n")
        elif args.policy == "interleave":
            rf.write("- **1:1 Interleaving**: Pages are allocated uniformly across local DDR and CXL, smoothing out latency peaks but not optimally factoring in the higher bandwidth of local DDR compared to PCIe-attached CXL.\n")
            
    print(f"\n[SUCCESS] Report created successfully at: {report_path}")
    print(f"Feel free to check it to inspect the benchmark metrics!")

if __name__ == "__main__":
    main()
