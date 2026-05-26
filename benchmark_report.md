# 📊 Citus TPC-H Asymmetric Multi-Socket Benchmark Report

## ⚙️ Configuration & Environment
- **Benchmark Mode**: THROUGHPUT (4-user Concurrency)
- **Scale Factor**: 10.0 (~10.0 GB Raw Data)
- **Table Storage Type**: ROW (`USING row`)
- **NUMA Memory Policy**: DDR-ONLY
- **DDR:CXL Weight Ratio**: 3:1
- **Benchmark Executed At**: 2026-05-20 18:38:15

## 🔍 Worker 1 (Socket 0) Proc NUMA Map Check
```text
55a83c491000 bind:0 file=/usr/lib/postgresql/16/bin/postgres mapped=210 mapmax=12 N0=210 kernelpagesize_kB=4
55a83c563000 bind:0 file=/usr/lib/postgresql/16/bin/postgres mapped=595 mapmax=24 N0=592 N1=3 kernelpagesize_kB=4
55a83cac9000 bind:0 file=/usr/lib/postgresql/16/bin/postgres mapped=245 mapmax=24 N0=245 kernelpagesize_kB=4
55a83cd6b000 bind:0 file=/usr/lib/postgresql/16/bin/postgres anon=34 dirty=34 mapmax=9 active=0 N0=34 kernelpagesize_kB=4
55a83cd8d000 bind:0 file=/usr/lib/postgresql/16/bin/postgres anon=21 dirty=21 mapped=23 mapmax=24 active=2 N0=23 kernelpagesize_kB=4
```

## 📈 Concurrent Streams Results (Throughput Test)
- **Number of Streams**: 4
- **Total Elapsed Time ($T_s$)**: **29.32 seconds**
- **🏆 TPC-H Throughput Metric ($Qth@Size$)**: **10805.54 Qth/Hour**

### ⏱️ Individual Streams Query Latency Breakdowns (seconds)
| Query ID | Stream 1 | Stream 2 | Stream 3 | Stream 4 |
| :---: | :---: | :---: | :---: | :---: |
| Q01 | 3.271 | 3.233 | 4.198 | 13.008 |
| Q02 | 0.153 | 0.146 | 0.214 | 0.205 |
| Q03 | 0.150 | 0.125 | 0.138 | 0.085 |
| Q04 | 1.386 | 0.934 | 1.277 | 0.760 |
| Q05 | 0.166 | 0.212 | 0.234 | 0.153 |
| Q06 | 1.017 | 0.979 | 0.794 | 1.024 |
| Q07 | 0.138 | 0.112 | 0.086 | 0.117 |
| Q08 | 0.123 | 0.099 | 0.178 | 0.137 |
| Q09 | 0.115 | 0.222 | 0.180 | 0.127 |
| Q10 | 0.196 | 0.127 | 0.128 | 0.111 |
| Q11 | 0.084 | 0.188 | 0.172 | 0.113 |
| Q12 | 2.253 | 1.194 | 1.429 | 2.101 |
| Q13 | 0.153 | 0.124 | 0.199 | 0.158 |
| Q14 | 0.125 | 0.170 | 0.148 | 0.116 |
| Q15 | 6.093 | 6.215 | 4.150 | 4.933 |
| Q16 | 6.681 | 7.675 | 7.721 | 5.294 |
| Q17 | 0.200 | 0.166 | 0.163 | 0.124 |
| Q18 | 0.114 | 0.142 | 0.086 | 0.175 |
| Q19 | 0.178 | 0.210 | 0.151 | 0.166 |
| Q20 | 0.196 | 0.192 | 0.138 | 0.147 |
| Q21 | 0.189 | 0.124 | 0.080 | 0.104 |
| Q22 | 0.086 | 0.166 | 0.147 | 0.156 |

## 💡 Architectural Insights
- **Row-based Layout**: Row-oriented heap tables load entire rows into buffers, causing substantial page traffic over Socket CXL links. In memory-constrained asymmetric environments, this leads to heavy remote access bottlenecking.
