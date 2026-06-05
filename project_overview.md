# Project Overview — Tp-integrador-SDyPP-CompuMundo

> **Course:** Sistemas Distribuidos y Programación Paralela (SDyPP) — UNLu  
> **Deadline:** 23/06/2026  
> **Subject:** Distributed Blockchain + CUDA Mining  

---

## What This Project Is

An end-to-end prototype of a **distributed, Proof-of-Work blockchain** built from scratch. The system chains financial transactions (sender → receiver, amount) into blocks whose mining is offloaded to a GPU cluster via CUDA. The project is organized into three sequential pillars:

| Pilar | Topic | Status |
|---|---|---|
| **Pilar 1** | CUDA GPU miner (MD5 PoW) | ✅ Complete |
| **Pilar 2** | Distributed Python microservices + Docker | ✅ Complete |
| **Pilar 3** | Kubernetes (GKE) + CI/CD + Cloud deployment | 🔲 Pending |

---

## Repository Layout

```
Tp-integrador-SDyPP-CompuMundo/
├── ASSIGNMENT.md               # Full course assignment specification (Spanish)
├── README.md                   # Minimal stub (just the repo title)
├── project_overview.md         # This file
│
├── pilar1/                     # CUDA GPU miner programs
│   ├── README.md               # Pilar 1 report (hits 2–7 + CPU vs GPU benchmarks)
│   ├── hello_cuda/
│   │   ├── hello.cu            # Hello World kernel (Hit 2)
│   │   └── Makefile
│   ├── thrust/
│   │   ├── thrust_vectors.cu   # Sort 32M ints with Thrust (Hit 3)
│   │   └── Makefile
│   ├── md5_one_input/
│   │   ├── md5_cuda.cu         # Hash a single string on GPU (Hit 4)
│   │   ├── md5.cuh             # Device-side MD5 implementation
│   │   └── Makefile
│   ├── md5_bruteforce/
│   │   ├── md5_bruteforce.cu   # Brute-force nonce search, full space (Hit 5/6)
│   │   ├── md5.cuh
│   │   └── Makefile
│   ├── md5_bf_range/
│   │   ├── md5_range.cu        # Brute-force with [min, max] range (Hit 7)
│   │   ├── md5.cuh
│   │   └── Makefile
│   └── md5_cpu/
│       └── md5_cpu.py          # Python CPU reference implementation
│
└── pilar2/                     # Distributed blockchain infrastructure
    ├── README.md               # Pilar 2 report (design decisions per step)
    ├── docker-compose.yml      # Full stack: Redis, RabbitMQ, NCT, Pool, 2 Workers
    │
    ├── shared/                 # Shared domain models (imported by all services)
    │   ├── block.py            # Transaction + Block dataclasses
    │   ├── miner.py            # MinerService (subprocess wrapper for CUDA binary)
    │   ├── schemas.py          # Pydantic models for HTTP API
    │   └── __init__.py
    │
    ├── broker/                 # RabbitMQ topology + message types
    │   ├── broker.py           # declare_topology(), publish_*, consume_*, broadcast_abort()
    │   ├── messages.py         # TaskMessage, ResultMessage, ControlMessage dataclasses
    │   └── __init__.py
    │
    ├── storage/                # Redis persistence layer
    │   ├── chain_store.py      # save_block(), get_block(), validate_chain()
    │   └── __init__.py
    │
    ├── nct/                    # Node Coordinator (orchestrator)
    │   ├── nct.py              # Main service: 3 threads (block_loop, result_loop, health_loop)
    │   ├── state.py            # NCTState + NCTConfig dataclasses
    │   ├── Dockerfile
    │   └── __init__.py
    │
    ├── pool/                   # Pool Coordinator (partitions work for its workers)
    │   ├── pool.py             # PoolCoordinator: receives task, splits nonce space, collects results
    │   ├── Dockerfile
    │   └── __init__.py
    │
    ├── worker/                 # Mining Worker
    │   ├── worker.py           # Consumes tasks, calls MinerService, publishes results + heartbeats
    │   ├── Dockerfile
    │   └── __init__.py
    │
    ├── miner/                  # Standalone miner module (mirrors shared/miner.py)
    │   ├── miner.py
    │   └── __init__.py
    │
    └── tests/                  # Unit tests (61 tests, all without real infra)
        ├── test_block.py
        ├── test_broker.py
        ├── test_chain_store.py
        ├── test_health.py
        ├── test_miner.py
        ├── test_nct.py
        ├── test_worker.py
        └── __init__.py
```

---

## Pilar 1 — CUDA Miner Deep Dive

### What it does

Implements a GPU-accelerated MD5 hash brute-forcer to solve Proof-of-Work puzzles. Given a `base_string` and a `target_prefix`, it finds a `nonce` such that:

```
MD5(base_string + str(nonce)).startswith(target_prefix)
```

### Key files

| File | Role |
|---|---|
| `md5.cuh` | Device-side MD5. All functions marked `__device__`. Implements RFC 1321 padding + four-round transform. |
| `md5_cuda.cu` | Single-thread kernel: hash one input, verify correctness. |
| `md5_bruteforce.cu` | 1280 blocks × 256 threads = 327,680 concurrent threads. Grid-stride loop. Atomic flag for first-winner termination. |
| `md5_range.cu` | Extends bruteforce with `[range_min, range_max]` bounds. Used by Pilar 2 workers. |
| `md5_cpu.py` | Python `hashlib.md5` sequential reference. Used for CPU vs GPU comparison. |

### Parallelization strategy

```
GPU Thread Grid (327,680 threads)
├── Thread 0  → nonces: 0, 327680, 655360, ...
├── Thread 1  → nonces: 1, 327681, 655361, ...
│   ...
└── Thread N  → nonces: N, N+327680, N+655360, ...
```

First thread to match calls `atomicExch(found_flag, 1)` and writes its result. All other threads check the flag at the start of each iteration and exit early.

### Benchmark results (Google Colab T4 GPU)

| Prefix zeros | CPU time | GPU time | Speedup |
|---|---|---|---|
| 4 | 0.049s | 0.404s | — (CUDA init overhead dominates) |
| 6 | 22.8s | 0.497s | ~45x |
| 7 | 624s | 1.709s | ~365x |

GPU throughput: ~1.1 billion hashes/sec. CPU: ~800K hashes/sec.

### Development environment

- **Platform:** Google Colab (Tesla T4, sm_75, CUDA 12.8, driver 580)
- **Local GPU:** NVIDIA GTX 1060 (sm_61) — incompatible with modern CUDA toolkit
- **Compiler flag:** `nvcc -arch=sm_75`
- **AI assistant used:** DeepSeek

---

## Pilar 2 — Distributed Infrastructure Deep Dive

### Architecture overview

```
                    ┌──────────────────────────────────────────┐
                    │              RabbitMQ (topic exchange)    │
                    │         exchange: "blockchain"            │
                    │                                           │
  POST /transaction │  task.mining ──▶ pool-a.inbox            │
  ───────────────▶  │  result.*    ◀── pool-a.result.*         │
       NCT          │  worker.*    ◀── worker heartbeats        │
       (:8080)      │  control     ──▶ all workers (abort)      │
                    └──────────────────────────────────────────┘
                              │                  ▲
                   publishes  │ task.mining       │ result.pool-a
                              ▼                  │
                         ┌─────────┐             │
                         │ Pool-A  │─────────────┘
                         │ (:8090) │
                         └────┬────┘
                    partition │ nonce space into 2 sub-ranges
                    ┌─────────┴──────────┐
                    ▼                    ▼
             ┌──────────┐        ┌──────────┐
             │ worker-a1│        │ worker-a2│
             │  (:8081) │        │  (:8082) │
             └────┬─────┘        └────┬─────┘
                  │ subprocess         │ subprocess
                  ▼                    ▼
             md5_range (CUDA)    md5_range (CUDA)

                    Redis (:6379)
                    blockchain:blocks → [block0, block1, ...]
```

### Message types (`broker/messages.py`)

```python
TaskMessage    # NCT → workers: fingerprint, difficulty, range_min, range_max
ResultMessage  # worker → NCT: nonce, hash (MD5), worker_id
ControlMessage # NCT → all workers broadcast: action="abort", task_id
```

### NCT — Node Coordinator (`nct/nct.py`)

The brain of the system. Runs 3 threads:

| Thread | Responsibility |
|---|---|
| `block_loop` | Waits for N transactions → creates block → publishes mining task → waits for `block_mined` event → expands nonce space on timeout |
| `result_loop` | Polls `mining_results` queue → verifies PoW (MD5 + prefix check) → persists to Redis → broadcasts abort → signals `block_mined` |
| `health_loop` | Serves FastAPI on `:8080`: `GET /health`, `GET /status`, `POST /transaction` |

**PoW verification (double-check):**
```python
pow_hash = MD5(fingerprint + str(nonce))
valid = (pow_hash == claimed_hash) and pow_hash.startswith("0" * difficulty)
```

**Stale result filter:** if `result.block_index != current_block.index`, the result is silently dropped (another worker already won).

**Timeout expansion:** if no result in `BLOCK_TIMEOUT` seconds, the nonce space doubles and a new task is published.

### Block data model (`shared/block.py`)

```
Block
├── index           (int)      position in chain
├── timestamp       (float)    unix UTC
├── transactions    (list)     list of Transaction objects
├── previous_hash   (str)      SHA-256 of previous block (64 hex chars)
├── difficulty      (int)      number of leading zero nibbles for PoW
├── nonce           (int)      solution found by miner
└── hash            (str)      SHA-256 of complete block (post-mining)

Block.fingerprint   → SHA-256(block WITHOUT nonce)  ← sent to miners
Block.compute_hash()→ SHA-256(block WITH nonce)     ← used for chain linking
```

**Two distinct hash algorithms in use:**

| Hash | Algorithm | Purpose |
|---|---|---|
| `fingerprint` | SHA-256 | Stable identifier sent to miners as PoW base string |
| PoW hash | MD5 | Must start with N zeros (cheaper, good enough for demo) |
| `block.hash` | SHA-256 | Final block ID stored in Redis, used as `previous_hash` |

### RabbitMQ topology (`broker/broker.py`)

```
Exchange: "blockchain" (topic, durable)

Queues:
  mining_tasks     ← bind: task.*       (work queue, prefetch=1)
  mining_results   ← bind: result.*     (results from workers/pools)
  worker_registry  ← bind: worker.*     (heartbeats for live worker tracking)
  {anon per worker}← bind: control      (abort broadcast, exclusive, auto-delete)
```

**Pool architecture (step 2.8):**  
NCT publishes ONE message to `task.mining`. Every pool that has bound a queue to that key gets a copy. Each pool then partitions the full nonce space among its own workers. Pools compete with each other; the first valid result wins.

### Redis persistence (`storage/chain_store.py`)

```
Key: blockchain:blocks
Type: Redis List
Values: JSON-serialized Block objects (sort_keys=True for determinism)

Operations:
  RPUSH  → save_block()        append to chain
  LINDEX → get_block(index)    random access by position
  LLEN   → get_chain_height()
  LLEN+LINDEX → get_latest_block()
  full scan → validate_chain() verifies hash chaining integrity
```

AOF persistence enabled (`--appendonly yes`) so chain survives container restarts.

### Worker (`worker/worker.py`)

- Consumes `TaskMessage` from its pool's task queue (or `mining_tasks` if solo)
- Converts `difficulty: int` → `target_prefix: str` (`"0" * difficulty`)
- Calls `MinerService.mine(fingerprint, target_prefix, range_min, range_max)`
- If aborted mid-flight: discards result, acks message
- If solution found: publishes `ResultMessage` to `result.{worker_id}`
- Sends heartbeats every `HEARTBEAT_INTERVAL` seconds to `worker_registry`

### MinerService (`shared/miner.py`)

Thin subprocess wrapper around the CUDA binary:
```python
result = MinerService(binary_path="./md5_range").mine(
    base_string=fingerprint,
    target_prefix="0000",
    range_min=0,
    range_max=1_000_000_000
)
# → MinerResult(nonce=10941, hash="0000b8d7...") | None
```

Parses stdout, handles timeouts and crashes. The binary is compiled from `pilar1/md5_bf_range/`.

### Docker Compose services

| Service | Image | Port | Depends on |
|---|---|---|---|
| `redis` | redis:7-alpine | 6379 | — |
| `rabbitmq` | rabbitmq:3-management-alpine | 5672, 15672 | — |
| `nct` | custom (python:3.12-alpine) | 8080 | redis (healthy), rabbitmq (healthy) |
| `pool-a` | custom | 8090 | rabbitmq (healthy) |
| `worker-a1` | custom | 8081 | rabbitmq (healthy) |
| `worker-a2` | custom | 8082 | rabbitmq (healthy) |

### Environment variables (key ones)

| Service | Variable | Default | Meaning |
|---|---|---|---|
| NCT | `BLOCK_SIZE` | 5 | Transactions per block |
| NCT | `BLOCK_TIMEOUT` | 30 | Seconds to wait before expanding nonce space |
| NCT | `DIFFICULTY` | 4 | Leading zeros required |
| NCT | `NONCE_SPACE` | 1,000,000,000 | Initial nonce search range |
| Worker | `MINER_BINARY` | `./md5_range` | Path to compiled CUDA binary |
| Worker | `POOL_ID` | — | If set, worker joins a pool instead of solo mode |
| All | `LOG_FILE` | — | If set, logs go to file + stdout |

### Test coverage (`tests/`)

61 unit tests, all run without real Redis or RabbitMQ (mocked via `MagicMock` / `FakeClient`):

| Test file | What it covers |
|---|---|
| `test_block.py` | Transaction/Block creation, serialization roundtrip, PoW verification |
| `test_broker.py` | Topology declaration, task partitioning, result polling, abort broadcast |
| `test_chain_store.py` | Redis list operations, chain validation, broken-chain detection |
| `test_health.py` | HTTP endpoints: `/health`, `/status`, 404 handling |
| `test_miner.py` | Subprocess stdout parsing, timeout, crash, argument passing |
| `test_nct.py` | `verify_pow_result`, `accumulate_transactions`, `handle_result`, `NCTState` |
| `test_worker.py` | Heartbeat registration, active worker counting, expiration |

Run all tests:
```bash
cd pilar2 && python -m unittest discover tests/ -v
```

---

## Pilar 3 — Pending (Cloud Deployment)

According to the assignment, Pilar 3 requires:

- **GKE cluster** via OpenTofu (IaC), with separate node groups for infra vs apps
- **4 CI/CD pipelines** (GitHub Actions): cluster setup, infra services, app deploy, VM workers
- **Kubernetes HPA** or Cloud Run for auto-scaling CPU miners when GPU workers are unavailable
- **gitleaks** in CI to block hardcoded secrets
- **Public endpoints** for each service
- **Load testing** with 1–100K transactions and prefix lengths 1–8 chars

No code has been written for Pilar 3 yet.

---

## How to Run Locally (Pilar 2)

```bash
# Prerequisites: Docker + Docker Compose installed, CUDA binary built

# Build the CUDA miner first (requires NVIDIA GPU + CUDA toolkit, or Colab)
cd pilar1/md5_bf_range && make
cp md5_range ../../pilar2/

# Start all services
cd pilar2
docker compose up --build -d

# Submit a transaction
curl -X POST http://localhost:8080/transaction \
  -H "Content-Type: application/json" \
  -d '{"sender": "alice", "receiver": "bob", "amount": 10.0}'

# Check chain status
curl http://localhost:8080/status

# RabbitMQ Management UI
open http://localhost:15672  # guest / guest
```

---

## Key Design Decisions

1. **MD5 for PoW, SHA-256 for chain linking** — MD5 is fast on GPU (good for demo), SHA-256 is collision-resistant (good for tamper evidence).

2. **No digital signatures** — Transactions lack ECDSA signatures. The NCT acts as a trusted validator. Acknowledged shortcut vs real blockchain.

3. **`subprocess` for CUDA** — Python calls the compiled binary via `subprocess.run()` instead of PyCUDA. Keeps Pilar 1 (C++/CUDA) and Pilar 2 (Python) cleanly separated.

4. **Threading over asyncio** — NCT uses `threading` because the bottleneck is network I/O (RabbitMQ, Redis), not CPU. Three daemon threads share state via `NCTState` with a `threading.Lock`.

5. **Pool architecture** — NCT publishes one task (full range) per block. Pools subscribe and partition internally. This lets multiple pools compete without the NCT managing individual workers.

6. **Lazy imports in broker** — `pika` is only imported when a connection is actually needed, so the test suite runs without RabbitMQ installed.

7. **Deterministic serialization** — All JSON is serialized with `sort_keys=True` to ensure consistent SHA-256 hashes across Python versions and platforms.

---

## Tech Stack Summary

| Layer | Technology |
|---|---|
| GPU Mining | CUDA C++ (nvcc), MD5 custom implementation |
| CPU Mining | Python 3.11+ hashlib |
| GPU Parallelism | NVIDIA Thrust (CCCL), raw CUDA kernels |
| Services | Python 3.12, FastAPI, uvicorn |
| Message Queue | RabbitMQ 3 (pika client), topic exchange |
| Storage | Redis 7 (redis-py client), AOF persistence |
| Containerization | Docker + Docker Compose |
| Testing | Python `unittest` + `MagicMock` |
| Target cloud | Google Kubernetes Engine (GKE) via OpenTofu |
| CI/CD | GitHub Actions (planned) |
