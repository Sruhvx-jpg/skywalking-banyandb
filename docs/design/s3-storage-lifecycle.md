<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
-->

# RFC: Pluggable S3 Storage Backend & Remote LSM Compaction for BanyanDB

- **Author**: Sruhvx-jpg
- **Status**: Proposed / Review
- **Target Component**: `banyand/backup/lifecycle`, `banyand/internal/storage`, `pkg/fs/remote/aws`
- **Issue**: [apache/skywalking#13989](https://github.com/apache/skywalking/issues/13989)

---

## 1. Executive Summary & Design Invariants

BanyanDB utilizes a stage-stepped lifecycle engine (`hot` → `warm` → `cold`) to transition time-series telemetry (metrics, streams, traces) across retention boundaries. This RFC introduces **S3 (and S3-compatible object storage such as MinIO and Ceph) as a pluggable storage backend** for BanyanDB data nodes and the lifecycle engine.

### Core Architectural Invariants
1. **S3 is a Storage Backend, Not a Lifecycle Stage**: Any stage can bind to local block storage (NVMe/SSD) or remote object storage (S3).
2. **Lifecycle-Driven Ingress Only (Phase 1 Scope)**: Data in S3 is generated strictly by the **Lifecycle Agent** offloading aged-out local segments. Direct flushes from ingestion MemTables directly to S3 are deferred to eliminate PUT cost explosion and dual-write split-brain.
3. **Decoupled Remote S3 LSM Compactor**: Accumulating fragmented small parts in S3 over time degrades query latency via HTTP Range-GET amplification. A decoupled background compactor merges small remote parts into optimized columnar parts.
4. **Zero S3 Listing in Hot Path**: Queriers and compactors never execute expensive, unindexed `ListObjectsV2` calls on S3. Active parts are resolved via **atomic snapshot manifests (`.snp`)**.
5. **Strict Metadata Authority (etcd = Authority, S3 = Substrate)**: Group schemas, catalogs, and retention policies remain strictly in `etcd`. S3 stores only physical segment data and snapshot manifests, eliminating dual-authority split-brain.

---

## 2. Upstream Comparative Storage Research & Citations

To ground BanyanDB's storage architecture in proven industry patterns, we audited the storage models of **Grafana Tempo, Thanos, Apache Doris, Grafana Loki, and OpenObserve** across 7 architectural dimensions:

### Comparative Storage Matrix

| Dimension | **Grafana Tempo** | **Thanos (Baseline)** | **Apache Doris / SelectDB** | **Grafana Loki** | **OpenObserve** | **BanyanDB Derived Design** |
|---|---|---|---|---|---|---|
| **1. Storage Layout** | `<tenant>/<ulid>/` with Parquet, Bloom, `meta.json` | `<ulid>/` directory with `chunks/`, `index`, `meta.json` | `data/<tablet_id>/<rowset_id>_<seg>.dat` | `<tenant>/` chunks + `<prefix>_<period>/` TSDB index | `<org>/<stream>/<year>/<month>/<day>/<hour>/` Parquet | **1:1 TSDB Mirror**: `.../<catalog>/<group>/seg-<suffix>/shard-<id>/parts/<part_id>/` |
| **2. Publication Model** | **Atomic Sentinel**: Payload uploaded first; `meta.json` written last | **Atomic Sentinel**: `meta.json` is the sole integrity marker uploaded last | **Centralized Meta-Service**: Distributed RPC transaction commits rowset meta | **`tsdb_shipper`**: Buffers local index on NVMe, ships finalized tables to S3 | **External Relational DB**: Stores file catalog (`file_list`) in PostgreSQL/SQLite | **Atomic `.snp` Sentinel**: Parts land first; snapshot manifest `<epoch>.snp` commits last |
| **3. Query Path** | Range-GET on `data.parquet` via footer offsets | **Store Gateway**: Stateless proxy; Range-GETs chunk objects | HTTP Range-GET directly into local `FileCache` | Querier memory/Memcached for chunks and index lookups | Querier reads Parquet headers and row groups | **Zero-Staging Range-GET**: Data nodes stream byte ranges directly via `s3.GetObjectInput{Range}` |
| **4. Caching Subsystem** | Multi-tier: Bloom, Index, Parquet Footer, Page | In-memory / NVMe cache for `index-header` | Block-level NVMe `FileCache` with LRU eviction | In-memory / Memcached for Chunks & Results | Memory cache for Tantivy index & Parquet footers | **Two-Tier Cache**: L1 Memory / L2 NVMe for Meta+`sidx`; Block-aligned LRU for column vectors |
| **5. Compaction Engine** | **Compactor Component**: Combines small blocks within compaction windows | **Compact Component**: Merges 2h blocks into 8h/14d blocks; Shipper supports `--upload-compacted` | **Cloud Compaction**: Coordinated via leases to merge remote rowsets; async `recycler` | **Compactor Service**: Merges 24h index tables and executes retention deletions | **Compactor Node**: Periodically merges small Parquet files into larger files | **Two-Phase**: Pre-upload local compaction (Shipper) + Remote S3 LSM Compactor (Worker) |
| **6. Retention / GC** | Compactor purges blocks past retention window | Compactor purges blocks via deletion marks | Async `Recycler` purges stale rowsets | Compactor deletes chunk prefixes when `retention_enabled: true` | Compactor purges expired Parquet entries | **Lifecycle Agent**: Scans segment boundaries against etcd TTL; invokes S3 `DeleteObjects` |
| **7. Failure Recovery** | Missing `meta.json` leaves block invisible | Missing `meta.json` treated as corrupt/partial | Uncommitted rowsets swept by `recycler` | Tombstones + local recovery directory | Re-scan / rebuild `file_list` catalog | **Sentinel Guard**: Reader only evaluates `.snp`; `progress.json` tracks resume; orphaned parts swept by GC |

### Upstream Verbatim Citations & Architectural Provenance

1. **Atomic Manifest Sentinel (Grafana Tempo)**:
   > *"The `meta.json` file makes a block visible to the read path. It contains the block ID, tenant ID, start and end timestamps, total number of objects (traces). A block is invisible to queriers until `meta.json` exists. The block-builder uses this property to ensure atomicity during flushes."*  
   *(Source: [Grafana Tempo Architecture: Block Format](https://grafana.com/docs/tempo/latest/reference-tempo-architecture/block-format/))*
   - **BanyanDB Application**: BanyanDB avoids an external database by using the `.snp` manifest as Tempo uses `meta.json`. Parts land in S3 under immutable IDs; uploading `<epoch>.snp` atomically activates the part set for readers.

2. **Integrity Marker & Pre-Compacted Shipping (Thanos)**:
   > *"TSDB meta.json file within block have to be uploaded as the last one in the block folder. Currently and as proposed meta.json is our integrity marker"*  
   *(Source: [Thanos Design: Read-Write Operations Bucket](https://thanos.io/tip/proposals-done/201901-read-write-operations-bucket.md/))*
   > `--[no-]shipper.upload-compacted`: *"If true shipper will try to upload compacted blocks as well. Useful for migration purposes. Works only if compaction is disabled on Prometheus."*  
   *(Source: [Thanos Shipper Component](https://thanos.io/tip/components/compact.md/))*
   - **BanyanDB Application**: Modeled on `--shipper.upload-compacted`, the BanyanDB Lifecycle Agent pre-compacts fragmented NVMe parts before shipping, eliminating 85–95% of S3 PUT calls.

3. **Local Block Caching (`FileCache`) (Apache Doris)**:
   > *"BE nodes cache a portion of tablet metadata and data locally to optimize performance."*  
   *(Source: [Apache Doris Compute-Storage Decoupled Overview](https://doris.apache.org/docs/3.x/compute-storage-decoupled/overview/))*  
   *(Source Code: `be/src/io/cache/cached_remote_file_reader.cpp`)*
   - **BanyanDB Application**: Adopted for data node query execution. Queriers never download whole segments; they stream block-aligned byte ranges from S3 directly into a local NVMe LRU block cache.

---

## 3. Architecture & System Topology

```
┌─────────────────────────────────────────────────────────────┐
│                    Metadata Registry (etcd)                 │
│  - Sole Authority: Schemas, Group TTLs, Stage Routing       │
└──────────────────────────────┬──────────────────────────────┘
                               │
       ┌───────────────────────┴───────────────────────┐
       ▼                                               ▼
┌──────────────────────────────┐        ┌──────────────────────────────┐
│       Lifecycle Agent        │        │          Data Node           │
│  - Scans expired segments    │        │  - S3 Block Range-GET Reader │
│  - Pre-upload shard merger   │        │  - Two-Tier Local FileCache  │
│  - Atomic .snp publisher     │        │  - Remote S3 LSM Compactor   │
│  - Post-verify NVMe pruner   │        │  - Async Garbage Collector   │
└──────────────┬───────────────┘        └──────────────┬───────────────┘
               │ PutObject                             │ Range-GET / Put / Delete
               ▼                                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│                           S3 Object Store                            │
│  s3://<bucket>/<prefix>/<catalog>/<group>/seg-<suffix>/shard-<id>/   │
└──────────────────────────────────────────────────────────────────────┘
```

### Schema Extensions (`api/proto/banyandb/common/v1/common.proto`)

```protobuf
message LifecycleStage {
  string name = 1;
  uint32 shard_num = 2;
  IntervalRule segment_interval = 3;
  IntervalRule ttl = 4;
  string node_selector = 5;
  bool close = 6;
  uint32 replicas = 7;

  // Storage backend: "local" (default) or "s3"
  string storage_backend = 8;
  // S3 path/bucket override (e.g., "s3://banyandb-cold/apm/")
  string s3_path = 9;
}

message ResourceOpts {
  uint32 shard_num = 1;
  IntervalRule segment_interval = 2;
  IntervalRule ttl = 3;
  repeated LifecycleStage stages = 4;
  repeated string default_stages = 5;
  uint32 replicas = 6;

  // Default fallback backend
  string storage_backend = 7;
}
```

---

## 4. S3 Object Hierarchy & Layout

To ensure zero cognitive translation overhead, S3 object keys mirror BanyanDB's native TSDB shard hierarchy 1:1. S3 stores **strictly segment payloads and manifests**:

```
s3://<bucket>/<base_prefix>/<catalog>/<group>/
  └── seg-<segment_suffix>/                            (e.g., seg-20260601)
       ├── metadata.json                               (Segment time boundaries: min/max timestamp)
       ├── sidx/                                       (Series Index Snapshots)
       │    ├── <epoch-hex16>.snp                      (Sidx snapshot sentinel)
       │    └── <part-id>/...                          (Posting lists & dictionaries)
       └── shard-<shard_id>/                           (LSM Data Shard)
            ├── manifests/
            │    ├── 0000000000000001.snp              (Active part ID array: ["0000000000000001"])
            │    └── 0000000000000002.snp              (Post-compaction manifest)
            └── parts/
                 ├── 0000000000000001/                 (Immutable Columnar Data Part)
                 │    ├── meta.bin                     (Part header: min/max ts, count, block offsets)
                 │    ├── timestamps.bin               (Timestamp vectors)
                 │    ├── tag_metadata.bin             (Tag descriptor blocks)
                 │    ├── tag_values.bin               (Columnar tag payloads)
                 │    ├── field_metadata.bin           (Field descriptors)
                 │    └── fv.bin                       (Field value blocks)
                 └── 0000000000000002/                 (Merged part)
```

---

## 5. Lifecycle Upload & Publication Protocol

```
[1. Scan & Filter] 
       │
       ▼
[2. Local Shard Pre-Compaction Pass]
       │
       ▼
[3. Size-Aware Multipart Upload (Bounded Buffer)]
       │
       ▼
[4. S3 Manifest Commit (.snp) & HeadObject Audit]
       │
       ▼
[5. Local NVMe Segment Prune]
```

### 5.1 Pre-Upload Shard Compaction
- Ingestion produces numerous small part flushes on NVMe. Directly uploading them incurs extreme AWS PUT costs ($0.005 per 1,000 requests) and forces Range-GET amplification during queries.
- The Lifecycle Agent invokes BanyanDB's native LSM merger locally prior to upload, consolidating flushed parts into a single part.
- Slashes S3 API PUT calls by 85–95% upon initial landing.

### 5.2 Size-Aware Upload Engine & Concurrency Bounds
- **`< 16 MB`** (`meta.bin`, descriptor files, `.snp`): Single `PutObject` with CRC32/MD5 checksum.
- **`≥ 16 MB`** (`fv.bin`, `timestamps.bin`): S3 Multipart Upload (MPU) in 16 MB chunks.
- **Concurrency Bounds**:
  - **Segments**: Sequential, oldest-first (`seg-YYYYMMDD` ascending) to free local NVMe storage immediately.
  - **Parts/Chunks**: Concurrent via bounded worker pool (8–16 workers, max memory buffer: $16 \times 16\text{ MB} = 256\text{ MB}$).

### 5.3 Atomic Manifest Commit & Safe Pruning
1. **Payload First**: Immutable binary files (`meta.bin`, `timestamps.bin`, `fv.bin`, `sidx`) upload under new part IDs.
2. **Atomic Commit**: The manifest `manifests/<epoch>.snp` commits **last**. Readers evaluate only the newest valid `.snp`.
3. **Safe Pruning Gate**: Source segment files on local NVMe are pruned only after:
   - `<epoch>.snp` is durably committed in S3.
   - S3 `HeadObject` audits verify remote part byte lengths match local lengths.

---

## 6. Data Node Query Path & Two-Tier Block Caching

Data nodes execute queries **without full-segment staging**, streaming only required columnar vectors:

```
                         Incoming Query
                                │
                                ▼
         ┌──────────────────────────────────────────────┐
         │          Series Index & Metadata             │
         │  (Check L1 Memory / L2 NVMe Index Cache)     │
         └──────────────────────┬───────────────────────┘
                                │
                  Index Hit / Manifest Loaded
                                │
                                ▼
         ┌──────────────────────────────────────────────┐
         │         Vector / Field Data Evaluation       │
         │     (Resolve Byte Ranges from meta.bin)      │
         └──────────────────────┬───────────────────────┘
                                │
            ┌───────────────────┴───────────────────┐
      [Cache Hit]                             [Cache Miss]
            │                                       │
            ▼                                       ▼
   Read Local NVMe Block              Issue Block-Aligned HTTP Range-GET
   (Zero Remote Network)              (Download 1MB–4MB Block to Cache)
            │                                       │
            └───────────────────┬───────────────────┘
                                │
                                ▼
                   Decode & Stream to Querier
```

- **Tier 1 (Index/Metadata)**: `sidx` series indexes and `meta.bin` block descriptors are lazily fetched and cached on local NVMe (`--s3-cache-dir`).
- **Tier 2 (Column Vectors)**: Readers parse `meta.bin` for exact byte offsets, then stream requested vector slices (`timestamps.bin`, `fv.bin`) using block-aligned HTTP `Range: bytes=start-end` into local LRU cache blocks.

---

## 7. Remote S3 LSM Compaction

Because periodic lifecycle migrations continuously push new parts to S3, a decoupled background **Remote LSM Compactor** prevents file fragmentation:

```
[1. Select Candidate Parts from .snp]
                  │
                  ▼
[2. Stream Columnar Blocks via Range-GET to Scratch (/tmp/banyandb-scratch)]
                  │
                  ▼
[3. K-Way Sorted Merge Iterator Consolidates into New Part ID]
                  │
                  ▼
[4. Upload New Merged Part to S3]
                  │
                  ▼
[5. Atomically Commit manifests/<epoch+1>.snp (Old Parts Swapped Out)]
                  │
                  ▼
[6. Asynchronous S3 DeleteObjects Purges Superseded Source Parts]
```

- **Zero Query Interruption**: Active queries continue reading old parts referenced by `<epoch>.snp` until `<epoch+1>.snp` is committed.
- **Fail-Safe Crash Recovery**: If the compactor crashes mid-merge, `<epoch>.snp` remains active; unreferenced merged parts are swept by background GC.

---

## 8. Failure Recovery Matrix

| Failure Mode | Impact | Recovery Action |
|---|---|---|
| **Lifecycle Agent crash mid-upload** | Orphaned chunks in S3; incomplete local state | Agent reads local `progress.json`, aborts abandoned MPU sessions, and resumes missing files. |
| **Crash before uploading `.snp`** | Unreferenced parts in S3 | Fail-safe: Data nodes never see uncommitted parts because active `.snp` was not swapped. Background GC sweeps orphaned prefixes. |
| **Compactor crash mid-merge** | Staged merged part in S3; local scratch files | Zero corruption: Active `.snp` still references old parts. New compactor clears scratch disk and deletes unreferenced part. |
| **S3 503 SlowDown / Throttling** | Request latency spike | S3 client applies exponential backoff with full jitter (AWS SDK v2 standard) with pooled HTTP/2 transports. |
| **Local Cache Corruption** | Cache read checksum failure | Evict corrupt block from cache index; transparently fall back to S3 Range-GET and re-populate block cache. |

---

## 9. Proof of Concept (PoC) & Benchmark Plan

The PoC will benchmark the four core architectural assumptions against MinIO and AWS S3:

1. **Upload Engine & Pre-Compaction**:
   - Compare un-compacted vs. pre-compacted uploads across 10GB, 50GB, and 100GB segment datasets.
   - Measure network throughput (MB/s), total S3 PUT operations, and memory consumption under bounded worker pools.
2. **Query Latency (Cold vs. Warm Cache)**:
   - Benchmark cold-cache read latency (fetching `.snp`, `meta.bin`, and streaming via 1MB-aligned Range-GET).
   - Benchmark warm-cache read latency (index and data blocks cached on local NVMe).
   - Compare against local NVMe baseline.
3. **Remote S3 Compaction**:
   - Merge 10 small S3 parts ($\approx 10\text{MB}$ each) into a single optimized part.
   - Measure scratch disk I/O, peak heap memory, and verification of zero query disruption during atomic manifest swap.
4. **Crash Recovery & Orphan Audit**:
   - Send `SIGKILL` to Lifecycle Agent mid-upload and Compactor mid-merge.
   - Verify zero reader degradation and automated cleanup of untracked parts.
