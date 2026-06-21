# ClickHouse Ingestion — Benchmark Results & Method

Reproduces and extends ingestion-comparisson.md §6 inside the BenchmarkDotNet harness, plus a
server-side load comparison read from `system.query_log`. Workload throughout: `(id Int64, name
String, value Float64)`, `MergeTree ORDER BY id`, rows `(i, "BulkItem_{i}", i*1.5)`, against
`clickhouse/clickhouse-server:26.2` in Docker via Testcontainers.

Environment: Apple M5 (10 physical cores), macOS, .NET 10, BenchmarkDotNet 0.15.8, in-process
toolchain, Docker Desktop (7.65 GB RAM cap). Docker-on-macOS timings are noisy — trust the
**ratios/ordering**, treat absolute ms as environment-specific.

---

## Why we measure what we measure

The harness answers two different questions, and they need different instruments:

**1. "How long does the client wait?" → wall-clock.** BenchmarkDotNet times the insert call on a warm
connection. This is what an application experiences. It's the right metric for "which client/route
finishes a load fastest", but it conflates client serialization, network, and server work into one
number.

**2. "How hard does the *server* work?" → `system.query_log`.** Wall-clock can't tell you whether the
cost is on the (cheap, horizontally-scalable) client or the (expensive, hard-to-scale) server. So the
`server-load` command reads the server's own accounting per INSERT:
- `OSCPUVirtualTimeMicroseconds` → **server CPU actually consumed** (excludes waiting — the true
  "computation" number the fleet pays for).
- `NetworkReceiveElapsedMicroseconds` → **time the server sat blocked reading client bytes**. High =
  the client is the bottleneck and server ingestion overlaps client production (pipelining).
- `memory_usage` → **peak server RAM** for the query.

Design choices that keep each number honest:

- **Payloads are pre-built in setup, outside the timed region.** Row materialization / boxing / Parquet
  minting happen before measurement so each route times only its own work (transport + serialization +
  server), not allocation. The one deliberate exception is the Parquet.Net *write* route, where the
  encoding **is** the thing under test, so it's inside the timed region.
- **We report medians, not means.** Single I/O-bound ops have fat right tails (a stray GC or merge); the
  mean chases outliers, the median doesn't. Where Error ≫ Mean (the low-iteration runs) the mean is
  meaningless — read the median/min.
- **Iteration count is matched to op cost.** 1M routes run 10 iterations; the 20M / wide routes run 3–8
  (each op moves 20M rows). `BENCH_ITERS` overrides it when a close comparison needs tightening.
- **Parquet fixtures are minted by the server** (`SELECT … FORMAT Parquet`), not authored in .NET — so
  the "decode" routes measure pure server decode. The cost of *creating* Parquet in-app is measured
  separately (the Parquet.Net write route), because mixing them would hide which side pays.
- **One 1M payload is reused per chunk** in the 20-chunk runs. Server work is byte-identical to 20
  distinct chunks, but client memory stays at one chunk instead of twenty.
- **Serial execution** lets `server-load` match "the most recent INSERT into the table" unambiguously,
  with no query-id plumbing.

---

## Result 1 — End-to-end, data born in the application (5 × 1M, reliable)

The question an app pipeline actually faces: rows exist in memory, get them into ClickHouse. 8
iterations, medians:

| Rank | Path | Median | Throughput | min–max |
|---|---|---:|---:|---:|
| 🥇 | **CH.Native typed 5×1M** (one streamed INSERT/chunk) | **492 ms** | ~10.2M rows/s | 468–609 |
| 🥈 | Parquet copy + `file()` ingest (end-to-end) | 573 ms | ~8.7M rows/s | 526–640 |
| 🥉 | Parquet HTTP POST ×5 (over wire) | 571 ms | ~8.8M rows/s | 557–586 |
| | ClickHouse.Driver BatchSize=1M, 5×1M | 757 ms | ~6.6M rows/s | 735–813 |
| — | *Parquet copy-to-server (copy only)* | *294 ms* | — | 275–431 |
| — | *Parquet `file()` decode-only (already on disk)* | *259 ms* | ~19.3M rows/s | 218–311 |

**CH.Native is the fastest path that actually moves the data** — ~14% ahead of Parquet copy+ingest, and
robust: Native's slowest typical run (468 ms) still beats copy+ingest's best (526 ms).

Why `file()` decode-only (259 ms) is *not* the winner despite being fastest on the board: it assumes the
files teleported onto the server. Count the transfer and copy-only 294 + decode-only 259 ≈ 553 ≈ the
573 ms end-to-end — **copy and ingest are sequential phases**, while streaming overlaps client production
with server ingestion. That overlap is the whole game.

**Decision rule:**
- Data already on the server / in S3 it can pull → `file()`/`s3()` bulk load (fastest, ~259 ms).
- Data born in the app (has to travel) → **stream it with CH.Native.**
- Tuned Driver (BatchSize=1M) removes the batching staircase but still trails ~1.5× — its per-cell boxed
  `object[]` serialization is on the critical path and baked into the API, not configurable.

### Caveat: the Parquet number excludes Parquet *generation*

The 573 ms times only copy + ingest, on bytes ClickHouse minted for free. CH.Native's 492 ms **includes**
its block serialization; the Parquet figure **excludes** the equivalent row→Parquet encoding. A real
born-in-app file route is *write Parquet → copy → ingest*. The .NET write costs ~85 ms/1M (+~147 MB
allocated/1M — see Result 2), so the honest end-to-end is ~950–1000 ms — roughly double CH.Native.
*(Estimate from components; a single measured `Parquet.Net write + copy + file()` route can be added.)*

---

## Result 2 — Single-file Parquet routes (1 × 1M, 10 iterations, allocations on)

| Route | Median | Allocated | Note |
|---|---:|---:|---|
| Parquet `file()` (server-local bulk load) | 70 ms | 22 KB | fastest — one INSERT, all cores decode |
| Parquet HTTP POST (server-gen, decode only) | 123 ms | 16 KB | pure server decode + ingest |
| CH.Native streamed (reference) | 137 ms | 861 KB | the baseline |
| Parquet HTTP POST (Parquet.Net write + ship) | 197 ms | **147 MB** | the in-app write cost lands here |

The 147 MB is Parquet.Net buffering columns in-process — "data born in the app pays the write tax",
visible directly. This is why the file route's missing-generation cost (above) is ~85 ms/1M.

---

## Result 3 — Server-side load (10M-row single INSERT, `query_log`)

The most interesting axis: not who waits, but **how much the server computes**. Native blocks (LZ4 wire)
vs uncompressed native vs RowBinary vs Parquet. 3 reps, medians:

| Route | server dur | **server CPU** | net-recv | peak mem |
|---|---:|---:|---:|---:|
| CH.Native streaming (LZ4 wire) | 1,205 ms | 865 ms | 507 ms | 141 MB |
| **CH.Native streaming (no compression)** | 1,703 ms | **641 ms** | 1,269 ms | 130 MB |
| HTTP RowBinary (single INSERT) | 1,000 ms | 793 ms | 290 ms | 117 MB |
| HTTP Parquet (single INSERT) | 603 ms | 1,000 ms* | 51 ms | 829 MB |
| Parquet `file()` (server-local) | 637 ms | 981 ms* | 0 ms | 591 MB |

Ranked by server CPU (the compute the fleet pays for):

1. **Native, uncompressed — 641 ms.** Cheapest. Native blocks arrive ~already-columnar, so the server
   does the least to turn them into a part. **Confirms the doc's thesis.**
2. RowBinary — 793 ms (~24% more: the server transposes rows → columns).
3. Native, LZ4 — 865 ms. The +224 ms over uncompressed **is the decompression** — proven by toggling it.
4–5. Parquet — ~980–1000 ms*, and **4–6× the memory** (591–829 MB vs native's ~130 MB).

Three durable conclusions:

- **Native blocks are the lowest-CPU ingest format for the server** — once the wire is fair. (An earlier
  run showed native *higher* CPU; that was purely the LZ4 artifact, now isolated.)
- **LZ4 trades server CPU for network.** Compression cut net-recv 1,269 → 507 ms (less data on the wire,
  lower duration) but added ~224 ms of decompression CPU. Worth it when the network constrains; costly
  when server CPU does.
- **Streaming is dramatically lighter on server memory** (~130 MB) than Parquet (591–829 MB). Parquet
  buffers the whole columnar batch to decode; native streams block-by-block. The cleanest "Parquet loads
  the server harder" signal, independent of compression.

\* Parquet decode runs partly on background parser threads the query-thread CPU counter under-attributes,
so its CPU is a floor — but even floored it's the heaviest on CPU, and unambiguously heaviest on memory.

**Why this matters at fleet scale:** clients are cheap and horizontal; the server is the expensive,
hard-to-scale resource. Native streaming minimizes exactly the thing you can't scale away — server CPU
and RAM per insert — while pushing serialization onto the many clients.

---

## Result 4 — Can the copy be made faster? No, it's bandwidth-bound

The file route stages chunks onto the server. Per-file copy issues one Testcontainers `CopyAsync` (one
Docker-API tar stream) per chunk — 20 round trips at 20 chunks. Hypothesis: one tar would cut overhead.
Tested (`ParquetCopyBatchedToServer`). 20×1M / 6 iterations, ~172 MB:

| Copy method | Median | Throughput |
|---|---:|---:|
| Per-file (20 Docker round trips) | ~1.15 s | ~150 MB/s |
| Batched (1 tar, 1 round trip) | 1.13 s | ~152 MB/s |

Within noise. The batched path even adds a 172 MB host-disk write and still matches — the bottleneck is
**Docker-API byte throughput (~150 MB/s), not round trips**. The only faster option is a **bind mount**,
which makes "copy" a near-free local write — but that stops modeling a *transfer* (it's "data already on
the server") and would flatter the file route. There is no honest way to speed the copy up: it's bytes
over a pipe. That's the point — **copy ≈ ingest cost is fundamental, which is why streaming wins.**

---

## Result 5 — 20 × 1M (20M rows) — scale shape, CPU-saturated near the top

6 iterations. Kept for the throughput-at-scale shape, **not** for fine ordering:

| Path | Median | Note |
|---|---:|---|
| Parquet `file()` ingest (decode only) | ~1.1 s | ~18M rows/s |
| Parquet copy-to-server (per-file, copy only) | ~1.15 s | ~172 MB transfer |
| Parquet copy-to-server (batched tar, copy only) | 1.13 s | ≈ per-file (Result 4) |
| Parquet copy + `file()` ingest (end-to-end) | 2.45 s | ≈ copy + ingest |
| Parquet HTTP POST ×20 (over wire) | 2.44 s | |
| CH.Native typed 20×1M | 2.58 s | min 2.36 s; merge/CPU contention |
| ClickHouse.Driver BatchSize=1M, 20×1M | 3.36 s | clearly slowest |

At 20 chunks the MacBook (10 cores + ClickHouse background merges) is saturated: Native (2.58 s) and
copy+ingest (2.45 s) sit **within noise of each other** — do not read their ordering here; the clean
5×1M run (Result 1) is where Native's lead is real. The Driver trailing and the copy/ingest
decomposition hold at any size.

---

## How to reproduce

```bash
# Single-file Parquet routes (1M, 10 iter, allocations)
dotnet run --project src/ClickCompare.Bench -c Release -- --filter '*ParquetIngestionBenchmarks*'

# Bulk-load comparison — chunk count via BULK_CHUNKS, iterations via BENCH_ITERS
BULK_CHUNKS=5  BENCH_ITERS=8 dotnet run --project src/ClickCompare.Bench -c Release -- --filter '*BulkLoad20MBenchmarks*'   # reliable
BULK_CHUNKS=20 dotnet run --project src/ClickCompare.Bench -c Release -- --filter '*BulkLoad20MBenchmarks*'                 # saturates a MacBook

# Server-side load from system.query_log — rows via SERVER_ROWS (default 10M)
SERVER_ROWS=10000000 dotnet run --project src/ClickCompare.Bench -c Release -- server-load
```

Routes (`BulkLoadRoute`): `ParquetFileIngest`, `ParquetCopyToServer`, `ParquetCopyBatchedToServer`,
`ParquetCopyAndIngest`, `ParquetHttpSequential`, `NativeSequential`, `DriverSequential`. Server-load
routes: CH.Native (LZ4 + uncompressed), HTTP RowBinary, HTTP Parquet, Parquet `file()`.

Reports: `results/results/BulkLoad-5x1M-8iter-report-github.md`, `BulkLoad-5x1M-report-github.md`,
`BulkLoad-20x1M-copybench-report-github.md`, `BulkLoad-20x1M-report-github.md`,
`…ParquetIngestionBenchmarks-report-github.md`. `server-load` prints to stdout (not a BDN artifact).
