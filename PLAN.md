# PLAN — rust-vs-jvm-runtime-tax

Reproducible artifact for *"Quantifying the Runtime Tax: Memory Inflation, CPU
Efficiency, and Cloud Infrastructure Costs of Native Rust vs. JVM Scala in Big
Data Platforms"* (Sowdari & Sowdari).

This document fixes the design **before** any measurement is taken. No
benchmark numbers appear anywhere in this repository except in `results/`
files produced by the harness.

---

## 1. Pinned toolchain and dependencies

| Component | Version | Notes |
|---|---|---|
| rustc / cargo | **1.81.0** | as stated in the paper (§II-B) |
| `parquet` crate | **=53.4.1** (`arrow` feature) | builds on rustc 1.81; 53.3.0 could not be pinned consistently because `arrow 53.3.0` pulls `arrow-* ^53.3.0`, which resolved to 53.4.1 |
| `arrow` crate | **=53.4.1** (default features off) | all 13 arrow/parquet crates locked to 53.4.1 |
| `rand` / `rand_chacha` / `rand_distr` | =0.8.5 / =0.3.1 / =0.4.3 | datagen only; stable value streams |
| `mimalloc` crate | =0.1.43 | optional feature of pipeline-rust |
| Scala | **3.5.0** | as stated in the paper |
| JDK | **Temurin 21.0.4+7** | as stated in the paper |
| sbt / sbt-assembly | 1.10.2 / 2.2.0 | |
| parquet-java (`parquet-hadoop`) | **1.14.4** | `LocalInputFile` / `LocalOutputFile` |
| Hadoop | `hadoop-client-api` + `hadoop-client-runtime` **3.3.6** | shaded clients; needed only for `Configuration` / codec classes |
| slf4j-nop | 2.0.16 | silences parquet-java logging |
| Python (verify/bench/analysis) | 3.12, pyarrow =17.0.0 | |

Transitive Rust dependencies are pinned in `Cargo.lock` (committed). The lock
file is resolved with `incompatible-rust-versions = "fallback"` so every crate
builds on rustc 1.81.0. Docker base images are pinned by digest in
`docker/*.Dockerfile`.

## 2. Dataset (datagen/)

Schema (all columns `REQUIRED`):

| column | Arrow type | Parquet physical type | generation |
|---|---|---|---|
| `id` | FixedSizeBinary(16) | FIXED_LEN_BYTE_ARRAY(16) | UUIDv4 (random bytes, version/variant bits set) |
| `ts` | Int64 | INT64 | epoch-ms, uniform over 30 days from 2024-01-01T00:00:00Z |
| `payload` | Utf8 | BYTE_ARRAY (UTF8) | exactly 1024 chars from `[A-Za-z0-9_-]` |
| `metric` | Float64 | DOUBLE | Normal(100, 25), clamped to ≥ 0 |

Parquet writer settings: `UNCOMPRESSED`, dictionary **disabled**, `PLAIN`
encoding, data page v1. These settings plus seed, row count and generator
version are written to the file key/value metadata (`rvj.*` keys).

Determinism: file *i* uses `ChaCha8Rng::seed_from_u64(seed)` with stream *i*,
so output is byte-identical for a given (seed, rows, files, row-group-size,
container image) regardless of thread count.

Row-group size default: **65,536 rows (~69 MB)**, *not* 1,048,576.
A 1 Mi-row group is ~1.1 GB; parquet-java materializes a whole row group in
memory, so 4 workers would need ~4.4 GB against a 3 GB heap and the Scala
pipeline could not run inside the 4 GB envelope at all.

Presets (≈1,060 bytes/row on disk):

| preset | rows | files | approx. size |
|---|---|---|---|
| dev | 1,000,000 | 4 | ~1.06 GB |
| small | 10,000,000 | 8 | ~10.6 GB |
| full | 950,000,000 | 64 | ~1.0 TB (0.92 TiB) — requires `--yes` |

> **Paper inconsistency to fix:** §II-C says 1.2 billion rows ≈ 1 TB. At
> 1,060 B/row, 1.2 B rows ≈ 1.27 TB. Either state 950 M rows / 1.0 TB or
> 1.2 B rows / 1.27 TB.

## 3. Workload (identical in both languages)

Work unit = one (file, row group). Units are numbered in (file name, row-group
index) order; 4 workers pull units from a shared atomic counter.

Within a unit, rows are processed in **batches of 8,192 rows**:

1. **Materialize** each row into a language-native owned record
   (`Row { id: [u8;16], ts: i64, payload: String, metric: f64 }` in Rust;
   `case class Row(id: Array[Byte], ts: Long, payload: String, metric: Double)`
   in Scala). The batch is first collected into a `Vec<Row>` / `Array[Row]`.
   Rust owns its `String` (heap allocation + copy per row), matching the JVM's
   per-row `String`.
2. **Filter** `metric > 75.0`.
3. **Transform** into `Enriched`:
   - `hour_bucket = ts / 3_600_000`
   - `payload_hash = FNV-1a-64(payload bytes)`
   - `tag = first 8 chars of payload, ASCII-uppercased` (owned string)
4. **Aggregate** by `hour_bucket`: `count`, `sum(metric)`, `min(metric)`,
   `max(metric)`, `checksum = XOR of row_hash` where
   `row_hash = payload_hash ^ FNV-1a-64(tag) ^ id_lo ^ id_hi`
   (`id_lo`/`id_hi` = little-endian u64 halves of `id`).
   Folding `tag` and `id` into the checksum guarantees neither LLVM nor the
   JIT can dead-code-eliminate that work.
5. **Write** aggregates sorted by `hour_bucket` to one Parquet file
   (`UNCOMPRESSED`, no dictionary), then `fsync` the file.

Output schema: `hour_bucket INT64, count INT64, sum_metric DOUBLE,
min_metric DOUBLE, max_metric DOUBLE, checksum INT64` (u64 bits stored as
signed).

### Determinism of floating point

Each unit keeps its own partial aggregates; `sum` is accumulated in row order
within the unit. After all workers finish, partials are merged **in unit
order** (sum via sequential `+=`). Both languages use IEEE-754 binary64 with no
reassociation (Rust never reassociates; Java is strict-FP since 17), so the
two outputs are bit-identical and reproducible across runs.

`make verify` decodes both output files and compares every value bit-for-bit
(file bytes differ because of writer `created_by` metadata). It also compares
`rows_read` / `rows_kept` from the run summaries.

### Instrumentation (same format in both)

- `batches.csv`: `batch_id,worker,start_nanos,rows,nanos`
  - `batch_id` = `<file_idx>-<row_group>-<batch_idx>`
  - `nanos` = time since the previous batch of the same unit ended (or since
    the unit started, for batch 0). Batch 0 therefore includes opening the
    file and loading/locating the row group, in both languages, so the sum of
    batch times equals the unit time.
  - `start_nanos` is relative to process start (for correlating with GC).
  - Rows are buffered in memory and written after processing ends.
- `summary.json`: `lang, workers, batch_size, files, units, rows_read,
  rows_kept, groups, wall_nanos`. Wall time runs from process start to after
  the output `fsync`.

## 4. Rust pipeline (pipeline-rust/)

- Cargo workspace at repo root (members `datagen`, `pipeline-rust`); release
  profile `opt-level=3, lto="fat", codegen-units=1, panic="abort"`.
- Reader: `ParquetRecordBatchReaderBuilder` restricted to one row group,
  `batch_size = 8192`.
- Allocator: feature `system` (default) or `mimalloc`
  (`--no-default-features --features mimalloc`).
- Threads: `std::thread::scope`, 4 workers, no rayon.
- Group map: `std::collections::HashMap<i64, Agg>`.

## 5. Scala pipeline (pipeline-scala/)

- Plain JVM app, no Spark. Fat jar via sbt-assembly.
- Reader: `ParquetFileReader.open(LocalInputFile, ParquetReadOptions)`,
  `readRowGroup(i)`, and a custom `RecordMaterializer[Row]` via
  `MessageColumnIO.getRecordReader` (no intermediate `Group` objects).
- Threads: `Executors.newFixedThreadPool(4)`, 4 tasks pulling from an
  `AtomicInteger`.
- Group map: `scala.collection.mutable.LongMap[Agg]` (unboxed keys).
- Writer: `ExampleParquetWriter` on `LocalOutputFile`, then
  `FileChannel.force(true)`.

## 6. Docker and isolation (docker/) — after verify

`--cpus=4 --memory=4g --memory-swap=4g --cpuset-cpus=<4 physical cores>`;
`docker/pick-cores.sh` uses `lscpu -e` to choose 4 cores whose SMT siblings
stay idle. Data mounted read-only; output on a separate volume.

## 7. Benchmark harness (bench/) — after verify

Configurations: `rust-system`, `rust-mimalloc`, `scala-g1`, `scala-zgc`
(`-XX:+UseZGC -XX:+ZGenerational`), `scala-parallel`; all JVM runs
`-Xms3G -Xmx3G -XX:+AlwaysPreTouch`, JFR with `profiling/gc.jfc`,
`-Xlog:gc*:file=...`.

Measurement corrections relative to the original spec:

- **Memory:** cgroup v2 `memory.peak` includes page cache, which a streaming
  1 TB read fills up to the limit for *every* configuration. We sample
  `memory.stat` (`anon`, `kernel`, `file`) and process RSS at 10 Hz and report
  peak `anon + kernel` as the primary metric; `memory.peak` is kept as a
  secondary column.
- **Minimum-heap experiment:** because `-Xms3G` + `AlwaysPreTouch` pins JVM
  RSS ≥ 3 GB by construction, an additional experiment bisects the smallest
  `-Xmx` at which each GC completes the small preset.
- **Cost:** report both *provisioned* (4 vCPU × wall time) and *consumed*
  (`cpu.stat usage_usec`) vCPU-hours; rates come from a YAML file with
  placeholders, never hardcoded.
- **GC:** parse `jdk.GarbageCollection` **and** `jdk.GCPhasePause` via
  `jfr print --json`. ZGC's cost appears mainly as CPU, not pauses.
- Page cache dropped before each run (root). JVM warm-up runs on small preset
  (discarded). N=10 measured runs per config, randomized order.
- `results/<timestamp>/manifest.json`: git SHA, image digests, versions, CPU
  model, kernel, flags, core set.

## 8. Analysis (analysis/) — after verify

Mean, stddev, 95% CI (t-distribution); Mann-Whitney U + effect size
(Cliff's δ) between configurations; bootstrapped CIs for p99/p99.9 batch
latency; GC pause p50/p95/p99/max. IEEE-column (3.5 in) PDF+PNG plots and
booktabs LaTeX tables.

## 9. Threats to validity (tracked for README/paper)

1. arrow-rs and parquet-java differ in more than language: the Rust reader
   streams pages; parquet-java loads the whole row group and creates `Binary`
   slice objects per value. Part of any gap is library, not runtime.
2. JVM heap flags determine JVM memory footprint (see minimum-heap
   experiment).
3. Hash map choice differs by idiom (SipHash `HashMap` vs open-addressing
   `LongMap`).
4. Single node, 4 vCPU, no distributed shuffle: results speak to per-executor
   runtime cost, not cluster-level behaviour.
5. JIT warm-up is included in wall time for measured runs (as in production
   batch jobs); warm-up runs only warm the OS/disk, not the JVM process.

## 10. Status log

- **2026-10-23 — verify milestone reached** (development machine, Docker
  Desktop; *no performance numbers recorded*):
  - datagen dev preset: 4 files × 265,562,707 bytes; byte-identical across
    `--threads 1` and `--threads 4`.
  - Rust (system, mimalloc) and Scala (G1, ZGC generational, ParallelGC)
    outputs are bit-identical: 720 hourly groups, 1,000,000 rows read,
    841,320 kept (84.13%, consistent with P(N(100,25) > 75) = 0.8413).
  - Invariant to worker count (1/3/4) and batch size (1000/8192).
  - Verifier rejects a deliberately different output (negative control).
  - Batch logs from both languages cover the same 124 batch ids with equal
    row counts.
  - Known build note: sbt-assembly resolves 564 duplicate classpath entries
    with `MergeStrategy.first` (overlap between hadoop-client-api/runtime and
    parquet-java transitive deps); to be audited before the artifact release.

## 11. Order of work

datagen → Rust pipeline → Scala pipeline → `verify` on dev preset
→ **stop and report** → Docker → bench → analysis → CI/README/CITATION.

Development happens on Windows + Docker Desktop; all builds run in Linux
containers. Benchmark numbers for the paper come only from the bare-metal
Linux host in Table I.
