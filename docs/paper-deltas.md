# Paper deltas

Every fact in the paper draft (Sections I–II, as of 2026-10-23) that the
artifact contradicts, refines, or adds. Each entry gives the paper's current
text, what the artifact actually does, and suggested replacement wording.
Strike an entry once the LaTeX is updated.

Status key: **MUST** = paper is currently wrong or incomplete;
**ADD** = new information the paper needs; **WORDING** = framing.

---

## A. Software versions (§II-B)

| # | Status | Paper says | Artifact uses | Suggested text |
|---|---|---|---|---|
| A1 | ADD | — (no Rust library named) | `arrow` / `parquet` crates **53.4.1** (all 13 arrow/parquet crates locked to 53.4.1) | "…reads Parquet via the Apache Arrow Rust implementation (`arrow`/`parquet` crates 53.4.1)…" |
| A2 | ADD | — (no JVM library named) | parquet-java (`parquet-hadoop`) **1.14.4**, `hadoop-client-api`/`hadoop-client-runtime` **3.3.6** (shaded), `slf4j-nop` 2.0.16 | "…reads Parquet via parquet-java 1.14.4 with `LocalInputFile`; Hadoop 3.3.6 shaded client jars supply configuration classes only…" |
| A3 | OK | rustc 1.81.0, `--release`, `opt-level = 3` | same, **plus** `lto = "fat"`, `codegen-units = 1`, `panic = "abort"` | add the three extra profile settings |
| A4 | OK | Scala 3.5.0, JDK 21.0.4 | Scala 3.5.0, Temurin 21.0.4+7, sbt 1.10.2, sbt-assembly 2.2.0 | name the Temurin build and the build tools |
| A5 | ADD | — | base images pinned by digest (see `docker/*.Dockerfile`) | one sentence + artifact DOI |

## B. Configurations (§II-B)

| # | Status | Paper says | Artifact uses | Suggested text |
|---|---|---|---|---|
| B1 | MUST | `-XX:+UseZGC` | `-XX:+UseZGC -XX:+ZGenerational` (generational ZGC; on JDK 21 the flag is required, otherwise single-generation ZGC runs) | "…the generational Z Garbage Collector (`-XX:+UseZGC -XX:+ZGenerational`)…" |
| B2 | ADD | two GC variants (ZGC, G1) | **three**: G1, generational ZGC, **ParallelGC** (`-XX:+UseParallelGC`, throughput-oriented reference) | add ParallelGC as a third sub-variant |
| B3 | ADD | one Rust variant | **two**: system allocator (glibc malloc) and **mimalloc** 0.1.43 | "…benchmarked with the system allocator and with mimalloc, to separate allocator cost from GC cost." |
| B4 | ADD | `-Xms3G -Xmx3G` | plus `-XX:+AlwaysPreTouch` on all JVM runs; JFR (`profiling/gc.jfc`) and `-Xlog:gc*` enabled | list all JVM flags in a table |
| B5 | ADD | — | one extra **NMT run** per JVM config (`-XX:NativeMemoryTracking=summary`), excluded from timing | describe as memory-accounting run |
| B6 | ADD | — | **minimum-heap experiment** (smallest `-Xmx` that completes the small preset, per GC) | needed because `-Xms3G` + `AlwaysPreTouch` fixes JVM RSS ≥ 3 GB by construction |

## C. Dataset (§II-C)

| # | Status | Paper says | Artifact uses | Suggested text |
|---|---|---|---|---|
| C1 | **MUST** | "1-Terabyte … containing 1.2 billion rows" | full preset = **950,000,000 rows ≈ 1.007 TB** (≈1,060 bytes/row on disk); 1.2 B rows would be ≈ 1.27 TB | "…a synthetic, uncompressed ≈1.0 TB dataset of 950 million rows…" |
| C2 | ADD | schema `{id: UUID, ts: Int64, payload: String(1024), metric: Float64}` | id = FixedSizeBinary(16) UUIDv4; ts = epoch-ms uniform over 30 days from 2024-01-01; payload = exactly 1024 chars from `[A-Za-z0-9_-]`; metric ~ Normal(100, 25) clamped ≥ 0; all columns REQUIRED | give the distributions |
| C3 | ADD | — | **row groups of 65,536 rows (~69 MB)**, 64 files | "Row groups hold 65,536 rows (≈69 MB) so that four concurrent row groups fit well within the 4 GB envelope." |
| C4 | ADD | "uncompressed" | UNCOMPRESSED, dictionary **disabled**, PLAIN encoding, data page v1 | state all three |
| C5 | ADD | — | deterministic generator (ChaCha8, seed 42, one stream per file); byte-identical regeneration | reproducibility sentence |

## D. Workload (paper has no workload section yet — §IV)

| # | Status | Content |
|---|---|---|
| D1 | **MUST** | Exact steps, identical in both languages, per (file, row group) unit, in batches of **8,192 rows**: (1) materialize each row into an owned language-native record (Rust `struct` with owned `String`; Scala `case class`); (2) filter `metric > 75.0`; (3) transform: `hour_bucket = ts / 3,600,000`, `payload_hash = FNV-1a-64(payload)`, `tag = upper(payload[0..8])`; (4) aggregate by `hour_bucket`: count, sum, min, max of `metric`, and XOR of `payload_hash ⊕ FNV-1a-64(tag) ⊕ id_lo ⊕ id_hi`; (5) write the 720 hourly aggregates, sorted, to one Parquet file and `fsync`. |
| D2 | ADD | Parallelism: 4 workers pull (file, row group) units from a shared counter (Rust: `std::thread::scope`; Scala: `Executors.newFixedThreadPool(4)`). |
| D3 | ADD | Correctness: both pipelines produce **bit-identical** aggregates (partials merged in unit order), verified on the dev (1 M rows) and small (10 M rows) presets for all five configurations. |
| D4 | ADD | Per-batch latency definition: **time to process one 8,192-row record batch** (decode + materialize + filter + transform + aggregate). Per-unit setup (open, footer, row-group preparation) is logged separately. |

## E. Measurement (§II-D)

| # | Status | Paper says | Artifact does | Suggested text |
|---|---|---|---|---|
| E1 | **MUST** | M_peak = max RSS(t) | cgroup v2 `memory.peak` includes page cache, which a streaming read fills to the limit for every configuration. **M_peak = max over 10 Hz samples of `memory.stat anon + shmem`.** `shmem` must be included: ZGC backs the Java heap with memfd shared memory, which the kernel accounts as `shmem` (inside `file`), not `anon` — `anon` alone under-reports ZGC by the full heap size (≈3 GB). Page cache (`file − shmem`), `memory.current` and `memory.peak` are reported separately. | "M_peak is the peak private memory of the container cgroup, `anon + shmem`, sampled at 10 Hz; page cache is excluded and reported separately. Shared memory is included because ZGC maps its heap through memfd." |
| E2 | MUST | "detached native eBPF host profiler" at 10 Hz | host-side sampler reading cgroup v2 files (`memory.stat`, `memory.current`, `memory.peak`, `cpu.stat`) at 10 Hz; final totals captured in-container at exit. eBPF is optional (`profiling/`). | describe the actual sampler, or implement eBPF before claiming it |
| E3 | ADD | E_compute integral of CPU utilization | `cpu.stat usage_usec` of the container cgroup at exit (exact kernel accounting, equals the integral) | note equivalence |
| E4 | ADD | GC pauses via JFR | `jdk.GarbageCollection` + `jdk.GCPhasePause` (+ `jdk.GCHeapSummary` for heap used/committed) parsed with `jfr print --json`; ZGC's cost appears mainly as concurrent CPU, not pauses | mention both event types and the ZGC caveat |
| E5 | ADD | cost from vCPU-hours | both **provisioned** (4 vCPU × wall time — what clouds bill) and **consumed** (`usage_usec`) vCPU-hours; rates from a user-supplied YAML (no hardcoded prices) | report both |
| E6 | ADD | — | protocol: page cache dropped before every run; 2 discarded JVM warm-up runs on the small preset; N = 10 measured runs per config in randomized order | add a "Protocol" paragraph |
| E7 | ADD | — | statistics: mean ± 95 % CI (t), Mann–Whitney U with Cliff's δ, bootstrapped CIs for p99/p99.9 | add a "Statistical treatment" paragraph |

## F. Framing (Abstract, §I, §II)

| # | Status | Issue |
|---|---|---|
| F1 | WORDING | Abstract states results ("eliminates long-tail p99 latency spikes", "reduces overall cloud infrastructure costs") before measurements exist; restate as hypotheses until data supports them. |
| F2 | MUST | §I roadmap says §II = related work, §III = setup; the draft's §II is the setup and there is no related-work section. |
| F3 | WORDING | "airtight", "peer-reviewed evaluation", "exact GC tax", "completely eliminating" — soften. |
| F4 | WORDING | Title/scope: single node, 4 vCPU, no distributed shuffle; scope the claim to per-executor runtime cost. |
| F5 | ADD | Threats to validity: arrow-rs vs parquet-java library differences (eager row-group load vs lazy page reads, per-value `Binary` objects); hash-map idiom (SipHash `HashMap` vs `LongMap`); heap sizing fixes JVM footprint; JIT warm-up included in wall time. |
