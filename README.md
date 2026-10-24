# rust-vs-jvm-runtime-tax

Reproducible artifact for *"Quantifying the Runtime Tax: Memory Inflation, CPU
Efficiency, and Cloud Infrastructure Costs of Native Rust vs. JVM Scala in Big
Data Platforms"* by Manoj Kumar Goud Sowdari and Akshay Kumar Goud Sowdari.

Two standalone pipelines run the **same workload** on the **same synthetic,
uncompressed Parquet dataset** inside **identical cgroup limits** (4 vCPU,
4 GB, no swap). One is native Rust and the other is Scala 3 on the JVM, with
no Spark or other framework. The harness measures:
- CPU time
- peak private memory
- per-batch latency
- GC pauses
- modeled cloud cost

Both pipelines produce bit-identical results, and every benchmark run checks
this.

No benchmark numbers are stored in this repository. Every figure and table is
regenerated from raw `results/` data.

## Layout

| Path | Contents |
|---|---|
| `datagen/` | Deterministic Parquet generator (Rust) |
| `pipeline-rust/` | Native pipeline: rustc 1.81.0, `arrow`/`parquet` 53.4.1; system or mimalloc allocator |
| `pipeline-scala/` | JVM pipeline: Scala 3.5.0, Temurin JDK 21.0.4+7, parquet-java 1.14.4 |
| `docker/` | Images (base images pinned by digest), in-container wrapper, core picker |
| `bench/` | `run.py` (production-style matrix), `heapsweep.py` (H_min search + heap sweep), `harness.py` (shared), `configs.yaml`, cgroup sampler, JFR parser, `verify.py` |
| `profiling/` | JFR settings (`gc.jfc`) |
| `analysis/` | `analyze.py` (statistics, figures, LaTeX tables), `heapsweep.py` (heap-sweep figures/tables), `cost.yaml` (rates you supply) |
| `results/` | Raw run output (git-ignored; schema in `results/README.md`) |
| `docs/` | `paper-deltas.md` (facts the paper must reflect), `fat-jar-merge.md` (dependency merge audit) |
| `PLAN.md` | Design rationale and status log |

## The workload

The work is split into units of one row group in one file. Four workers pull
units from a shared counter and process each unit in batches of 8,192 rows.
For each batch:

1. **Materialize** every row into an owned record: a Rust `struct` with an
   owned `String`, or a Scala `case class`.
2. **Filter** rows to those with `metric > 75.0`.
3. **Transform** each kept row:
   - `hour_bucket = ts / 3,600,000`
   - `payload_hash = FNV-1a-64(payload)`
   - `tag` = the first 8 characters of `payload`, uppercased
4. **Aggregate** by `hour_bucket`: count, sum, min and max of `metric`, plus
   an XOR checksum that folds in `payload_hash`, `tag` and `id`. Folding them
   in means the compiler or JIT can't eliminate any of that work.
5. **Write** the 720 hourly rows to a Parquet file, then `fsync` it.

Partial results are merged in unit order, so floating-point sums are
reproducible and the two languages agree to the bit.

**Latency.** The primary metric is **per-unit latency**: the time to process
one full 65,536-row row group, including opening the file and preparing the
row group. Its scope is identical in both languages.

The secondary metric is **per-batch latency**: the time for one 8,192-row
batch, excluding setup. It is not symmetric, because parquet-java loads the
whole row group during setup while arrow-rs reads pages lazily inside
batches. See `PLAN.md` §3 for the full specification.

## Dataset presets

| Preset | Rows | Files | Size on disk | Free disk to provision |
|---|---|---|---|---|
| `dev` | 1,000,000 | 4 | ≈1.06 GB | 3 GB |
| `small` | 10,000,000 | 8 | ≈10.6 GB | 15 GB |
| `full` | 950,000,000 | 64 | ≈1.01 TB (0.92 TiB) | ≥1.2 TB (input plus headroom for the OS, images and results) |

Data is UNCOMPRESSED, PLAIN-encoded, with no dictionary, in row groups of
65,536 rows (≈69 MB). That row-group size keeps four in-flight row groups well
inside the 4 GB limit. Data is generated deterministically: seed 42 gives
byte-identical files for a given container image. `datagen` refuses to write
more than 100 GB without `--yes`.

## Requirements

**Benchmark host (for numbers that go in the paper).** The paper's
configuration is in Table I: AMD EPYC 9354, 256 GB RAM, NVMe, Ubuntu 24.04,
kernel 6.8. The harness requires all of the following, and refuses to record
timings without them:

- Linux on **bare metal**: the CPU must not report the `hypervisor` flag
- **cgroup v2**: `stat -fc %T /sys/fs/cgroup` prints `cgroup2fs`
- root, or passwordless `sudo`, to drop the page cache before each run
- Docker Engine
- Python 3.12 with `pip install -r bench/requirements.txt -r analysis/requirements.txt`

Recommended host settings:
- CPU governor set to `performance`
- boost/turbo state fixed. Leave it on or off as you prefer, but it gets recorded either way.
- SMT disabled, or the benchmark cores isolated with `isolcpus=`

The manifest records every one of these settings.

**Development and correctness (any OS with Docker, or native):**
- rustc 1.81.0
- Temurin JDK 21.0.4+7
- sbt 1.10.2
- Python 3.12 with `bench/requirements.txt`

Windows and macOS machines, and Docker Desktop, are for correctness checks
only.

## Reproduce

```sh
# 1. Correctness (native toolchain): unit tests, then dev-preset end-to-end check
make build
make test PRESET=dev

# 2. Images
make images

# 3. Data (on the benchmark host)
make datagen PRESET=small
make datagen PRESET=full            # ≈1 TB; passes --yes for you

# 4. Benchmark: small preset first to validate the host, then the full preset
make bench DATA=data/small WARMUP_DATA=data/small
make bench DATA=data/full  WARMUP_DATA=data/small

# 5. Heap-size sweep (H_min search + 1.25x-4x H_min; small preset by default)
make heapsweep

# 6. Analysis: latest results directory by default (or RESULTS=results/<ts>)
make plot
```

**Two JVM experiments.**
- `make bench` runs the *production-style* JVM configuration: a fixed,
  pre-touched 3 GiB heap, as a capacity-planned service would run. Its memory
  figure reflects the setting.
- `make heapsweep` measures what each collector actually needs, following
  Hertz & Berger (OOPSLA 2005):
  1. Binary-search the smallest heap `H_min` that completes (128 MiB steps; a
     heap size passes only if 3 of 3 attempts complete).
  2. Run each collector at 1.25×, 1.5×, 2×, 3× and 4× its own `H_min`, with
     `-Xms = -Xmx` and **no pre-touch**. Points that don't fit the 4 GB
     cgroup alongside a 768 MiB off-heap reserve are skipped.
  3. Rust runs alongside as flat reference lines.

  Its outputs are CPU-seconds vs heap size and M_peak vs heap size.

`make bench` calls `bench/run.py`. Useful options:
- `--configs rust-system,scala-g1` runs a subset of configurations
- `--runs N` sets the number of measured runs (default 10)
- `--cpuset 2,4,6,8` pins specific cores. The default lets `docker/pick-cores.sh` choose 4 physical cores and leave their SMT siblings unused.
- `--seed S` fixes the run order (the default is random and is recorded)
- `--dry-run` tests the plumbing on a non-qualifying host. Its results are marked `-dryrun`, and the analysis watermarks them.

Fill in `analysis/cost.yaml` with a per-vCPU-hour rate and its source before
`make plot` if you want the cost table. No prices are hardcoded.

## What gets measured

| Metric | Source |
|---|---|
| CPU time | container cgroup `cpu.stat usage_usec` at exit, captured inside the container |
| Wall time | in-container start/end of the pipeline process (includes JVM startup); the pipeline's own main→fsync time is also logged |
| **M_peak** | max over 10 Hz host samples of `memory.stat anon + shmem`. `shmem` is required because ZGC backs its heap with memfd shared memory. |
| Page cache, `memory.current`, `memory.peak` | reported separately; never part of M_peak |
| Per-unit latency (primary) and per-batch latency (secondary) | `batches.csv` from both pipelines |
| H_min and heap-sweep curves | `bench/heapsweep.py` (JVM), with Rust reference runs in the same session |
| GC pauses, heap used/committed | JFR (`profiling/gc.jfc`): `jdk.GCPhasePause`, `jdk.GarbageCollection`, `jdk.GCHeapSummary` |
| JVM native memory | one extra NMT run per JVM config, excluded from timing statistics |
| Cost | provisioned (4 vCPU × wall time) and consumed vCPU-hours × your `cost.yaml` rate |

Run protocol:
- page cache dropped before every run
- 2 discarded JVM warm-up runs per JVM config
- 10 measured runs per config, in randomized order

Statistics:
- mean ± 95% CI (t-distribution)
- Mann–Whitney U and Cliff's δ against `rust-system`
- cluster-bootstrap CIs (resampling whole runs) for p99 and p99.9 latency

Figures are 3.5-inch IEEE single-column PDF/PNG with embedded fonts, and
tables are booktabs `.tex` for `\input{}`.

## Configurations

Production-style matrix (`make bench`). The heap sweep reuses the same GC
flags with its own heap sizes and no pre-touch.

| Name | Runtime | Flags |
|---|---|---|
| `rust-system` | native | glibc malloc |
| `rust-mimalloc` | native | mimalloc 0.1.43 |
| `scala-g1` | JVM | `-Xms3G -Xmx3G -XX:+AlwaysPreTouch -XX:+UseG1GC` |
| `scala-zgc` | JVM | `-Xms3G -Xmx3G -XX:+AlwaysPreTouch -XX:+UseZGC -XX:+ZGenerational` |
| `scala-parallel` | JVM | `-Xms3G -Xmx3G -XX:+AlwaysPreTouch -XX:+UseParallelGC` |

All containers run with `--cpus=4 --memory=4g --memory-swap=4g
--cpuset-cpus=<4 physical cores> --network none`.

## Dependencies

Rust (exact versions pinned in `Cargo.lock`; MSRV 1.81):
- `arrow` 53.4.1 and `parquet` 53.4.1 (all 13 arrow/parquet crates on 53.4.1)
- `mimalloc` 0.1.43 (optional)
- datagen only: `rand` 0.8.5, `rand_chacha` 0.3.1, `rand_distr` 0.4.3

Scala (`pipeline-scala/build.sbt`):

| Dependency | Version | Why |
|---|---|---|
| `org.apache.parquet:parquet-hadoop` | 1.14.4 | Parquet reader/writer (`LocalInputFile`/`LocalOutputFile`) |
| `org.apache.hadoop:hadoop-client-api` | 3.3.6 | parquet-hadoop needs Hadoop `Configuration` and codec classes, which it declares as `provided` |
| `org.apache.hadoop:hadoop-client-runtime` | 3.3.6 | the shaded runtime for the above |
| `org.slf4j:slf4j-nop` | 2.0.16 | silences parquet-java logging |
| `org.scalameta:munit` | 1.0.2 | tests only |

Transitive runtime jars:
- from parquet-java: `parquet-column`, `parquet-common`, `parquet-encoding`, `parquet-format-structures`, `parquet-jackson`
- `jackson-core`/`databind`/`annotations`/`datatype-jdk8` 2.17.0
- `snappy-java`, `zstd-jni`, `aircompressor`
- `commons-pool`, `commons-logging`, `javax.annotation-api`, `jsr305`, `byte-buddy`
- the Scala 3 standard library

The fat-jar merge policy is explicit and was audited: there are no `.class`
conflicts. See `docs/fat-jar-merge.md`.

## Threats to validity

1. **Library differences are a confound.** The two pipelines share a
   workload, but each language uses a different Parquet library.
   - arrow-rs decodes columnar batches and reads pages lazily, so Rust's page
     I/O falls inside batch latency.
   - parquet-java's `readRowGroup` reads the whole row group up front (so
     Scala's I/O falls in `setup_nanos`) and assembles records through
     per-value `Binary` objects.

   Part of any measured gap is library design, not the language runtime.
2. **Heap sizing fixes the JVM's footprint.** In the production-style runs,
   `-Xms3G` with `AlwaysPreTouch` puts JVM M_peak at 3 GB or more by
   construction. Read that figure as "memory the configuration holds." The
   heap sweep measures "memory the workload needs."

   `H_min` depends on the dataset and on the 128 MiB search granularity, and
   it is measured on the small preset. Its 3-of-3 pass rule makes it
   conservative, but it is not a guarantee: a run near 1.25× `H_min` can
   still fail occasionally. Such failures are counted and reported per point.
3. **Hash maps follow each language's idiom:** Rust's std `HashMap` (SipHash)
   versus Scala's `mutable.LongMap` (open addressing, no key boxing).
4. **JIT warm-up is included.** Each run is a fresh process, as in a batch
   job. JVM warm-up runs warm the OS and disk, not the JIT.
5. **Recording overhead.** JFR is enabled on every JVM run, at low overhead
   with GC events only. Rust runs have no equivalent instrumentation.
6. **Scope.** One node, 4 vCPU, no distributed shuffle and no network. The
   results describe per-executor runtime cost, not cluster-level behavior.
7. **Isolation.** CPU pinning leaves SMT siblings unassigned but does not
   reserve them. Use `isolcpus` or disable SMT for strict isolation. The
   host state is recorded in `manifest.json`.

## Citation

See `CITATION.cff`. A Zenodo DOI will be added on release.

## License

Apache-2.0 (`LICENSE`).
