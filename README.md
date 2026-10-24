# rust-vs-jvm-runtime-tax

Reproducible artifact for *"Quantifying the Runtime Tax: Memory Inflation, CPU
Efficiency, and Cloud Infrastructure Costs of Native Rust vs. JVM Scala in Big
Data Platforms"* (M. K. G. Sowdari, A. K. G. Sowdari).

> **Status:** work in progress. The data generator, both pipelines and the
> correctness verifier are done. The Docker isolation, benchmark harness and
> analysis are not built yet. See [PLAN.md](PLAN.md) for the full design.

## What exists today

| Path | Contents |
|---|---|
| `datagen/` | Deterministic Parquet generator (Rust) |
| `pipeline-rust/` | Native pipeline (Rust 1.81, arrow-rs/parquet 53.4.1) |
| `pipeline-scala/` | JVM pipeline (Scala 3.5.0, JDK 21, parquet-java 1.14.4) |
| `bench/verify.py` | Checks that outputs are bit-identical and inputs are encoded as specified |
| `tests/integration.sh` | End-to-end correctness test |

## Quick start (native toolchain)

Requires rustc 1.81.0, JDK 21.0.4, sbt 1.10.2, and Python 3.12 with
`pip install -r bench/requirements.txt`.

```sh
make build            # Rust (system + mimalloc) and the Scala fat jar
make test             # unit tests + integration test on the dev preset (~1.1 GB)
make verify PRESET=dev
```

## License

Apache-2.0
