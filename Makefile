# Correctness targets use the native toolchain (rustc 1.81.0, JDK 21.0.4, sbt 1.10.2,
# Python 3.12). Benchmark targets use the Docker images and must run on a bare-metal
# Linux host with cgroup v2 (bench/run.py refuses otherwise unless DRY_RUN=1).

PRESET ?= dev
DATA ?= data/$(PRESET)
WARMUP_DATA ?= data/small
OUT ?= out/$(PRESET)
PYTHON ?= python3
SCALA_JAR := pipeline-scala/target/scala-3.5.0/pipeline-scala.jar
JVM_HEAP := -Xms3G -Xmx3G
BENCH_ARGS ?=
RESULTS ?= $(lastword $(sort $(wildcard results/2*)))

.PHONY: build build-rust build-rust-mimalloc build-scala datagen run verify test \
        images bench plot clean

# --------------------------------------------------------------- native build + correctness

build: build-rust build-rust-mimalloc build-scala

build-rust:
	cargo build --release --locked --workspace

# Separate target dir so the mimalloc binary does not overwrite the system one.
build-rust-mimalloc:
	cargo build --release --locked -p pipeline-rust --no-default-features --features mimalloc \
		--target-dir target/mimalloc

build-scala:
	cd pipeline-scala && sbt -batch test assembly

datagen: build-rust
	target/release/datagen --preset $(PRESET) --out $(DATA) $(if $(filter full,$(PRESET)),--yes)

run: build
	mkdir -p $(OUT)
	target/release/pipeline-rust --input $(DATA) --output $(OUT)/rust.parquet \
		--summary $(OUT)/rust.json --batch-log $(OUT)/rust-batches.csv
	java $(JVM_HEAP) -XX:+UseG1GC -jar $(SCALA_JAR) --input $(DATA) --output $(OUT)/scala.parquet \
		--summary $(OUT)/scala.json --batch-log $(OUT)/scala-batches.csv

verify: run
	$(PYTHON) bench/verify.py $(OUT)/rust.parquet $(OUT)/scala.parquet \
		--summary $(OUT)/rust.json $(OUT)/scala.json --input-dir $(DATA)

test: build
	cargo test --release --locked --workspace
	PRESET=$(PRESET) PYTHON=$(PYTHON) tests/integration.sh

# --------------------------------------------------------------- benchmark (Linux host)

images:
	docker build -f docker/rust.Dockerfile -t rvj-rust:latest .
	docker build -f docker/scala.Dockerfile -t rvj-scala:latest .

# Needs root or passwordless sudo for `echo 3 > /proc/sys/vm/drop_caches`.
bench: images
	$(PYTHON) bench/run.py --data $(DATA) --warmup-data $(WARMUP_DATA) $(if $(DRY_RUN),--dry-run) $(BENCH_ARGS)

plot:
	@test -n "$(RESULTS)" || { echo "no results/ directory; run 'make bench' first"; exit 1; }
	$(PYTHON) analysis/analyze.py $(RESULTS) $(if $(DRY_RUN),--allow-dry-run)

clean:
	cargo clean
	cd pipeline-scala && sbt -batch clean
	rm -rf out
