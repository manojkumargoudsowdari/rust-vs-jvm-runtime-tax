# Native-toolchain targets (rustc 1.81.0, JDK 21.0.4, sbt 1.10.2, Python 3.12).
# Docker-based build/bench targets are added with the benchmark harness.

PRESET ?= dev
DATA ?= data/$(PRESET)
OUT ?= out/$(PRESET)
PYTHON ?= python3
SCALA_JAR := pipeline-scala/target/scala-3.5.0/pipeline-scala.jar
JVM_HEAP := -Xms3G -Xmx3G

.PHONY: build build-rust build-rust-mimalloc build-scala datagen run verify test bench plot clean

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

bench plot:
	@echo "'$@' is not implemented yet (PLAN.md §6-8)"; exit 1

clean:
	cargo clean
	cd pipeline-scala && sbt -batch clean
	rm -rf out
