#!/usr/bin/env bash
# End-to-end correctness test on the dev preset (or $PRESET):
#   1. datagen is deterministic across thread counts
#   2. Rust and Scala (G1, ZGC, ParallelGC) produce bit-identical aggregates
#   3. results do not depend on worker count or batch size
#   4. inputs are UNCOMPRESSED / PLAIN / no dictionary
# Requires built artifacts (make build) plus java 21 and python3 with pyarrow.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
PRESET=${PRESET:-dev}
WORK=${WORK:-$(mktemp -d)}
DATAGEN=${DATAGEN:-$ROOT/target/release/datagen}
RUST_BIN=${RUST_BIN:-$ROOT/target/release/pipeline-rust}
RUST_MIMALLOC_BIN=${RUST_MIMALLOC_BIN:-$ROOT/target/mimalloc/release/pipeline-rust}
SCALA_JAR=${SCALA_JAR:-$ROOT/pipeline-scala/target/scala-3.5.0/pipeline-scala.jar}
JAVA=${JAVA:-java}
PYTHON=${PYTHON:-python3}
JVM_HEAP=(-Xms3G -Xmx3G)

log() { printf '\n== %s\n' "$*"; }

log "datagen determinism (threads=4 vs threads=1)"
"$DATAGEN" --rows 200000 --files 3 --row-group-size 50000 --out "$WORK/det-a" --threads 4
"$DATAGEN" --rows 200000 --files 3 --row-group-size 50000 --out "$WORK/det-b" --threads 1
(cd "$WORK/det-a" && sha256sum ./*.parquet) > "$WORK/det-a.sha256"
(cd "$WORK/det-b" && sha256sum -c "$WORK/det-a.sha256")

log "datagen preset=$PRESET"
DATA="$WORK/data"
"$DATAGEN" --preset "$PRESET" --out "$DATA"

log "rust pipeline"
"$RUST_BIN" --input "$DATA" --output "$WORK/rust.parquet" \
  --summary "$WORK/rust.json" --batch-log "$WORK/rust-batches.csv"
"$RUST_BIN" --input "$DATA" --output "$WORK/rust-w1.parquet" --workers 1 --batch-size 1000 >/dev/null
OUTPUTS=("$WORK/rust.parquet" "$WORK/rust-w1.parquet")
if [[ -x "$RUST_MIMALLOC_BIN" ]]; then
  "$RUST_MIMALLOC_BIN" --input "$DATA" --output "$WORK/rust-mimalloc.parquet" >/dev/null
  OUTPUTS+=("$WORK/rust-mimalloc.parquet")
fi

log "scala pipeline"
"$JAVA" "${JVM_HEAP[@]}" -XX:+UseG1GC -jar "$SCALA_JAR" --input "$DATA" --output "$WORK/scala-g1.parquet" \
  --summary "$WORK/scala.json" --batch-log "$WORK/scala-batches.csv"
"$JAVA" "${JVM_HEAP[@]}" -XX:+UseZGC -XX:+ZGenerational -jar "$SCALA_JAR" \
  --input "$DATA" --output "$WORK/scala-zgc.parquet" >/dev/null
"$JAVA" "${JVM_HEAP[@]}" -XX:+UseParallelGC -jar "$SCALA_JAR" \
  --input "$DATA" --output "$WORK/scala-parallel.parquet" --workers 1 --batch-size 1000 >/dev/null
OUTPUTS+=("$WORK/scala-g1.parquet" "$WORK/scala-zgc.parquet" "$WORK/scala-parallel.parquet")

log "verify"
"$PYTHON" "$ROOT/bench/verify.py" "${OUTPUTS[@]}" \
  --summary "$WORK/rust.json" "$WORK/scala.json" --input-dir "$DATA"

log "batch logs cover the same batches"
diff <(cut -d, -f1,4 "$WORK/rust-batches.csv") <(cut -d, -f1,4 "$WORK/scala-batches.csv")
echo "OK: identical batch_id/rows sequence ($(($(wc -l <"$WORK/rust-batches.csv") - 1)) batches)"

log "negative control: verify must reject a different output"
mkdir -p "$WORK/subset" && ln -sf "$DATA/part-00000.parquet" "$WORK/subset/"
"$RUST_BIN" --input "$WORK/subset" --output "$WORK/rust-subset.parquet" >/dev/null
if "$PYTHON" "$ROOT/bench/verify.py" "$WORK/rust.parquet" "$WORK/rust-subset.parquet" 2>/dev/null; then
  echo "FAIL: verify accepted mismatching outputs" >&2
  exit 1
fi
echo "OK: mismatch detected"

log "PASS (work dir: $WORK)"
