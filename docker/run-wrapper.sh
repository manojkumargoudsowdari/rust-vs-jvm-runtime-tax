#!/bin/sh
# Container entrypoint: run the pipeline, then snapshot this container's own
# cgroup v2 counters before exit (the host-side cgroup directory disappears
# as soon as the container stops, so final totals are captured from inside).
#
# Usage: run-wrapper.sh <command> [args...]
# Writes to $RVJ_OUT (default /out): stdout.txt, stderr.txt, wrapper.json,
# cgroup_final/{cpu.stat,memory.stat,memory.peak,memory.current,memory.events}.
set -u
OUT=${RVJ_OUT:-/out}
mkdir -p "$OUT/cgroup_final"

start_ns=$(date +%s%N)
"$@" >"$OUT/stdout.txt" 2>"$OUT/stderr.txt"
rc=$?
end_ns=$(date +%s%N)

for f in cpu.stat memory.stat memory.peak memory.current memory.events memory.max cpu.max cpuset.cpus.effective; do
  if [ -r "/sys/fs/cgroup/$f" ]; then
    cat "/sys/fs/cgroup/$f" >"$OUT/cgroup_final/$f"
  fi
done
printf '{"start_ns":%s,"end_ns":%s,"exit_code":%s}\n' "$start_ns" "$end_ns" "$rc" >"$OUT/wrapper.json"
exit "$rc"
