#!/bin/sh
# Container entrypoint: run the pipeline, then snapshot this container's own
# cgroup v2 counters before exit (the host-side cgroup directory disappears
# as soon as the container stops, so final totals are captured from inside).
#
# Usage: run-wrapper.sh <command> [args...]
# Writes to $RVJ_OUT (default /out): stdout.txt, stderr.txt, wrapper.json,
# cgroup_final/{cpu.stat,memory.stat,memory.peak,memory.current,memory.events}.
#
# Timing: /proc/uptime is not stepped by NTP or VM clock resyncs (10 ms
# resolution) and is the primary wall time; `date +%s%N` (realtime clock) is
# recorded alongside for reference only.
set -u
OUT=${RVJ_OUT:-/out}
mkdir -p "$OUT/cgroup_final"

read -r start_up _ </proc/uptime
start_ns=$(date +%s%N)
"$@" >"$OUT/stdout.txt" 2>"$OUT/stderr.txt"
rc=$?
end_ns=$(date +%s%N)
read -r end_up _ </proc/uptime

for f in cpu.stat memory.stat memory.peak memory.current memory.events memory.max cpu.max cpuset.cpus.effective; do
  if [ -r "/sys/fs/cgroup/$f" ]; then
    cat "/sys/fs/cgroup/$f" >"$OUT/cgroup_final/$f"
  fi
done
printf '{"start_ns":%s,"end_ns":%s,"start_uptime_s":"%s","end_uptime_s":"%s","exit_code":%s}\n' \
  "$start_ns" "$end_ns" "$start_up" "$end_up" "$rc" >"$OUT/wrapper.json"
exit "$rc"
