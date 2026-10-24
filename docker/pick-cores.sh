#!/usr/bin/env bash
# Print a --cpuset-cpus list of N logical CPUs on N distinct physical cores of
# one NUMA node, so that each chosen core's SMT sibling(s) stay unassigned.
#
# Usage: pick-cores.sh [N=4] [--skip-core 0]
#   --skip-core K   avoid physical core K (default 0: housekeeping/IRQs)
#
# Siblings are left *unassigned*, not reserved: for strict isolation also boot
# with isolcpus=/nohz_full= covering the chosen cores and their siblings, or
# disable SMT (echo off > /sys/devices/system/cpu/smt/control).
set -euo pipefail

N=${1:-4}
SKIP_CORE=0
if [[ ${2:-} == --skip-core ]]; then SKIP_CORE=${3:?}; fi

command -v lscpu >/dev/null || { echo "pick-cores: lscpu not found" >&2; exit 1; }

# lscpu -p=CPU,CORE,SOCKET,NODE prints "cpu,core,socket,node" (node may be empty).
lscpu -p=CPU,CORE,SOCKET,NODE | grep -v '^#' | awk -F, -v n="$N" -v skip="$SKIP_CORE" '
  {
    key = $3 ":" $2                      # socket:core identifies a physical core
    node = ($4 == "" ? 0 : $4)
    if (!(key in first)) { first[key] = $1; corenode[key] = node; order[++k] = key }
  }
  END {
    # Prefer the node with the most physical cores; take the first n of them.
    for (i = 1; i <= k; i++) count[corenode[order[i]]]++
    best = -1; for (nd in count) if (best < 0 || count[nd] > count[best]) best = nd
    picked = 0
    for (i = 1; i <= k && picked < n; i++) {
      key = order[i]; split(key, p, ":")
      if (corenode[key] != best || p[2] == skip) continue
      out = out (picked ? "," : "") first[key]; picked++
    }
    if (picked < n) { print "pick-cores: only " picked " eligible physical cores" > "/dev/stderr"; exit 1 }
    print out
  }'
