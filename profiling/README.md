# profiling/

| File | Purpose |
|---|---|
| `gc.jfc` | JFR settings used by every JVM run: `jdk.GarbageCollection`, `jdk.GCPhasePause`, `jdk.GCHeapSummary`, `jdk.ZAllocationStall` and configuration events only, to keep recording overhead low. |

**eBPF.** The paper draft mentions an eBPF host profiler. The harness does not
use one: CPU time and memory come from cgroup v2 accounting files
(`cpu.stat`, `memory.stat`, `memory.current`, `memory.peak`), sampled at 10 Hz
from the host by `bench/sampler.py`. Kernel accounting gives exact
cumulative CPU time without probe overhead. If eBPF data is wanted (for
example off-CPU time or run-queue latency per container), add the scripts
here and update `docs/paper-deltas.md` (entry E2) to match.
