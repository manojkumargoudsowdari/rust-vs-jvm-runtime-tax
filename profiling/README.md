# profiling/

| File | Purpose |
|---|---|
| `gc.jfc` | JFR settings used by every measured JVM run: `jdk.GarbageCollection`, `jdk.GCPhasePause`, `jdk.GCHeapSummary`, `jdk.ZAllocationStall` and configuration events only, to keep recording overhead low. |

CPU time and memory come from cgroup v2 accounting (`cpu.stat`,
`memory.stat`, `memory.current`, `memory.peak`), sampled at 10 Hz from the
host by `bench/sampler.py` and snapshotted in-container at exit by
`docker/run-wrapper.sh`.
