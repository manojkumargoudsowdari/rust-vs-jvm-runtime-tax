# results/

Each benchmark invocation writes `results/<UTC timestamp>/`. Run directories
are git-ignored and are archived alongside the paper instead.

```
results/<timestamp>/
├── manifest.json        # host, toolchain, image digests, flags, protocol
├── runs.csv             # one row per run (measured, warmup, nmt)
└── runs/<run_id>/
    ├── summary.json         # pipeline summary (rows, groups, wall_nanos)
    ├── batches.csv          # batch_id,worker,start_nanos,rows,nanos
    ├── cgroup_samples.csv   # 10 Hz host samples of memory.stat / memory.current / memory.peak / cpu.stat
    ├── cgroup_final/        # in-container snapshot of cpu.stat, memory.* at exit
    ├── wrapper.json         # in-container start/end timestamps and exit code
    ├── output.parquet       # aggregates (checked bit-identical across runs)
    ├── stdout.txt, stderr.txt
    └── (JVM only) gc.jfr, gc.log, gc_events.csv, heap.csv, nmt.txt (NMT runs)
```

## runs.csv columns

| column | meaning |
|---|---|
| `run_id` | `<order>-<config>-<kind>` |
| `config` | configuration name from `bench/configs.yaml` |
| `kind` | `measured`, `warmup` (discarded) or `nmt` (memory accounting only; excluded from timing) |
| `order` | position in the randomized schedule |
| `preset` | dataset preset |
| `exit_code`, `oom_killed` | container outcome |
| `wall_ns` | in-container wall time of the pipeline process (start → exit, includes JVM startup) |
| `pipeline_wall_ns` | wall time reported by the pipeline itself (main → output fsync) |
| `cpu_usage_usec`, `cpu_user_usec`, `cpu_system_usec` | cgroup `cpu.stat` at exit |
| `peak_anon_bytes` | max sampled `memory.stat anon` — **M_peak** in the paper |
| `peak_file_bytes` | max sampled `memory.stat file` (page cache) |
| `peak_current_bytes` | max sampled `memory.current` |
| `memory_peak_bytes` | cgroup `memory.peak` at exit (includes page cache) |
| `samples` | number of 10 Hz samples collected |
| `rows_read`, `rows_kept`, `groups` | from `summary.json` |
