#!/usr/bin/env python3
"""Run the benchmark matrix (bench/configs.yaml) and record raw results.

Timed runs are only recorded on a bare-metal Linux host with cgroup v2; use
--dry-run anywhere else to exercise the plumbing (results are marked as dry
runs and analysis refuses to publish them). See PLAN.md §7 and
results/README.md for the output layout.

Example:
    sudo -E python3 bench/run.py --data data/small --warmup-data data/small
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import platform
import random
import re
import subprocess
import sys
import time
from pathlib import Path

import yaml

import hostinfo
import jfrparse
import verify
from sampler import CgroupSampler, container_cgroup

REPO = Path(__file__).resolve().parent.parent
JAR = "/opt/rvj/pipeline-scala.jar"
JFC = "/opt/rvj/gc.jfc"

RUN_COLUMNS = [
    "run_id", "config", "kind", "order", "preset", "dry_run", "exit_code", "oom_killed",
    "wall_ns", "host_wall_ns", "pipeline_wall_ns",
    "cpu_usage_usec", "cpu_user_usec", "cpu_system_usec", "cpu_throttled_usec",
    "peak_private_bytes", "peak_anon_bytes", "peak_shmem_bytes", "peak_pagecache_bytes", "peak_file_bytes",
    "peak_kernel_bytes", "peak_current_bytes", "memory_peak_bytes", "samples",
    "rows_read", "rows_kept", "groups",
    "gc_count", "gc_pause_count", "gc_pause_total_ns", "gc_pause_max_ns", "z_alloc_stall_total_ns",
    "heap_used_max", "heap_committed_max", "nmt_total_committed_bytes", "nmt_heap_committed_bytes",
]


def log(msg: str) -> None:
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def docker(*args: str, check: bool = True) -> str:
    res = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check and res.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args[:2])} failed: {res.stderr.strip()}")
    return res.stdout.strip()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True, type=Path, help="dataset directory (datagen output) on this host")
    ap.add_argument("--warmup-data", type=Path, help="dataset for JVM warm-up runs (default: --data)")
    ap.add_argument("--config-file", type=Path, default=REPO / "bench" / "configs.yaml")
    ap.add_argument("--configs", help="comma-separated subset of configs to run")
    ap.add_argument("--runs", type=int, help="measured runs per config (overrides configs.yaml)")
    ap.add_argument("--warmups", type=int, help="JVM warm-up runs per JVM config (overrides configs.yaml)")
    ap.add_argument("--nmt-runs", type=int, help="NMT runs per JVM config (overrides configs.yaml)")
    ap.add_argument("--cpuset", help="explicit --cpuset-cpus list (default: docker/pick-cores.sh)")
    ap.add_argument("--results", type=Path, default=REPO / "results")
    ap.add_argument("--seed", type=int, help="seed for the randomized run order (default: random, recorded)")
    ap.add_argument("--dry-run", action="store_true",
                    help="plumbing test on any host; skips host checks; results are marked DRY RUN")
    return ap.parse_args()


# --------------------------------------------------------------------------- setup

def load_config(args: argparse.Namespace) -> dict:
    cfg = yaml.safe_load(args.config_file.read_text())
    proto = cfg["protocol"]
    for key, override in (("runs", args.runs), ("jvm_warmup_runs", args.warmups), ("nmt_runs", args.nmt_runs)):
        if override is not None:
            proto[key] = override
    if args.configs:
        wanted = args.configs.split(",")
        unknown = set(wanted) - set(cfg["configs"])
        if unknown:
            sys.exit(f"unknown configs: {', '.join(sorted(unknown))}")
        cfg["configs"] = {k: v for k, v in cfg["configs"].items() if k in wanted}
    return cfg


def is_jvm(conf: dict) -> bool:
    return conf["image"] == "scala"


def check_host(args: argparse.Namespace, cfg: dict) -> bool:
    """Return whether caches can be dropped; exit if timings may not be recorded."""
    blockers = hostinfo.timing_blockers()
    can_drop = hostinfo.can_drop_caches()
    if cfg["protocol"]["drop_caches"] and not can_drop:
        blockers.append("cannot drop the page cache: run as root or allow passwordless sudo")
    if blockers and not args.dry_run:
        for b in blockers:
            print(f"refusing to record timings: {b}", file=sys.stderr)
        print("(pass --dry-run to exercise the harness without recording valid timings)", file=sys.stderr)
        sys.exit(2)
    for b in blockers:
        log(f"DRY RUN: ignoring host check: {b}")
    return can_drop and cfg["protocol"]["drop_caches"]


def pick_cpuset(args: argparse.Namespace, cfg: dict) -> tuple[str | None, str]:
    if args.cpuset:
        return args.cpuset, "--cpuset"
    if platform.system() != "Linux":
        return None, "none (non-Linux dry run)"
    out = subprocess.run(["bash", str(REPO / "docker" / "pick-cores.sh"), str(cfg["container"]["cores"])],
                         capture_output=True, text=True)
    if out.returncode != 0:
        if args.dry_run:
            return None, f"none (pick-cores failed: {out.stderr.strip()})"
        sys.exit(f"pick-cores.sh failed: {out.stderr.strip()}")
    return out.stdout.strip(), "docker/pick-cores.sh"


def dataset_info(path: Path) -> dict:
    files = sorted(path.glob("*.parquet"))
    if not files:
        sys.exit(f"no .parquet files in {path}")
    desc = path / "datagen.json"
    return {
        "path": str(path),
        "datagen": json.loads(desc.read_text()) if desc.exists() else None,
        "files": len(files),
        "bytes": sum(f.stat().st_size for f in files),
    }


def build_schedule(cfg: dict, seed: int, have_warmup: bool) -> list[tuple[str, str]]:
    proto, configs = cfg["protocol"], cfg["configs"]
    jvm = [name for name, c in configs.items() if is_jvm(c)]
    schedule = [(name, "warmup") for name in jvm for _ in range(proto["jvm_warmup_runs"] if have_warmup else 0)]
    measured = [(name, "measured") for name in configs for _ in range(proto["runs"])]
    random.Random(seed).shuffle(measured)
    schedule += measured
    schedule += [(name, "nmt") for name in jvm for _ in range(proto["nmt_runs"])]
    return schedule


# --------------------------------------------------------------------------- one run

def container_command(cfg: dict, conf: dict, kind: str) -> list[str]:
    p = cfg["pipeline"]
    pipe_args = ["--input", "/data", "--output", "/out/output.parquet", "--summary", "/out/summary.json",
                 "--batch-log", "/out/batches.csv", "--workers", str(p["workers"]), "--batch-size", str(p["batch_size"])]
    if not is_jvm(conf):
        return [*conf["command"], *pipe_args]
    flags = [*cfg["jvm_common"], *conf["jvm_flags"],
             f"-XX:StartFlightRecording=filename=/out/gc.jfr,settings={JFC},dumponexit=true",
             "-Xlog:gc*:file=/out/gc.log:uptime,level,tags"]
    if kind == "nmt":
        flags += ["-XX:NativeMemoryTracking=summary", "-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintNMTStatistics"]
    return ["java", *flags, "-jar", JAR, *pipe_args]


def read_kv(path: Path) -> dict[str, int]:
    if not path.exists():
        return {}
    return {k: int(v) for k, _, v in (l.partition(" ") for l in path.read_text().splitlines()) if v.isdigit()}


def read_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def parse_nmt(stdout: Path, run_dir: Path) -> dict:
    text = stdout.read_text() if stdout.exists() else ""
    start = text.find("Native Memory Tracking:")
    if start < 0:
        return {}
    block = text[start:]
    (run_dir / "nmt.txt").write_text(block)
    # JDK 21 prints bytes at exit (no unit); other scales print a KB/MB/GB suffix.
    units = {"": 1, "B": 1, "KB": 2**10, "MB": 2**20, "GB": 2**30}

    def committed(pattern: str) -> int | None:
        m = re.search(pattern + r"reserved=\d+(?:[KMG]?B)?, committed=(\d+)([KMG]?B)?", block)
        return int(m.group(1)) * units[m.group(2) or ""] if m else None

    return {"nmt_total_committed_bytes": committed(r"Total: "),
            "nmt_heap_committed_bytes": committed(r"Java Heap \(")}


def execute_run(cfg: dict, name: str, kind: str, order: int, run_dir: Path, data: Path,
                cpuset: str | None, drop: bool, dry_run: bool, preset: str) -> dict:
    conf = cfg["configs"][name]
    c = cfg["container"]
    run_dir.mkdir(parents=True)
    os.chmod(run_dir, 0o777)  # the container may run as a different uid

    create = ["create", "--name", f"rvj-{run_dir.name}", "--network", "none",
              "--cpus", str(c["cpus"]), "--memory", c["memory"], "--memory-swap", c["memory_swap"],
              "-v", f"{data.resolve()}:/data:ro", "-v", f"{run_dir.resolve()}:/out"]
    if cpuset:
        create += ["--cpuset-cpus", cpuset]
    if hasattr(os, "getuid"):
        create += ["--user", f"{os.getuid()}:{os.getgid()}"]
    create += [cfg["images"][conf["image"]], *container_command(cfg, conf, kind)]

    if drop:
        hostinfo.drop_caches()
    cid = docker(*create)
    try:
        t0 = time.monotonic_ns()
        docker("start", cid)
        smp = None
        pid = int(docker("inspect", "--format", "{{.State.Pid}}", cid) or 0)
        cgroup = container_cgroup(pid, cid) if pid else None
        if cgroup:
            smp = CgroupSampler(cgroup, cfg["protocol"]["sample_hz"], t0)
            smp.start()
        elif not dry_run:
            log(f"WARNING: could not locate cgroup for {name}; no samples for this run")
        exit_code = int(docker("wait", cid))
        host_wall_ns = time.monotonic_ns() - t0
        if smp:
            smp.stop()
            smp.write_csv(run_dir / "cgroup_samples.csv")
        oom = docker("inspect", "--format", "{{.State.OOMKilled}}", cid) == "true"
    finally:
        docker("rm", "-f", cid, check=False)

    wrapper = read_json(run_dir / "wrapper.json")
    summary = read_json(run_dir / "summary.json")
    cpu = read_kv(run_dir / "cgroup_final" / "cpu.stat")
    mem_peak = (run_dir / "cgroup_final" / "memory.peak")
    row = {
        "run_id": run_dir.name, "config": name, "kind": kind, "order": order, "preset": preset,
        "dry_run": dry_run, "exit_code": exit_code, "oom_killed": oom,
        "wall_ns": wrapper["end_ns"] - wrapper["start_ns"] if wrapper else None,
        "host_wall_ns": host_wall_ns,
        "pipeline_wall_ns": summary.get("wall_nanos"),
        "cpu_usage_usec": cpu.get("usage_usec"), "cpu_user_usec": cpu.get("user_usec"),
        "cpu_system_usec": cpu.get("system_usec"), "cpu_throttled_usec": cpu.get("throttled_usec"),
        # M_peak: private memory = anon + shmem. ZGC backs its heap with memfd
        # shared memory, which cgroup v2 accounts as shmem (inside `file`), not anon.
        "peak_private_bytes": smp.peak_of(lambda r: r["anon"] + r["shmem"]) if smp else None,
        "peak_anon_bytes": smp.peak("anon") if smp else None,
        "peak_shmem_bytes": smp.peak("shmem") if smp else None,
        "peak_pagecache_bytes": smp.peak_of(lambda r: r["file"] - r["shmem"]) if smp else None,
        "peak_file_bytes": smp.peak("file") if smp else None,
        "peak_kernel_bytes": smp.peak("kernel") if smp else None,
        "peak_current_bytes": smp.peak("memory_current") if smp else None,
        "memory_peak_bytes": int(mem_peak.read_text().split()[0]) if mem_peak.exists() else None,
        "samples": len(smp.rows) if smp else 0,
        "rows_read": summary.get("rows_read"), "rows_kept": summary.get("rows_kept"),
        "groups": summary.get("groups"),
    }
    if kind == "nmt":
        row.update(parse_nmt(run_dir / "stdout.txt", run_dir))
    return row


# --------------------------------------------------------------------------- post-processing

def parse_jfr(cfg: dict, run_dir: Path) -> dict:
    if not (run_dir / "gc.jfr").exists():
        return {}
    out = docker("run", "--rm", "--network", "none", "--entrypoint", "jfr", "-v", f"{run_dir.resolve()}:/out",
                 cfg["images"]["scala"], "print", "--json", "--events", jfrparse.EVENTS, "/out/gc.jfr")
    (run_dir / "gc.json").write_text(out)
    totals = jfrparse.convert(run_dir / "gc.json", run_dir)
    totals.pop("jvm_version", None)
    return totals


def write_runs(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RUN_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def check_outputs(rows: list[dict], runs_dir: Path) -> dict:
    paths = [runs_dir / r["run_id"] / "output.parquet" for r in rows
             if r["kind"] != "warmup" and r["exit_code"] == 0]
    paths = [p for p in paths if p.exists()]
    if len(paths) < 2:
        return {"compared": len(paths), "identical": None, "errors": []}
    errors = verify.compare_outputs(paths)
    return {"compared": len(paths), "identical": not errors, "errors": errors[:20]}


# --------------------------------------------------------------------------- main

def main() -> int:
    args = parse_args()
    cfg = load_config(args)
    drop = check_host(args, cfg)
    for ref in cfg["images"].values():
        docker("image", "inspect", ref)  # fail fast if an image is missing

    data = dataset_info(args.data)
    warm = dataset_info(args.warmup_data) if args.warmup_data else data
    preset = (data["datagen"] or {}).get("preset", "custom")
    cpuset, cpuset_source = pick_cpuset(args, cfg)
    seed = args.seed if args.seed is not None else random.SystemRandom().randrange(2**32)
    schedule = build_schedule(cfg, seed, have_warmup=True)

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = args.results / (stamp + ("-dryrun" if args.dry_run else ""))
    runs_dir = out / "runs"
    runs_dir.mkdir(parents=True)

    manifest = {
        "schema": 1,
        "dry_run": args.dry_run,
        "started_utc": stamp,
        "argv": sys.argv,
        "git": hostinfo.git_section(REPO),
        "host": hostinfo.host_section(),
        "docker": hostinfo.docker_section(cfg["images"]),
        "cpuset": {"cpus": cpuset, "source": cpuset_source},
        "drop_caches_effective": drop,
        "dataset": data,
        "warmup_dataset": warm,
        "config": cfg,
        "commands": {name: {kind: container_command(cfg, conf, kind) for kind in ("measured", "nmt")}
                     for name, conf in cfg["configs"].items()},
        "schedule_seed": seed,
        "schedule": [f"{i:03d}-{n}-{k}" for i, (n, k) in enumerate(schedule)],
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    log(f"{len(schedule)} runs -> {out}" + ("  [DRY RUN]" if args.dry_run else ""))

    rows: list[dict] = []
    for order, (name, kind) in enumerate(schedule):
        run_dir = runs_dir / f"{order:03d}-{name}-{kind}"
        dataset = Path(warm["path"]) if kind == "warmup" else Path(data["path"])
        log(f"[{order + 1}/{len(schedule)}] {name} ({kind})")
        row = execute_run(cfg, name, kind, order, run_dir, dataset, cpuset, drop, args.dry_run, preset)
        rows.append(row)
        write_runs(out / "runs.csv", rows)
        status = "ok" if row["exit_code"] == 0 else f"exit {row['exit_code']}" + (" OOM" if row["oom_killed"] else "")
        log(f"    {status}; wall {(row['wall_ns'] or 0) / 1e9:.2f}s; cpu {(row['cpu_usage_usec'] or 0) / 1e6:.2f}s; "
            f"peak anon+shmem {(row['peak_private_bytes'] or 0) / 2**20:.0f} MiB; samples {row['samples']}")

    log("parsing JFR recordings")
    for row in rows:
        if is_jvm(cfg["configs"][row["config"]]):
            row.update(parse_jfr(cfg, runs_dir / row["run_id"]))
    write_runs(out / "runs.csv", rows)

    manifest["finished_utc"] = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest["output_check"] = check_outputs(rows, runs_dir)
    manifest["failed_runs"] = [r["run_id"] for r in rows if r["exit_code"] != 0]
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))

    check = manifest["output_check"]
    log(f"outputs compared: {check['compared']}, bit-identical: {check['identical']}")
    log(f"failed runs: {len(manifest['failed_runs'])}; results in {out}")
    return 0 if not manifest["failed_runs"] and check["identical"] is not False else 1


if __name__ == "__main__":
    sys.exit(main())
