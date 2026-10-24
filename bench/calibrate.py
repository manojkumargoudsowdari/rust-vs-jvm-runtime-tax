#!/usr/bin/env python3
"""Calibrate run time before committing to the long experiments.

Runs ONE rust-system and ONE production-style scala-g1 (-Xms3G) run on the
small preset, under the same host gate, cache dropping and container limits as
the real harness, then prints projected totals for:

  (a) make bench           on the small preset
  (b) make heapsweep       on the small preset
  (c) make bench           on the full preset
  (d) make heapsweep-validate on the full preset

Projections are linear extrapolations from these two runs (see the printed
assumptions). Nothing long is started.

Example:
    python3 bench/calibrate.py --data data/small
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path

import harness as h

FULL_ROWS = 950_000_000


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True, type=Path, help="small-preset dataset directory")
    ap.add_argument("--full-rows", type=int, default=FULL_ROWS, help="rows in the full preset (default 950M)")
    ap.add_argument("--config-file", type=Path, default=h.REPO / "bench" / "configs.yaml")
    ap.add_argument("--cpuset", help="explicit --cpuset-cpus list (default: docker/pick-cores.sh)")
    ap.add_argument("--results", type=Path, default=h.REPO / "results")
    ap.add_argument("--dry-run", action="store_true", help="plumbing test on any host (projections meaningless)")
    return ap.parse_args()


def hours(seconds: float) -> str:
    return f"{seconds / 3600:7.2f} h  ({seconds / 60:8.1f} min)"


def main() -> int:
    args = parse_args()
    cfg = h.load_yaml(args.config_file)
    drop = h.check_host(args.dry_run, cfg["protocol"]["drop_caches"])
    for ref in cfg["images"].values():
        h.docker("image", "inspect", ref)
    data = h.dataset_info(args.data)
    small_rows = (data["datagen"] or {}).get("rows")
    if not small_rows:
        sys.exit(f"{args.data}/datagen.json missing: cannot scale to the full preset")
    cpuset, cpuset_source = h.pick_cpuset(args.cpuset, cfg["container"]["cores"], args.dry_run)

    stamp = h.utc_stamp()
    out = args.results / (stamp + "-calibrate" + ("-dryrun" if args.dry_run else ""))
    (out / "runs").mkdir(parents=True)
    manifest = h.base_manifest(cfg, args.dry_run, stamp, cpuset, cpuset_source, drop, data)
    manifest["experiment"] = "calibrate"

    measured = {}
    for name in ("rust-system", "scala-g1"):
        conf = cfg["configs"][name]
        cmd = h.rust_command(cfg, conf) if not h.is_jvm(conf) else \
            h.jvm_command(cfg, cfg["jvm_common"], conf["jvm_flags"])
        h.log(f"calibration run: {name}")
        t0 = time.monotonic()
        row = h.execute(cfg, image=cfg["images"][conf["image"]], command=cmd, run_dir=out / "runs" / name,
                        data=Path(data["path"]), cpuset=cpuset, drop=drop, dry_run=args.dry_run)
        t_exec = time.monotonic() - t0
        t1 = time.monotonic()
        if h.is_jvm(conf):
            row.update(h.parse_jfr(cfg, out / "runs" / name))
        t_jfr = time.monotonic() - t1
        h.log("    " + h.status_line(row))
        if row["exit_code"] != 0:
            sys.exit(f"calibration run {name} failed; fix this before any long experiment")
        wall = (row["wall_ns"] or row["host_wall_ns"]) / 1e9
        measured[name] = {"wall_s": wall, "per_run_s": t_exec + t_jfr, "overhead_s": max(0.0, t_exec + t_jfr - wall),
                          "cpu_s": (row["cpu_usage_usec"] or 0) / 1e6}

    # ---- projections -------------------------------------------------------
    p, hs = cfg["protocol"], cfg["heapsweep"]
    rust_cfgs = [n for n, c in cfg["configs"].items() if not h.is_jvm(c)]
    jvm_cfgs = [n for n, c in cfg["configs"].items() if h.is_jvm(c)]
    R, J = measured["rust-system"]["per_run_s"], measured["scala-g1"]["per_run_s"]
    scale = args.full_rows / small_rows

    def full(name: str) -> float:
        m = measured[name]
        return m["overhead_s"] + m["wall_s"] * scale  # processing (incl. JVM startup) scaled linearly

    Rf, Jf = full("rust-system"), full("scala-g1")
    n, warm, nmt = p["runs"], p["jvm_warmup_runs"], p["nmt_runs"]

    bench_small = len(rust_cfgs) * n * R + len(jvm_cfgs) * (n + warm + nmt) * J
    s = hs["search"]
    bisect_steps = math.ceil(math.log2(max(2, (s["high_mib"] - s["low_mib"]) / s["granularity_mib"])))
    search_runs = (2 + bisect_steps) * s["attempts"]          # upper, lower, bisection; every attempt run
    collectors = len(hs["collectors"])
    sweep_runs = len(hs["multiples"]) * (hs["runs"] + hs["warmup_runs"])
    heapsweep_small = (collectors * (1 + search_runs + sweep_runs) * J      # 1 = NMT reserve run
                       + len(hs["references"]) * hs["runs"] * R)
    bench_full = len(rust_cfgs) * n * Rf + len(jvm_cfgs) * ((n + nmt) * Jf + warm * J)  # warm-ups on small
    validate_full = collectors * 2 * s["attempts"] * Jf

    proj = {
        "bench_small_s": bench_small, "heapsweep_small_s": heapsweep_small,
        "bench_full_s": bench_full, "heapsweep_validate_full_s": validate_full,
        "inputs": {"per_run_rust_small_s": R, "per_run_jvm_small_s": J, "per_run_rust_full_s": Rf,
                   "per_run_jvm_full_s": Jf, "row_scale": scale, "search_runs_per_collector": search_runs,
                   "sweep_runs_per_collector": sweep_runs},
    }
    manifest.update({"calibration": measured, "projection": proj, "finished_utc": h.utc_stamp()})
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    (out / "calibration.json").write_text(json.dumps({"measured": measured, "projection": proj}, indent=2))

    print()
    if args.dry_run:
        print("DRY RUN: this host is not a valid benchmark host; the numbers below are meaningless.")
    print("Measured on the small preset (one run each; per-run time includes cache drop, container")
    print("lifecycle and, for the JVM, JFR parsing):")
    for name, m in measured.items():
        print(f"  {name:12s} wall {m['wall_s']:8.2f} s   per run {m['per_run_s']:8.2f} s   cpu {m['cpu_s']:8.2f} s")
    print(f"\nProjected totals (row scale small -> full = {scale:g}x):")
    print(f"  (a) make bench            small preset   {hours(bench_small)}"
          f"   [{len(rust_cfgs)}x{n} Rust + {len(jvm_cfgs)}x({n}+{warm} warm-up+{nmt} NMT) JVM runs]")
    print(f"  (b) make heapsweep        small preset   {hours(heapsweep_small)}"
          f"   [{collectors} collectors x (1 NMT + <={search_runs} probe + {sweep_runs} sweep) "
          f"+ {len(hs['references'])}x{hs['runs']} Rust]")
    print(f"  (c) make bench            full preset    {hours(bench_full)}"
          f"   [per run: Rust {Rf / 60:.1f} min, JVM {Jf / 60:.1f} min]")
    print(f"  (d) make heapsweep-validate full preset  {hours(validate_full)}"
          f"   [{collectors} collectors x 2 heaps x {s['attempts']} attempts]")
    print("\nAssumptions: every JVM run (any GC, any heap) takes as long as scala-g1 at 3 GiB, and")
    print("rust-mimalloc as long as rust-system; full-preset time scales linearly with rows;")
    print("(b) counts every probe attempt and no skipped sweep points, so it is an upper bound on")
    print("run count, but runs near H_min are usually slower (more GC) and a timed-out probe costs")
    print("up to its timeout. Dataset generation time is not included.")
    print(f"\nWritten to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
