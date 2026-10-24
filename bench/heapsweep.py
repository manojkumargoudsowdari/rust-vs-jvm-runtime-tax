#!/usr/bin/env python3
"""Heap-size sweep (Hertz & Berger, OOPSLA 2005 methodology).

Phase 1 - H_min search. For each JVM collector, binary-search the smallest
-Xmx (granularity 128 MiB) at which the dataset completes. A probe passes only
if every attempt (default 3) exits 0; a Java OutOfMemoryError, a cgroup OOM
kill, any other failure, or a timeout fails the probe.

Phase 2 - sweep. Each collector runs at multiples of its own H_min
(default 1.25x, 1.5x, 2x, 3x, 4x), skipping points whose heap plus an off-heap
reserve would not fit the cgroup. Warm-up runs come first; measured runs of
all points and of the Rust references are then interleaved in random order.

All JVM runs here use -Xms = -Xmx and NO -XX:+AlwaysPreTouch. Same host gate
and --dry-run semantics as bench/run.py.

Example:
    python3 bench/heapsweep.py --data data/small
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from pathlib import Path

import harness as h

SWEEP_COLUMNS = h.RUN_COLUMNS + ["heap_mib", "multiple", "attempt"]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True, type=Path, help="dataset directory (default study: small preset)")
    ap.add_argument("--config-file", type=Path, default=h.REPO / "bench" / "configs.yaml")
    ap.add_argument("--collectors", help="comma-separated subset of heapsweep.collectors")
    ap.add_argument("--hmin", help="skip the search for some collectors, e.g. scala-g1=640,scala-zgc=896 (MiB)")
    ap.add_argument("--runs", type=int, help="measured runs per point (overrides configs.yaml)")
    ap.add_argument("--warmups", type=int, help="warm-up runs per JVM point (overrides configs.yaml)")
    ap.add_argument("--attempts", type=int, help="attempts per H_min probe (overrides configs.yaml)")
    ap.add_argument("--multiples", help="comma-separated heap multiples (overrides configs.yaml)")
    ap.add_argument("--cpuset", help="explicit --cpuset-cpus list (default: docker/pick-cores.sh)")
    ap.add_argument("--results", type=Path, default=h.REPO / "results")
    ap.add_argument("--seed", type=int, help="seed for the randomized run order (default: random, recorded)")
    ap.add_argument("--dry-run", action="store_true",
                    help="plumbing test on any host; skips host checks; results are marked DRY RUN")
    return ap.parse_args()


def heap_flags(mib: int) -> list[str]:
    return [f"-Xms{mib}m", f"-Xmx{mib}m"]


class Sweep:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        cfg = h.load_yaml(args.config_file)
        hs = cfg["heapsweep"]
        if args.collectors:
            hs["collectors"] = [c for c in hs["collectors"] if c in args.collectors.split(",")]
        for key, val in (("runs", args.runs), ("warmup_runs", args.warmups)):
            if val is not None:
                hs[key] = val
        if args.attempts is not None:
            hs["search"]["attempts"] = args.attempts
        if args.multiples:
            hs["multiples"] = [float(m) for m in args.multiples.split(",")]
        self.cfg, self.hs = cfg, hs
        self.rows: list[dict] = []
        self.seq = 0

    # ------------------------------------------------------------------ plumbing

    def setup(self) -> None:
        a, cfg = self.args, self.cfg
        self.drop = h.check_host(a.dry_run, cfg["protocol"]["drop_caches"])
        for ref in cfg["images"].values():
            h.docker("image", "inspect", ref)
        self.data = h.dataset_info(a.data)
        self.preset = (self.data["datagen"] or {}).get("preset", "custom")
        self.cpuset, cpuset_source = h.pick_cpuset(a.cpuset, cfg["container"]["cores"], a.dry_run)
        self.seed = a.seed if a.seed is not None else random.SystemRandom().randrange(2**32)
        stamp = h.utc_stamp()
        self.out = a.results / (stamp + "-heapsweep" + ("-dryrun" if a.dry_run else ""))
        self.runs_dir = self.out / "runs"
        self.runs_dir.mkdir(parents=True)
        self.manifest = h.base_manifest(cfg, a.dry_run, stamp, self.cpuset, cpuset_source, self.drop, self.data)
        self.manifest.update({"experiment": "heapsweep", "schedule_seed": self.seed})
        self.save_manifest()
        h.log(f"heap sweep -> {self.out}" + ("  [DRY RUN]" if a.dry_run else ""))

    def save_manifest(self) -> None:
        (self.out / "manifest.json").write_text(json.dumps(self.manifest, indent=2))

    def run(self, name: str, kind: str, command: list[str], image: str, *, heap_mib: int | None = None,
            multiple: float | None = None, attempt: int | None = None, timeout_s: float | None = None) -> dict:
        tag = f"{heap_mib}m" if heap_mib else "ref"
        suffix = f"-a{attempt}" if attempt is not None else ""
        run_dir = self.runs_dir / f"{self.seq:04d}-{name}-{kind}-{tag}{suffix}"
        row = h.execute(self.cfg, image=image, command=command, run_dir=run_dir, data=Path(self.data["path"]),
                        cpuset=self.cpuset, drop=self.drop, dry_run=self.args.dry_run, timeout_s=timeout_s)
        row.update({"config": name, "kind": kind, "order": self.seq, "preset": self.preset,
                    "heap_mib": heap_mib, "multiple": multiple, "attempt": attempt})
        self.seq += 1
        self.rows.append(row)
        h.write_csv(self.out / "runs.csv", self.rows, SWEEP_COLUMNS)
        h.log(f"    {name} {kind} {tag}" + (f" #{attempt}" if attempt is not None else "") + ": " + h.status_line(row))
        return row

    def gc_flags(self, collector: str) -> list[str]:
        return self.cfg["configs"][collector]["jvm_flags"]

    # ------------------------------------------------------------------ phase 1

    def probe(self, collector: str, mib: int, timeout_s: float | None) -> tuple[bool, list[dict]]:
        attempts = []
        for a in range(self.hs["search"]["attempts"]):
            cmd = h.jvm_command(self.cfg, heap_flags(mib), self.gc_flags(collector), jfr=False)
            row = self.run(collector, "probe", cmd, self.cfg["images"]["scala"], heap_mib=mib, attempt=a,
                           timeout_s=timeout_s)
            attempts.append(row)
            if row["exit_code"] != 0 or row["timed_out"]:
                return False, attempts  # one failure fails the probe; skip remaining attempts
        return True, attempts

    def search(self, collector: str) -> dict:
        s = self.hs["search"]
        g = s["granularity_mib"]
        lo, hi = s["low_mib"], s["high_mib"] // g * g
        record = {"probes": [], "granularity_mib": g, "attempts": s["attempts"]}

        def note(mib: int, passed: bool, rows: list[dict]) -> None:
            record["probes"].append({
                "heap_mib": mib, "passed": passed, "attempts_run": len(rows),
                "failures": [{"run_id": r["run_id"], "exit_code": r["exit_code"], "java_oom": r["java_oom"],
                              "cgroup_oom": r["oom_killed"], "timed_out": r["timed_out"]}
                             for r in rows if r["exit_code"] != 0 or r["timed_out"]],
            })

        h.log(f"[search] {collector}: upper bound {hi} MiB")
        passed, rows = self.probe(collector, hi, None)
        note(hi, passed, rows)
        if not passed:
            record.update({"h_min_mib": None, "error": f"upper bound {hi} MiB failed"})
            return record
        baseline = max(r["wall_ns"] or r["host_wall_ns"] for r in rows) / 1e9
        timeout = max(s["timeout_min_s"], s["timeout_factor"] * baseline)
        record.update({"baseline_wall_s": baseline, "timeout_s": timeout})

        h.log(f"[search] {collector}: lower bound {lo} MiB")
        passed, rows = self.probe(collector, lo, timeout)
        note(lo, passed, rows)
        if passed:
            record["h_min_mib"] = lo
            return record
        while hi - lo > g:  # invariant: lo fails, hi passes
            mid = lo + max(g, (hi - lo) // 2 // g * g)
            h.log(f"[search] {collector}: probe {mid} MiB (fail {lo}, pass {hi})")
            passed, rows = self.probe(collector, mid, timeout)
            note(mid, passed, rows)
            if passed:
                hi = mid
            else:
                lo = mid
        record["h_min_mib"] = hi
        return record

    # ------------------------------------------------------------------ phase 2

    def points(self, hmin: dict[str, int]) -> tuple[list[dict], list[dict]]:
        cap = self.hs["cgroup_mib"] - self.hs["offheap_reserve_mib"]
        points, skipped = [], []
        for collector, base in hmin.items():
            for m in self.hs["multiples"]:
                mib = math.ceil(m * base)
                p = {"collector": collector, "multiple": m, "heap_mib": mib}
                (points if mib <= cap else skipped).append(p)
        return points, skipped

    def sweep(self, points: list[dict], timeouts: dict[str, float | None]) -> None:
        hs, scala = self.hs, self.cfg["images"]["scala"]
        warm = [(p, "warmup") for p in points for _ in range(hs["warmup_runs"])]
        measured = [(p, "measured") for p in points for _ in range(hs["runs"])]
        measured += [({"collector": r, "reference": True}, "measured")
                     for r in hs["references"] for _ in range(hs["runs"])]
        random.Random(self.seed).shuffle(measured)
        schedule = warm + measured
        self.manifest["sweep_schedule"] = [
            f"{p['collector']}:{p.get('heap_mib', 'ref')}:{kind}" for p, kind in schedule]
        self.save_manifest()
        for i, (p, kind) in enumerate(schedule):
            h.log(f"[sweep {i + 1}/{len(schedule)}]")
            if p.get("reference"):
                conf = self.cfg["configs"][p["collector"]]
                self.run(p["collector"], kind, h.rust_command(self.cfg, conf), self.cfg["images"][conf["image"]])
            else:
                cmd = h.jvm_command(self.cfg, heap_flags(p["heap_mib"]), self.gc_flags(p["collector"]))
                self.run(p["collector"], kind, cmd, scala, heap_mib=p["heap_mib"], multiple=p["multiple"],
                         timeout_s=timeouts.get(p["collector"]))

    # ------------------------------------------------------------------ main

    def main(self) -> int:
        self.setup()
        overrides = dict(kv.split("=") for kv in self.args.hmin.split(",")) if self.args.hmin else {}
        search, hmin, timeouts = {}, {}, {}
        for c in self.hs["collectors"]:
            if c in overrides:
                search[c] = {"h_min_mib": int(overrides[c]), "source": "--hmin override"}
            else:
                search[c] = self.search(c)
            if search[c].get("h_min_mib"):
                hmin[c] = search[c]["h_min_mib"]
            timeouts[c] = search[c].get("timeout_s")
            h.log(f"[search] {c}: H_min = {search[c].get('h_min_mib')} MiB")
            self.manifest["hmin_search"] = search
            self.save_manifest()
        (self.out / "hmin.json").write_text(json.dumps(search, indent=2))

        points, skipped = self.points(hmin)
        self.manifest.update({"sweep_points": points, "skipped_points": skipped,
                              "heap_cap_mib": self.hs["cgroup_mib"] - self.hs["offheap_reserve_mib"]})
        for p in skipped:
            h.log(f"[sweep] skip {p['collector']} {p['multiple']}x = {p['heap_mib']} MiB (exceeds cap)")
        self.sweep(points, timeouts)

        h.log("parsing JFR recordings")
        for row in self.rows:
            if row["kind"] != "probe" and h.is_jvm(self.cfg["configs"][row["config"]]):
                row.update(h.parse_jfr(self.cfg, self.runs_dir / row["run_id"]))
        h.write_csv(self.out / "runs.csv", self.rows, SWEEP_COLUMNS)

        sweep_rows = [r for r in self.rows if r["kind"] != "probe"]
        self.manifest["finished_utc"] = h.utc_stamp()
        self.manifest["output_check"] = h.check_outputs(self.rows, self.runs_dir)
        self.manifest["failed_sweep_runs"] = [r["run_id"] for r in sweep_rows if r["exit_code"] != 0]
        self.save_manifest()
        check = self.manifest["output_check"]
        h.log(f"H_min: {hmin}")
        h.log(f"outputs compared: {check['compared']}, bit-identical: {check['identical']}")
        h.log(f"failed sweep runs: {len(self.manifest['failed_sweep_runs'])}; results in {self.out}")
        return 0 if not self.manifest["failed_sweep_runs"] and check["identical"] is not False else 1


if __name__ == "__main__":
    sys.exit(Sweep(parse_args()).main())
