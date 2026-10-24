#!/usr/bin/env python3
"""Verify that pipeline outputs are bit-identical, and that inputs are encoded as specified.

Usage:
    verify.py OUTPUT.parquet OUTPUT.parquet [...] [--summary S.json ...] [--input-dir DIR]

All outputs are compared value-by-value against the first one; doubles are
compared by their IEEE-754 bit pattern. File bytes are not compared because
the writers stamp different `created_by` metadata.
"""

from __future__ import annotations

import argparse
import json
import struct
import sys
from pathlib import Path

import pyarrow.parquet as pq

OUTPUT_COLUMNS = ["hour_bucket", "count", "sum_metric", "min_metric", "max_metric", "checksum"]
SUMMARY_KEYS = ["rows_read", "rows_kept", "groups", "files", "units"]
INPUT_COLUMNS = ["id", "ts", "payload", "metric"]


def bits(v):
    """Comparison key: doubles by IEEE-754 bit pattern (distinguishes -0.0, NaN payloads)."""
    return struct.unpack("<q", struct.pack("<d", v))[0] if isinstance(v, float) else v


def show(v) -> str:
    return f"{v!r} ({v.hex()})" if isinstance(v, float) else repr(v)


def load_output(path: Path) -> dict[str, list]:
    table = pq.read_table(path)
    if table.column_names != OUTPUT_COLUMNS:
        raise SystemExit(f"FAIL {path}: columns {table.column_names} != {OUTPUT_COLUMNS}")
    return {c: table.column(c).to_pylist() for c in OUTPUT_COLUMNS}


def compare_outputs(paths: list[Path]) -> list[str]:
    errors = []
    ref_path, ref = paths[0], load_output(paths[0])
    hours = ref["hour_bucket"]
    if hours != sorted(hours) or len(set(hours)) != len(hours):
        errors.append(f"{ref_path}: hour_bucket not strictly ascending")
    for path in paths[1:]:
        other = load_output(path)
        for col in OUTPUT_COLUMNS:
            a, b = ref[col], other[col]
            if len(a) != len(b):
                errors.append(f"{path}: {col} has {len(b)} rows, {ref_path} has {len(a)}")
                continue
            diffs = [i for i, (x, y) in enumerate(zip(a, b)) if bits(x) != bits(y)]
            if diffs:
                i = diffs[0]
                errors.append(
                    f"{path}: {col} differs from {ref_path} in {len(diffs)} rows "
                    f"(first at row {i}: {show(b[i])} vs {show(a[i])})"
                )
    return errors


def compare_summaries(paths: list[Path]) -> list[str]:
    errors = []
    summaries = [(p, json.loads(p.read_text())) for p in paths]
    ref_path, ref = summaries[0]
    for path, s in summaries[1:]:
        for key in SUMMARY_KEYS:
            if s.get(key) != ref.get(key):
                errors.append(f"{path}: {key}={s.get(key)} but {ref_path} has {ref.get(key)}")
    return errors


def check_inputs(directory: Path) -> list[str]:
    errors = []
    files = sorted(directory.glob("*.parquet"))
    if not files:
        return [f"{directory}: no .parquet files"]
    for path in files:
        meta = pq.ParquetFile(path).metadata
        if meta.schema.names != INPUT_COLUMNS:
            errors.append(f"{path}: schema {meta.schema.names}")
        kv = {k.decode(): v.decode() for k, v in (meta.metadata or {}).items()}
        for key, want in [("rvj.compression", "UNCOMPRESSED"), ("rvj.dictionary", "disabled")]:
            if kv.get(key) != want:
                errors.append(f"{path}: metadata {key}={kv.get(key)!r}, want {want!r}")
        for rg in range(meta.num_row_groups):
            for c in range(meta.num_columns):
                col = meta.row_group(rg).column(c)
                if col.compression != "UNCOMPRESSED":
                    errors.append(f"{path}: rg {rg} {col.path_in_schema} compression {col.compression}")
                if col.has_dictionary_page or set(col.encodings) - {"PLAIN", "RLE", "BIT_PACKED"}:
                    errors.append(f"{path}: rg {rg} {col.path_in_schema} encodings {col.encodings}")
    return errors


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("outputs", nargs="+", type=Path)
    ap.add_argument("--summary", nargs="+", type=Path, default=[])
    ap.add_argument("--input-dir", type=Path)
    args = ap.parse_args()
    if len(args.outputs) < 2:
        ap.error("need at least two outputs to compare")

    errors = compare_outputs(args.outputs)
    if len(args.summary) >= 2:
        errors += compare_summaries(args.summary)
    if args.input_dir:
        errors += check_inputs(args.input_dir)

    for e in errors:
        print(f"FAIL {e}", file=sys.stderr)
    if errors:
        return 1
    rows = len(load_output(args.outputs[0])["hour_bucket"])
    print(f"OK: {len(args.outputs)} outputs bit-identical ({rows} groups)"
          + (f"; {len(args.summary)} summaries agree" if len(args.summary) >= 2 else "")
          + ("; inputs UNCOMPRESSED/PLAIN/no-dictionary" if args.input_dir else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
