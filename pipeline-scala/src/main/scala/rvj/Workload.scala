package rvj

import java.util.Locale
import scala.collection.mutable.LongMap

// The benchmark workload. Must stay semantically identical to
// pipeline-rust/src/workload.rs; see PLAN.md §3.

/** One input row, fully materialized (the payload is a decoded String). */
final case class Row(id: Array[Byte], ts: Long, payload: String, metric: Double)

/** A filtered and transformed row. */
final case class Enriched(id: Array[Byte], hourBucket: Long, metric: Double, payloadHash: Long, tag: String):
  /** Per-row checksum; folds in `tag` and `id` so no part of the transform is dead code. */
  def rowHash: Long =
    payloadHash ^ Workload.fnv1a64(tag) ^ Workload.leLong(id, 0) ^ Workload.leLong(id, 8)

final class Agg:
  var count: Long = 0L
  var sum: Double = 0.0
  var min: Double = Double.PositiveInfinity
  var max: Double = Double.NegativeInfinity
  var checksum: Long = 0L

  def add(e: Enriched): Unit =
    count += 1
    sum += e.metric
    if e.metric < min then min = e.metric
    if e.metric > max then max = e.metric
    checksum ^= e.rowHash

  /** Merging must happen in unit order for `sum` to be reproducible. */
  def merge(o: Agg): Unit =
    count += o.count
    sum += o.sum
    if o.min < min then min = o.min
    if o.max > max then max = o.max
    checksum ^= o.checksum

object Workload:
  final val MetricThreshold = 75.0
  final val HourMs = 3_600_000L
  final val TagLen = 8

  private final val FnvOffset = 0xcbf29ce484222325L
  private final val FnvPrime = 0x100000001b3L

  /** FNV-1a 64 over the string's bytes. Inputs are ASCII, so chars == UTF-8 bytes. */
  def fnv1a64(s: String): Long =
    var h = FnvOffset
    var i = 0
    while i < s.length do
      h ^= s.charAt(i).toLong
      h *= FnvPrime
      i += 1
    h

  def leLong(b: Array[Byte], off: Int): Long =
    var v = 0L
    var i = 7
    while i >= 0 do
      v = (v << 8) | (b(off + i) & 0xffL)
      i -= 1
    v

  def enrich(r: Row): Enriched =
    Enriched(
      id = r.id,
      hourBucket = r.ts / HourMs,
      metric = r.metric,
      payloadHash = fnv1a64(r.payload),
      tag = r.payload.substring(0, TagLen).toUpperCase(Locale.ROOT)
    )

  /** Filter → transform → aggregate the first `n` rows of a batch. Returns rows kept. */
  def processBatch(rows: Array[Row], n: Int, groups: LongMap[Agg]): Long =
    var kept = 0L
    rows.iterator
      .take(n)
      .filter(_.metric > MetricThreshold)
      .map(enrich)
      .foreach { e =>
        groups.getOrElseUpdate(e.hourBucket, Agg()).add(e)
        kept += 1
      }
    kept
