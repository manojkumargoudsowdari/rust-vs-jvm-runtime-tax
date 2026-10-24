package rvj

import scala.collection.mutable.LongMap

// Mirrors the unit tests in pipeline-rust/src/workload.rs.
class WorkloadSuite extends munit.FunSuite:

  test("fnv1a64 reference vectors") {
    assertEquals(Workload.fnv1a64(""), 0xcbf29ce484222325L)
    assertEquals(Workload.fnv1a64("a"), 0xaf63dc4c8601ec8cL)
    assertEquals(Workload.fnv1a64("foobar"), 0x85944171f73967e8L)
  }

  private def row(ts: Long, metric: Double, payload: String): Row =
    val id = new Array[Byte](16)
    id(0) = 1
    id(15) = 2
    Row(id, ts, payload * (1024 / payload.length), metric)

  test("filters, buckets and aggregates") {
    val rows = Array(
      row(0, 80.0, "abcdefgh"),
      row(Workload.HourMs - 1, 90.0, "abcdefgh"),
      row(Workload.HourMs, 100.0, "zyxwvuts"),
      row(Workload.HourMs, 75.0, "zyxwvuts") // not > threshold
    )
    val groups = LongMap.empty[Agg]
    assertEquals(Workload.processBatch(rows, rows.length, groups), 3L)
    assertEquals(groups.size, 2)
    val g0 = groups(0)
    assertEquals((g0.count, g0.sum, g0.min, g0.max), (2L, 170.0, 80.0, 90.0))
    // Two identical row hashes cancel out under XOR.
    assertEquals(g0.checksum, 0L)
    assertEquals(groups(1).count, 1L)
  }

  test("tag is uppercased prefix") {
    assertEquals(Workload.enrich(row(0, 80.0, "abcDEfg-")).tag, "ABCDEFG-")
  }

  test("leLong matches little-endian layout") {
    val b = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8)
    assertEquals(Workload.leLong(b, 0), 0x0807060504030201L)
  }
