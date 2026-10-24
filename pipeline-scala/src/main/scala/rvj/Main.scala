package rvj

import java.lang.management.ManagementFactory
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.{ArrayBuffer, LongMap}
import scala.jdk.CollectionConverters.*
import scala.util.Using

import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter}
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.{ColumnIOFactory, LocalInputFile, LocalOutputFile}
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter, RecordMaterializer}
import org.apache.parquet.schema.{MessageType, MessageTypeParser}

// Standalone JVM pipeline: stream (file, row group) units through 4 workers,
// filter/transform/aggregate per row, write hourly aggregates to Parquet.

final case class Config(
    input: Path,
    output: Path,
    workers: Int,
    batchSize: Int,
    batchLog: Option[Path],
    summary: Option[Path]
)

/** `nanos`: time to process this batch (materialize + filter/transform/aggregate).
  * `setupNanos`: unit setup before the first batch (open, footer, readRowGroup); non-zero only on batch 0.
  */
final case class BatchRecord(
    file: Int,
    rowGroup: Int,
    batch: Int,
    worker: Int,
    startNanos: Long,
    rows: Int,
    nanos: Long,
    setupNanos: Long
)

final class UnitResult(val groups: LongMap[Agg], var rowsRead: Long, var rowsKept: Long)

/** Assembles each Parquet record directly into a [[Row]] (no intermediate Group objects). */
final class RowMaterializer(schema: MessageType) extends RecordMaterializer[Row]:
  private var id: Array[Byte] = null
  private var ts: Long = 0L
  private var payload: String = null
  private var metric: Double = 0.0

  private val converters = new Array[Converter](schema.getFieldCount)
  converters(schema.getFieldIndex("id")) = new PrimitiveConverter:
    override def addBinary(v: Binary): Unit = id = v.getBytes
  converters(schema.getFieldIndex("ts")) = new PrimitiveConverter:
    override def addLong(v: Long): Unit = ts = v
  converters(schema.getFieldIndex("payload")) = new PrimitiveConverter:
    override def addBinary(v: Binary): Unit = payload = v.toStringUsingUTF8
  converters(schema.getFieldIndex("metric")) = new PrimitiveConverter:
    override def addDouble(v: Double): Unit = metric = v

  private val root = new GroupConverter:
    override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)
    override def start(): Unit = ()
    override def end(): Unit = ()

  override def getCurrentRecord: Row = Row(id, ts, payload, metric)
  override def getRootConverter: GroupConverter = root

object Main:
  private val OutputSchema = MessageTypeParser.parseMessageType(
    """message aggregates {
      |  required int64 hour_bucket;
      |  required int64 count;
      |  required double sum_metric;
      |  required double min_metric;
      |  required double max_metric;
      |  required int64 checksum;
      |}""".stripMargin
  )

  private def usage(): Nothing =
    System.err.println(
      "usage: pipeline-scala --input DIR --output FILE [--workers 4] [--batch-size 8192]\n" +
        "                      [--batch-log FILE.csv] [--summary FILE.json]"
    )
    sys.exit(2)

  private def parseArgs(args: Array[String]): Config =
    var input, output: Option[Path] = None
    var batchLog, summary: Option[Path] = None
    var workers = 4
    var batchSize = 8192
    val it = args.iterator
    def value(): String = if it.hasNext then it.next() else usage()
    while it.hasNext do
      it.next() match
        case "--input"      => input = Some(Paths.get(value()))
        case "--output"     => output = Some(Paths.get(value()))
        case "--workers"    => workers = value().toIntOption.getOrElse(usage())
        case "--batch-size" => batchSize = value().toIntOption.getOrElse(usage())
        case "--batch-log"  => batchLog = Some(Paths.get(value()))
        case "--summary"    => summary = Some(Paths.get(value()))
        case _              => usage()
    if workers <= 0 || batchSize <= 0 then usage()
    Config(input.getOrElse(usage()), output.getOrElse(usage()), workers, batchSize, batchLog, summary)

  /** Input files (`*.parquet`) sorted by file name. */
  private def listInputs(dir: Path): Vector[Path] =
    val files = Using.resource(Files.list(dir)) { s =>
      s.iterator.asScala.filter(_.getFileName.toString.endsWith(".parquet")).toVector
    }.sortBy(_.getFileName.toString)
    if files.isEmpty then throw IllegalArgumentException(s"no .parquet files in $dir")
    files

  private def openReader(path: Path): ParquetFileReader =
    ParquetFileReader.open(LocalInputFile(path), ParquetReadOptions.builder().build())

  private def runUnit(
      cfg: Config,
      path: Path,
      file: Int,
      rowGroup: Int,
      worker: Int,
      t0: Long,
      log: ArrayBuffer[BatchRecord]
  ): UnitResult =
    val unitStart = System.nanoTime()
    Using.resource(openReader(path)) { reader =>
      val schema = reader.getFooter.getFileMetaData.getSchema
      val pages = reader.readRowGroup(rowGroup)
      val records = ColumnIOFactory().getColumnIO(schema).getRecordReader(pages, RowMaterializer(schema))

      val result = UnitResult(LongMap.empty[Agg], 0L, 0L)
      val rows = new Array[Row](cfg.batchSize)
      var remaining = pages.getRowCount
      var prev = System.nanoTime()
      val setupNanos = prev - unitStart
      var batch = 0
      while remaining > 0 do
        val n = math.min(cfg.batchSize.toLong, remaining).toInt
        var i = 0
        while i < n do
          rows(i) = records.read()
          i += 1
        result.rowsRead += n
        result.rowsKept += Workload.processBatch(rows, n, result.groups)

        val now = System.nanoTime()
        log += BatchRecord(file, rowGroup, batch, worker, prev - t0, n, now - prev, if batch == 0 then setupNanos else 0L)
        prev = now
        remaining -= n
        batch += 1
      result
    }

  private def writeOutput(path: Path, groups: Seq[(Long, Agg)]): Unit =
    val factory = SimpleGroupFactory(OutputSchema)
    Using.resource(
      ExampleParquetWriter
        .builder(LocalOutputFile(path))
        .withType(OutputSchema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withDictionaryEncoding(false)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build()
    ) { writer =>
      for (hour, a) <- groups do
        writer.write(
          factory
            .newGroup()
            .append("hour_bucket", hour)
            .append("count", a.count)
            .append("sum_metric", a.sum)
            .append("min_metric", a.min)
            .append("max_metric", a.max)
            .append("checksum", a.checksum)
        )
    }
    // Durable output: the measured lifetime ends after this fsync.
    Using.resource(FileChannel.open(path, StandardOpenOption.WRITE))(_.force(true))

  private def writeBatchLog(path: Path, log: Seq[BatchRecord]): Unit =
    Using.resource(Files.newBufferedWriter(path, StandardCharsets.UTF_8)) { w =>
      w.write("batch_id,worker,start_nanos,rows,nanos,setup_nanos\n")
      for r <- log do
        w.write(s"${r.file}-${r.rowGroup}-${r.batch},${r.worker},${r.startNanos},${r.rows},${r.nanos},${r.setupNanos}\n")
    }

  def main(args: Array[String]): Unit =
    val t0 = System.nanoTime()
    val cfg = parseArgs(args)

    val files = listInputs(cfg.input)
    val units = files.zipWithIndex.flatMap { (path, f) =>
      val n = Using.resource(openReader(path))(_.getRowGroups.size)
      (0 until n).map(rg => (f, rg))
    }

    // Workers pull units from a shared counter; results are slotted by unit index.
    val next = AtomicInteger(0)
    val slots = new Array[UnitResult](units.length)
    val logs = Array.fill(cfg.workers)(ArrayBuffer.empty[BatchRecord])
    val pool = Executors.newFixedThreadPool(cfg.workers)
    try
      val futures = (0 until cfg.workers).map { worker =>
        pool.submit(new Runnable:
          def run(): Unit =
            var u = next.getAndIncrement()
            while u < units.length do
              val (f, rg) = units(u)
              slots(u) = runUnit(cfg, files(f), f, rg, worker, t0, logs(worker))
              u = next.getAndIncrement()
        )
      }
      futures.foreach(_.get()) // propagates failures; establishes happens-before for `slots`
    finally pool.shutdown()

    // Deterministic merge in unit order.
    val groups = LongMap.empty[Agg]
    var rowsRead, rowsKept = 0L
    for r <- slots do
      rowsRead += r.rowsRead
      rowsKept += r.rowsKept
      r.groups.foreachEntry((hour, partial) => groups.getOrElseUpdate(hour, Agg()).merge(partial))
    val sorted = groups.toSeq.sortBy(_._1)

    writeOutput(cfg.output, sorted)
    val wallNanos = System.nanoTime() - t0

    // Reporting happens after the measured lifetime.
    cfg.batchLog.foreach { p =>
      writeBatchLog(p, logs.toSeq.flatten.sortBy(r => (r.file, r.rowGroup, r.batch)))
    }
    val gc = ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getName).mkString("+")
    val summary =
      s"""{"lang":"scala","gc":"$gc","workers":${cfg.workers},"batch_size":${cfg.batchSize},""" +
        s""""files":${files.length},"units":${units.length},"rows_read":$rowsRead,"rows_kept":$rowsKept,""" +
        s""""groups":${sorted.length},"wall_nanos":$wallNanos}"""
    cfg.summary.foreach(p => Files.writeString(p, summary + "\n"))
    println(summary)
