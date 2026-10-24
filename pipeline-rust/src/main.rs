//! Native Rust pipeline: stream (file, row group) units through 4 workers,
//! filter/transform/aggregate per row, write hourly aggregates to Parquet.

mod workload;

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, AsArray, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use workload::{process_batch, Agg, Row};

#[cfg(all(feature = "system", feature = "mimalloc"))]
compile_error!("enable exactly one allocator feature: `system` (default) or `mimalloc`");

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ALLOCATOR: &str = if cfg!(feature = "mimalloc") { "mimalloc" } else { "system" };

type Error = Box<dyn std::error::Error + Send + Sync>;

struct Config {
    input: PathBuf,
    output: PathBuf,
    workers: usize,
    batch_size: usize,
    batch_log: Option<PathBuf>,
    summary: Option<PathBuf>,
}

fn usage() -> ! {
    eprintln!(
        "usage: pipeline-rust --input DIR --output FILE [--workers 4] [--batch-size 8192]\n\
         \x20                    [--batch-log FILE.csv] [--summary FILE.json]"
    );
    std::process::exit(2);
}

fn parse_args() -> Config {
    let (mut input, mut output, mut batch_log, mut summary) = (None, None, None, None);
    let (mut workers, mut batch_size) = (4usize, 8192usize);
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || args.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--input" => input = Some(PathBuf::from(value())),
            "--output" => output = Some(PathBuf::from(value())),
            "--workers" => workers = value().parse().unwrap_or_else(|_| usage()),
            "--batch-size" => batch_size = value().parse().unwrap_or_else(|_| usage()),
            "--batch-log" => batch_log = Some(PathBuf::from(value())),
            "--summary" => summary = Some(PathBuf::from(value())),
            _ => usage(),
        }
    }
    if workers == 0 || batch_size == 0 {
        usage();
    }
    Config {
        input: input.unwrap_or_else(|| usage()),
        output: output.unwrap_or_else(|| usage()),
        workers,
        batch_size,
        batch_log,
        summary,
    }
}

struct BatchRecord {
    file: usize,
    row_group: usize,
    batch: usize,
    worker: usize,
    start_nanos: u128,
    rows: usize,
    /// Time to process this batch: decode + materialize + filter/transform/aggregate.
    nanos: u128,
    /// Unit setup before the first batch (open file, read footer, prepare reader);
    /// non-zero only on batch 0 of each unit.
    setup_nanos: u128,
}

struct UnitResult {
    groups: HashMap<i64, Agg>,
    rows_read: u64,
    rows_kept: u64,
}

/// Input files (`*.parquet`) sorted by file name.
fn list_inputs(dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<_, _>>()?;
    files.retain(|p| p.extension().is_some_and(|e| e == "parquet"));
    files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    if files.is_empty() {
        return Err(format!("no .parquet files in {}", dir.display()).into());
    }
    Ok(files)
}

fn materialize(batch: &RecordBatch) -> Vec<Row> {
    let schema = batch.schema();
    let col = |name: &str| batch.column(schema.index_of(name).expect("input column"));
    let ids = col("id").as_fixed_size_binary();
    let tss = col("ts").as_primitive::<Int64Type>();
    let payloads = col("payload").as_string::<i32>();
    let metrics = col("metric").as_primitive::<Float64Type>();

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        rows.push(Row {
            id: ids.value(i).try_into().expect("16-byte id"),
            ts: tss.value(i),
            payload: payloads.value(i).to_owned(),
            metric: metrics.value(i),
        });
    }
    rows
}

fn run_unit(
    cfg: &Config,
    path: &Path,
    (file, row_group): (usize, usize),
    worker: usize,
    t0: Instant,
    log: &mut Vec<BatchRecord>,
) -> Result<UnitResult, Error> {
    let unit_start = Instant::now();
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
        .with_row_groups(vec![row_group])
        .with_batch_size(cfg.batch_size)
        .build()?;

    let mut result = UnitResult { groups: HashMap::new(), rows_read: 0, rows_kept: 0 };
    let mut prev = Instant::now();
    let setup_nanos = (prev - unit_start).as_nanos();
    for (batch_idx, batch) in reader.enumerate() {
        let batch = batch?;
        let rows = materialize(&batch);
        result.rows_read += rows.len() as u64;
        result.rows_kept += process_batch(rows, &mut result.groups);

        let now = Instant::now();
        log.push(BatchRecord {
            file,
            row_group,
            batch: batch_idx,
            worker,
            start_nanos: (prev - t0).as_nanos(),
            rows: batch.num_rows(),
            nanos: (now - prev).as_nanos(),
            setup_nanos: if batch_idx == 0 { setup_nanos } else { 0 },
        });
        prev = now;
    }
    Ok(result)
}

fn write_output(path: &Path, groups: &BTreeMap<i64, Agg>) -> Result<(), Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("hour_bucket", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
        Field::new("sum_metric", DataType::Float64, false),
        Field::new("min_metric", DataType::Float64, false),
        Field::new("max_metric", DataType::Float64, false),
        Field::new("checksum", DataType::Int64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(groups.keys().copied())),
        Arc::new(Int64Array::from_iter_values(groups.values().map(|a| a.count))),
        Arc::new(Float64Array::from_iter_values(groups.values().map(|a| a.sum))),
        Arc::new(Float64Array::from_iter_values(groups.values().map(|a| a.min))),
        Arc::new(Float64Array::from_iter_values(groups.values().map(|a| a.max))),
        Arc::new(Int64Array::from_iter_values(groups.values().map(|a| a.checksum as i64))),
    ];
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema.clone(), Some(props))?;
    writer.write(&RecordBatch::try_new(schema, columns)?)?;
    writer.close()?;
    // Durable output: the measured lifetime ends after this fsync.
    File::options().write(true).open(path)?.sync_all()?;
    Ok(())
}

fn write_batch_log(path: &Path, log: &[BatchRecord]) -> std::io::Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    writeln!(w, "batch_id,worker,start_nanos,rows,nanos,setup_nanos")?;
    for r in log {
        writeln!(
            w,
            "{}-{}-{},{},{},{},{},{}",
            r.file, r.row_group, r.batch, r.worker, r.start_nanos, r.rows, r.nanos, r.setup_nanos
        )?;
    }
    w.flush()
}

fn main() -> Result<(), Error> {
    let t0 = Instant::now();
    let cfg = parse_args();

    let files = list_inputs(&cfg.input)?;
    let mut units = Vec::new();
    for (f, path) in files.iter().enumerate() {
        let n = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
            .metadata()
            .num_row_groups();
        units.extend((0..n).map(|rg| (f, rg)));
    }

    // Workers pull units from a shared counter; results are slotted by unit index.
    let next = AtomicUsize::new(0);
    let per_worker: Vec<(Vec<(usize, UnitResult)>, Vec<BatchRecord>)> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..cfg.workers)
            .map(|worker| {
                let (cfg, files, units, next) = (&cfg, &files, &units, &next);
                s.spawn(move || {
                    let mut done = Vec::new();
                    let mut log = Vec::new();
                    loop {
                        let u = next.fetch_add(1, Ordering::Relaxed);
                        if u >= units.len() {
                            break;
                        }
                        let unit = units[u];
                        let r = run_unit(cfg, &files[unit.0], unit, worker, t0, &mut log)
                            .unwrap_or_else(|e| panic!("unit {unit:?}: {e}"));
                        done.push((u, r));
                    }
                    (done, log)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().expect("worker panicked")).collect()
    });

    let mut slots: Vec<Option<UnitResult>> = (0..units.len()).map(|_| None).collect();
    let mut log = Vec::new();
    for (done, worker_log) in per_worker {
        for (u, r) in done {
            slots[u] = Some(r);
        }
        log.extend(worker_log);
    }

    // Deterministic merge in unit order.
    let mut groups: BTreeMap<i64, Agg> = BTreeMap::new();
    let (mut rows_read, mut rows_kept) = (0u64, 0u64);
    for slot in slots {
        let r = slot.expect("every unit processed");
        rows_read += r.rows_read;
        rows_kept += r.rows_kept;
        for (hour, partial) in &r.groups {
            groups.entry(*hour).or_default().merge(partial);
        }
    }

    write_output(&cfg.output, &groups)?;
    let wall_nanos = t0.elapsed().as_nanos();

    // Reporting happens after the measured lifetime.
    if let Some(path) = &cfg.batch_log {
        log.sort_by_key(|r| (r.file, r.row_group, r.batch));
        write_batch_log(path, &log)?;
    }
    let summary = format!(
        "{{\"lang\":\"rust\",\"allocator\":\"{ALLOCATOR}\",\"workers\":{},\"batch_size\":{},\
         \"files\":{},\"units\":{},\"rows_read\":{rows_read},\"rows_kept\":{rows_kept},\
         \"groups\":{},\"wall_nanos\":{wall_nanos}}}",
        cfg.workers,
        cfg.batch_size,
        files.len(),
        units.len(),
        groups.len()
    );
    if let Some(path) = &cfg.summary {
        fs::write(path, format!("{summary}\n"))?;
    }
    println!("{summary}");
    Ok(())
}
