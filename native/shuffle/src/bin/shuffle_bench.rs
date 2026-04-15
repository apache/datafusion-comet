// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Standalone shuffle benchmark tool for profiling Comet shuffle write
//! performance outside of Spark. Streams input directly from Parquet files.
//!
//! # Usage
//!
//! ```sh
//! cargo run --release --bin shuffle_bench -- \
//!   --input /data/tpch-sf100/lineitem/ \
//!   --partitions 200 \
//!   --codec lz4 \
//!   --hash-columns 0,3
//! ```
//!
//! Profile with flamegraph:
//! ```sh
//! cargo flamegraph --release --bin shuffle_bench -- \
//!   --input /data/tpch-sf100/lineitem/ \
//!   --partitions 200 --codec lz4
//! ```

use arrow::datatypes::{DataType, SchemaRef};
use clap::Parser;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_comet_shuffle::{CometPartitioning, CompressionCodec, ShuffleWriterExec};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(
    name = "shuffle_bench",
    about = "Standalone benchmark for Comet shuffle write performance"
)]
struct Args {
    /// Path to input Parquet file or directory of Parquet files
    #[arg(long)]
    input: PathBuf,

    /// Batch size for reading Parquet data
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    /// Number of output shuffle partitions
    #[arg(long, default_value_t = 200)]
    partitions: usize,

    /// Partitioning scheme: hash, single, round-robin
    #[arg(long, default_value = "hash")]
    partitioning: String,

    /// Column indices to hash on (comma-separated, e.g. "0,3")
    #[arg(long, default_value = "0")]
    hash_columns: String,

    /// Compression codec: none, lz4, zstd, snappy
    #[arg(long, default_value = "lz4")]
    codec: String,

    /// Zstd compression level (1-22)
    #[arg(long, default_value_t = 1)]
    zstd_level: i32,

    /// Memory limit in bytes (triggers spilling when exceeded)
    #[arg(long)]
    memory_limit: Option<usize>,

    /// Number of iterations to run
    #[arg(long, default_value_t = 1)]
    iterations: usize,

    /// Number of warmup iterations before timing
    #[arg(long, default_value_t = 0)]
    warmup: usize,

    /// Output directory for shuffle data/index files
    #[arg(long, default_value = "/tmp/comet_shuffle_bench")]
    output_dir: PathBuf,

    /// Write buffer size in bytes
    #[arg(long, default_value_t = 1048576)]
    write_buffer_size: usize,

    /// Limit rows processed per iteration (0 = no limit)
    #[arg(long, default_value_t = 0)]
    limit: usize,

    /// Number of concurrent shuffle tasks to simulate executor parallelism.
    /// Each task reads the same input and writes to its own output files.
    #[arg(long, default_value_t = 1)]
    concurrent_tasks: usize,

    /// Shuffle mode: 'immediate' writes IPC blocks per batch as they arrive,
    /// 'buffered' buffers all rows before writing (original behavior).
    #[arg(long, default_value = "immediate")]
    mode: String,
}

fn main() {
    let args = Args::parse();

    // Create output directory
    fs::create_dir_all(&args.output_dir).expect("Failed to create output directory");
    let data_file = args.output_dir.join("data.out");
    let index_file = args.output_dir.join("index.out");

    let (schema, total_rows) = read_parquet_metadata(&args.input, args.limit);

    let codec = parse_codec(&args.codec, args.zstd_level);
    let hash_col_indices = parse_hash_columns(&args.hash_columns);

    println!("=== Shuffle Benchmark ===");
    println!("Input:          {}", args.input.display());
    println!(
        "Schema:         {} columns ({})",
        schema.fields().len(),
        describe_schema(&schema)
    );
    println!("Total rows:     {}", format_number(total_rows as usize));
    println!("Batch size:     {}", format_number(args.batch_size));
    println!("Partitioning:   {}", args.partitioning);
    println!("Partitions:     {}", args.partitions);
    println!("Codec:          {:?}", codec);
    println!("Mode:           {}", args.mode);
    println!("Hash columns:   {:?}", hash_col_indices);
    if let Some(mem_limit) = args.memory_limit {
        println!("Memory limit:   {}", format_bytes(mem_limit));
    }
    if args.concurrent_tasks > 1 {
        println!("Concurrent:     {} tasks", args.concurrent_tasks);
    }
    println!(
        "Iterations:     {} (warmup: {})",
        args.iterations, args.warmup
    );
    println!();

    let total_iters = args.warmup + args.iterations;
    let mut write_times = Vec::with_capacity(args.iterations);
    let mut data_file_sizes = Vec::with_capacity(args.iterations);
    let mut last_metrics: Option<MetricsSet> = None;
    let mut last_input_metrics: Option<MetricsSet> = None;

    for i in 0..total_iters {
        let is_warmup = i < args.warmup;
        let label = if is_warmup {
            format!("warmup {}/{}", i + 1, args.warmup)
        } else {
            format!("iter {}/{}", i - args.warmup + 1, args.iterations)
        };

        let (write_elapsed, metrics, input_metrics) = if args.concurrent_tasks > 1 {
            let elapsed = run_concurrent_shuffle_writes(
                &args.input,
                &schema,
                &codec,
                &hash_col_indices,
                &args,
            );
            (elapsed, None, None)
        } else {
            run_shuffle_write(
                &args.input,
                &schema,
                &codec,
                &hash_col_indices,
                &args,
                data_file.to_str().unwrap(),
                index_file.to_str().unwrap(),
            )
        };
        let data_size = fs::metadata(&data_file).map(|m| m.len()).unwrap_or(0);

        if !is_warmup {
            write_times.push(write_elapsed);
            data_file_sizes.push(data_size);
            last_metrics = metrics;
            last_input_metrics = input_metrics;
        }

        print!("  [{label}] write: {:.3}s", write_elapsed);
        if args.concurrent_tasks <= 1 {
            print!("  output: {}", format_bytes(data_size as usize));
        }

        println!();
    }

    if args.iterations > 0 {
        println!();
        println!("=== Results ===");

        let avg_write = write_times.iter().sum::<f64>() / write_times.len() as f64;
        let write_throughput_rows = (total_rows as f64 * args.concurrent_tasks as f64) / avg_write;

        println!("Write:");
        println!("  avg time:         {:.3}s", avg_write);
        if write_times.len() > 1 {
            let min = write_times.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = write_times
                .iter()
                .cloned()
                .fold(f64::NEG_INFINITY, f64::max);
            println!("  min/max:          {:.3}s / {:.3}s", min, max);
        }
        println!(
            "  throughput:       {} rows/s (total across {} tasks)",
            format_number(write_throughput_rows as usize),
            args.concurrent_tasks
        );
        if args.concurrent_tasks <= 1 {
            let avg_data_size = data_file_sizes.iter().sum::<u64>() / data_file_sizes.len() as u64;
            println!(
                "  output size:      {}",
                format_bytes(avg_data_size as usize)
            );
        }

        if let Some(ref metrics) = last_input_metrics {
            println!();
            println!("Input Metrics (last iteration):");
            print_input_metrics(metrics);
        }

        if let Some(ref metrics) = last_metrics {
            println!();
            println!("Shuffle Metrics (last iteration):");
            print_shuffle_metrics(metrics, avg_write);
        }
    }

    let _ = fs::remove_file(&data_file);
    let _ = fs::remove_file(&index_file);
}

fn print_shuffle_metrics(metrics: &MetricsSet, total_wall_time_secs: f64) {
    let get_metric = |name: &str| -> Option<usize> {
        metrics
            .iter()
            .find(|m| m.value().name() == name)
            .map(|m| m.value().as_usize())
    };

    let total_ns = (total_wall_time_secs * 1e9) as u64;
    let fmt_time = |nanos: usize| -> String {
        let secs = nanos as f64 / 1e9;
        let pct = if total_ns > 0 {
            (nanos as f64 / total_ns as f64) * 100.0
        } else {
            0.0
        };
        format!("{:.3}s ({:.1}%)", secs, pct)
    };

    if let Some(input_batches) = get_metric("input_batches") {
        println!("  input batches:    {}", format_number(input_batches));
    }
    if let Some(nanos) = get_metric("repart_time") {
        println!("  repart time:      {}", fmt_time(nanos));
    }
    if let Some(nanos) = get_metric("encode_time") {
        println!("  encode time:      {}", fmt_time(nanos));
    }
    if let Some(nanos) = get_metric("write_time") {
        println!("  write time:       {}", fmt_time(nanos));
    }

    if let Some(spill_count) = get_metric("spill_count") {
        if spill_count > 0 {
            println!("  spill count:      {}", format_number(spill_count));
        }
    }
    if let Some(spilled_bytes) = get_metric("spilled_bytes") {
        if spilled_bytes > 0 {
            println!("  spilled bytes:    {}", format_bytes(spilled_bytes));
        }
    }
    if let Some(data_size) = get_metric("data_size") {
        if data_size > 0 {
            println!("  data size:        {}", format_bytes(data_size));
        }
    }
}

fn print_input_metrics(metrics: &MetricsSet) {
    let aggregated = metrics.aggregate_by_name();
    for m in aggregated.iter() {
        let value = m.value();
        let name = value.name();
        let v = value.as_usize();
        if v == 0 {
            continue;
        }
        // Format time metrics as seconds, everything else as a number
        // Skip start/end timestamps — not useful in benchmark output
        if matches!(
            value,
            MetricValue::StartTimestamp(_) | MetricValue::EndTimestamp(_)
        ) {
            continue;
        }
        let is_time = matches!(
            value,
            MetricValue::ElapsedCompute(_) | MetricValue::Time { .. }
        );
        if is_time {
            println!("  {name}: {:.3}s", v as f64 / 1e9);
        } else if name.contains("bytes") || name.contains("size") {
            println!("  {name}: {}", format_bytes(v));
        } else {
            println!("  {name}: {}", format_number(v));
        }
    }
}

/// Read schema and total row count from Parquet metadata without loading any data.
fn read_parquet_metadata(path: &Path, limit: usize) -> (SchemaRef, u64) {
    let paths = collect_parquet_paths(path);
    let mut schema = None;
    let mut total_rows = 0u64;

    for file_path in &paths {
        let file = fs::File::open(file_path)
            .unwrap_or_else(|e| panic!("Failed to open {}: {}", file_path.display(), e));
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap_or_else(|e| {
            panic!(
                "Failed to read Parquet metadata from {}: {}",
                file_path.display(),
                e
            )
        });
        if schema.is_none() {
            schema = Some(Arc::clone(builder.schema()));
        }
        total_rows += builder.metadata().file_metadata().num_rows() as u64;
        if limit > 0 && total_rows >= limit as u64 {
            total_rows = total_rows.min(limit as u64);
            break;
        }
    }

    (schema.expect("No parquet files found"), total_rows)
}

fn collect_parquet_paths(path: &Path) -> Vec<PathBuf> {
    if path.is_dir() {
        let mut files: Vec<PathBuf> = fs::read_dir(path)
            .unwrap_or_else(|e| panic!("Failed to read directory {}: {}", path.display(), e))
            .filter_map(|entry| {
                let p = entry.ok()?.path();
                if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    Some(p)
                } else {
                    None
                }
            })
            .collect();
        files.sort();
        if files.is_empty() {
            panic!("No .parquet files found in {}", path.display());
        }
        files
    } else {
        vec![path.to_path_buf()]
    }
}

fn run_shuffle_write(
    input_path: &Path,
    schema: &SchemaRef,
    codec: &CompressionCodec,
    hash_col_indices: &[usize],
    args: &Args,
    data_file: &str,
    index_file: &str,
) -> (f64, Option<MetricsSet>, Option<MetricsSet>) {
    let partitioning = build_partitioning(
        &args.partitioning,
        args.partitions,
        hash_col_indices,
        schema,
    );

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let start = Instant::now();
        let (shuffle_metrics, input_metrics) = execute_shuffle_write(
            input_path.to_str().unwrap(),
            codec.clone(),
            partitioning,
            args.batch_size,
            args.memory_limit,
            args.write_buffer_size,
            args.limit,
            data_file.to_string(),
            index_file.to_string(),
            args.mode == "immediate",
        )
        .await
        .unwrap();
        (
            start.elapsed().as_secs_f64(),
            Some(shuffle_metrics),
            Some(input_metrics),
        )
    })
}

/// Core async shuffle write logic shared by single and concurrent paths.
#[allow(clippy::too_many_arguments)]
async fn execute_shuffle_write(
    input_path: &str,
    codec: CompressionCodec,
    partitioning: CometPartitioning,
    batch_size: usize,
    memory_limit: Option<usize>,
    write_buffer_size: usize,
    limit: usize,
    data_file: String,
    index_file: String,
    immediate_mode: bool,
) -> datafusion::common::Result<(MetricsSet, MetricsSet)> {
    let config = SessionConfig::new().with_batch_size(batch_size);
    let mut runtime_builder = RuntimeEnvBuilder::new();
    if let Some(mem_limit) = memory_limit {
        runtime_builder = runtime_builder.with_memory_limit(mem_limit, 1.0);
    }
    let runtime_env = Arc::new(runtime_builder.build().unwrap());
    let ctx = SessionContext::new_with_config_rt(config, runtime_env);

    let mut df = ctx
        .read_parquet(input_path, ParquetReadOptions::default())
        .await
        .expect("Failed to create Parquet scan");
    if limit > 0 {
        df = df.limit(0, Some(limit)).unwrap();
    }

    let parquet_plan = df
        .create_physical_plan()
        .await
        .expect("Failed to create physical plan");

    let input: Arc<dyn ExecutionPlan> = if parquet_plan
        .properties()
        .output_partitioning()
        .partition_count()
        > 1
    {
        Arc::new(CoalescePartitionsExec::new(parquet_plan))
    } else {
        parquet_plan
    };

    let exec = ShuffleWriterExec::try_new(
        input,
        partitioning,
        codec,
        data_file,
        index_file,
        false,
        write_buffer_size,
        immediate_mode,
    )
    .expect("Failed to create ShuffleWriterExec");

    let task_ctx = ctx.task_ctx();
    let stream = exec.execute(0, task_ctx).unwrap();
    collect(stream).await.unwrap();

    // Collect metrics from the input plan (Parquet scan + optional coalesce)
    let input_metrics = collect_input_metrics(&exec);

    Ok((exec.metrics().unwrap_or_default(), input_metrics))
}

/// Walk the plan tree and aggregate all metrics from input plans (everything below shuffle writer).
fn collect_input_metrics(exec: &ShuffleWriterExec) -> MetricsSet {
    let mut all_metrics = MetricsSet::new();
    fn gather(plan: &dyn ExecutionPlan, out: &mut MetricsSet) {
        if let Some(metrics) = plan.metrics() {
            for m in metrics.iter() {
                out.push(Arc::clone(m));
            }
        }
        for child in plan.children() {
            gather(child.as_ref(), out);
        }
    }
    for child in exec.children() {
        gather(child.as_ref(), &mut all_metrics);
    }
    all_metrics
}

/// Run N concurrent shuffle writes to simulate executor parallelism.
/// Returns wall-clock time for all tasks to complete.
fn run_concurrent_shuffle_writes(
    input_path: &Path,
    schema: &SchemaRef,
    codec: &CompressionCodec,
    hash_col_indices: &[usize],
    args: &Args,
) -> f64 {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let start = Instant::now();

        let mut handles = Vec::with_capacity(args.concurrent_tasks);
        for task_id in 0..args.concurrent_tasks {
            let task_dir = args.output_dir.join(format!("task_{task_id}"));
            fs::create_dir_all(&task_dir).expect("Failed to create task output directory");
            let data_file = task_dir.join("data.out").to_str().unwrap().to_string();
            let index_file = task_dir.join("index.out").to_str().unwrap().to_string();

            let input_str = input_path.to_str().unwrap().to_string();
            let codec = codec.clone();
            let partitioning = build_partitioning(
                &args.partitioning,
                args.partitions,
                hash_col_indices,
                schema,
            );
            let batch_size = args.batch_size;
            let memory_limit = args.memory_limit;
            let write_buffer_size = args.write_buffer_size;
            let limit = args.limit;
            let immediate_mode = args.mode == "immediate";

            handles.push(tokio::spawn(async move {
                execute_shuffle_write(
                    &input_str,
                    codec,
                    partitioning,
                    batch_size,
                    memory_limit,
                    write_buffer_size,
                    limit,
                    data_file,
                    index_file,
                    immediate_mode,
                )
                .await
                .unwrap()
            }));
        }

        for handle in handles {
            handle.await.expect("Task panicked");
        }

        for task_id in 0..args.concurrent_tasks {
            let task_dir = args.output_dir.join(format!("task_{task_id}"));
            let _ = fs::remove_dir_all(&task_dir);
        }

        start.elapsed().as_secs_f64()
    })
}

fn build_partitioning(
    scheme: &str,
    num_partitions: usize,
    hash_col_indices: &[usize],
    schema: &SchemaRef,
) -> CometPartitioning {
    match scheme {
        "single" => CometPartitioning::SinglePartition,
        "round-robin" => CometPartitioning::RoundRobin(num_partitions, 0),
        "hash" => {
            let exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = hash_col_indices
                .iter()
                .map(|&idx| {
                    let field = schema.field(idx);
                    Arc::new(Column::new(field.name(), idx))
                        as Arc<dyn datafusion::physical_expr::PhysicalExpr>
                })
                .collect();
            CometPartitioning::Hash(exprs, num_partitions)
        }
        other => {
            eprintln!("Unknown partitioning scheme: {other}. Using hash.");
            build_partitioning("hash", num_partitions, hash_col_indices, schema)
        }
    }
}

fn parse_codec(codec: &str, zstd_level: i32) -> CompressionCodec {
    match codec.to_lowercase().as_str() {
        "none" => CompressionCodec::None,
        "lz4" => CompressionCodec::Lz4Frame,
        "zstd" => CompressionCodec::Zstd(zstd_level),
        "snappy" => CompressionCodec::Snappy,
        other => {
            eprintln!("Unknown codec: {other}. Using zstd.");
            CompressionCodec::Zstd(zstd_level)
        }
    }
}

fn parse_hash_columns(s: &str) -> Vec<usize> {
    s.split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().parse::<usize>().expect("Invalid column index"))
        .collect()
}

fn describe_schema(schema: &arrow::datatypes::Schema) -> String {
    let mut counts = std::collections::HashMap::new();
    for field in schema.fields() {
        let type_name = match field.data_type() {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => "int",
            DataType::Float16 | DataType::Float32 | DataType::Float64 => "float",
            DataType::Utf8 | DataType::LargeUtf8 => "string",
            DataType::Boolean => "bool",
            DataType::Date32 | DataType::Date64 => "date",
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "decimal",
            DataType::Timestamp(_, _) => "timestamp",
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => "binary",
            _ => "other",
        };
        *counts.entry(type_name).or_insert(0) += 1;
    }
    let mut parts: Vec<String> = counts
        .into_iter()
        .map(|(k, v)| format!("{}x{}", v, k))
        .collect();
    parts.sort();
    parts.join(", ")
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}
