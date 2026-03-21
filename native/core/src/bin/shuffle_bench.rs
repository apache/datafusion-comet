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

//! Standalone shuffle benchmark tool for profiling Comet shuffle write and read
//! outside of Spark.
//!
//! # Usage
//!
//! Read from Parquet files (e.g. TPC-H lineitem):
//! ```sh
//! cargo run --release --bin shuffle_bench -- \
//!   --input /data/tpch-sf100/lineitem/ \
//!   --partitions 200 \
//!   --codec zstd --zstd-level 1 \
//!   --hash-columns 0,3 \
//!   --read-back
//! ```
//!
//! Generate synthetic data:
//! ```sh
//! cargo run --release --bin shuffle_bench -- \
//!   --generate --gen-rows 10000000 --gen-string-cols 4 --gen-int-cols 4 \
//!   --gen-decimal-cols 2 --gen-avg-string-len 32 \
//!   --partitions 200 --codec lz4 --read-back
//! ```
//!
//! Profile with flamegraph:
//! ```sh
//! cargo flamegraph --release --bin shuffle_bench -- \
//!   --input /data/tpch-sf100/lineitem/ \
//!   --partitions 200 --codec zstd --zstd-level 1
//! ```

use arrow::array::builder::{Date32Builder, Decimal128Builder, Int64Builder, StringBuilder};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use clap::Parser;
use comet::execution::shuffle::{
    read_shuffle_block, CometPartitioning, CompressionCodec, ShuffleWriterExec,
};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rand::RngExt;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(
    name = "shuffle_bench",
    about = "Standalone benchmark for Comet shuffle write and read performance"
)]
struct Args {
    /// Path to input Parquet file or directory of Parquet files
    #[arg(long)]
    input: Option<PathBuf>,

    /// Generate synthetic data instead of reading from Parquet
    #[arg(long, default_value_t = false)]
    generate: bool,

    /// Number of rows to generate (requires --generate)
    #[arg(long, default_value_t = 1_000_000)]
    gen_rows: usize,

    /// Number of Int64 columns to generate
    #[arg(long, default_value_t = 4)]
    gen_int_cols: usize,

    /// Number of Utf8 string columns to generate
    #[arg(long, default_value_t = 2)]
    gen_string_cols: usize,

    /// Number of Decimal128 columns to generate
    #[arg(long, default_value_t = 2)]
    gen_decimal_cols: usize,

    /// Number of Date32 columns to generate
    #[arg(long, default_value_t = 1)]
    gen_date_cols: usize,

    /// Average string length for generated string columns
    #[arg(long, default_value_t = 24)]
    gen_avg_string_len: usize,

    /// Batch size for reading Parquet or generating data
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
    #[arg(long, default_value = "zstd")]
    codec: String,

    /// Zstd compression level (1-22)
    #[arg(long, default_value_t = 1)]
    zstd_level: i32,

    /// Memory limit in bytes (triggers spilling when exceeded)
    #[arg(long)]
    memory_limit: Option<usize>,

    /// Also benchmark reading back the shuffle output
    #[arg(long, default_value_t = false)]
    read_back: bool,

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

    /// Maximum number of rows to use (default: 1,000,000)
    #[arg(long, default_value_t = 1_000_000)]
    limit: usize,
}

fn main() {
    let args = Args::parse();

    // Validate args
    if args.input.is_none() && !args.generate {
        eprintln!("Error: must specify either --input <path> or --generate");
        std::process::exit(1);
    }

    // Create output directory
    fs::create_dir_all(&args.output_dir).expect("Failed to create output directory");

    let data_file = args.output_dir.join("data.out");
    let index_file = args.output_dir.join("index.out");

    // Load data
    let load_start = Instant::now();
    let batches = if let Some(ref input_path) = args.input {
        load_parquet(input_path, args.batch_size, args.limit)
    } else {
        generate_data(&args)
    };
    let load_elapsed = load_start.elapsed();

    let schema = batches[0].schema();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let total_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

    println!("=== Shuffle Benchmark ===");
    println!(
        "Data source:    {}",
        if args.input.is_some() {
            "parquet"
        } else {
            "generated"
        }
    );
    println!(
        "Schema:         {} columns ({} fields)",
        schema.fields().len(),
        describe_schema(&schema)
    );
    println!("Total rows:     {}", format_number(total_rows));
    println!("Total size:     {}", format_bytes(total_bytes));
    println!("Batches:        {}", batches.len());
    println!(
        "Rows/batch:     ~{}",
        if batches.is_empty() {
            0
        } else {
            total_rows / batches.len()
        }
    );
    println!("Load time:      {:.3}s", load_elapsed.as_secs_f64());
    println!();

    let codec = parse_codec(&args.codec, args.zstd_level);
    let hash_col_indices = parse_hash_columns(&args.hash_columns);

    println!("Partitioning:   {}", args.partitioning);
    println!("Partitions:     {}", args.partitions);
    println!("Codec:          {:?}", codec);
    println!("Hash columns:   {:?}", hash_col_indices);
    if let Some(mem_limit) = args.memory_limit {
        println!("Memory limit:   {}", format_bytes(mem_limit));
    }
    println!(
        "Iterations:     {} (warmup: {})",
        args.iterations, args.warmup
    );
    println!();

    // Run warmup + timed iterations
    let total_iters = args.warmup + args.iterations;
    let mut write_times = Vec::with_capacity(args.iterations);
    let mut read_times = Vec::with_capacity(args.iterations);
    let mut data_file_sizes = Vec::with_capacity(args.iterations);

    for i in 0..total_iters {
        let is_warmup = i < args.warmup;
        let label = if is_warmup {
            format!("warmup {}/{}", i + 1, args.warmup)
        } else {
            format!("iter {}/{}", i - args.warmup + 1, args.iterations)
        };

        // Write phase
        let write_elapsed = run_shuffle_write(
            &batches,
            &schema,
            &codec,
            &hash_col_indices,
            &args,
            data_file.to_str().unwrap(),
            index_file.to_str().unwrap(),
        );
        let data_size = fs::metadata(&data_file).map(|m| m.len()).unwrap_or(0);

        if !is_warmup {
            write_times.push(write_elapsed);
            data_file_sizes.push(data_size);
        }

        print!("  [{label}] write: {:.3}s", write_elapsed);
        print!("  output: {}", format_bytes(data_size as usize));

        // Read phase
        if args.read_back {
            let read_elapsed = run_shuffle_read(
                data_file.to_str().unwrap(),
                index_file.to_str().unwrap(),
                args.partitions,
                &schema,
            );
            if !is_warmup {
                read_times.push(read_elapsed);
            }
            print!("  read: {:.3}s", read_elapsed);
        }
        println!();
    }

    // Print summary
    if args.iterations > 0 {
        println!();
        println!("=== Results ===");

        let avg_write = write_times.iter().sum::<f64>() / write_times.len() as f64;
        let avg_data_size = data_file_sizes.iter().sum::<u64>() / data_file_sizes.len() as u64;
        let write_throughput_rows = total_rows as f64 / avg_write;
        let write_throughput_bytes = total_bytes as f64 / avg_write;
        let compression_ratio = if avg_data_size > 0 {
            total_bytes as f64 / avg_data_size as f64
        } else {
            0.0
        };

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
            "  throughput:       {}/s ({} rows/s)",
            format_bytes(write_throughput_bytes as usize),
            format_number(write_throughput_rows as usize)
        );
        println!(
            "  output size:      {}",
            format_bytes(avg_data_size as usize)
        );
        println!("  compression:      {:.2}x", compression_ratio);

        if !read_times.is_empty() {
            let avg_read = read_times.iter().sum::<f64>() / read_times.len() as f64;
            let read_throughput_bytes = avg_data_size as f64 / avg_read;

            println!("Read:");
            println!("  avg time:         {:.3}s", avg_read);
            if read_times.len() > 1 {
                let min = read_times.iter().cloned().fold(f64::INFINITY, f64::min);
                let max = read_times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                println!("  min/max:          {:.3}s / {:.3}s", min, max);
            }
            println!(
                "  throughput:       {}/s (from compressed)",
                format_bytes(read_throughput_bytes as usize)
            );
        }
    }

    // Cleanup
    let _ = fs::remove_file(&data_file);
    let _ = fs::remove_file(&index_file);
}

fn load_parquet(path: &PathBuf, batch_size: usize, limit: usize) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    let mut total_rows = 0usize;

    let paths = if path.is_dir() {
        let mut files: Vec<PathBuf> = fs::read_dir(path)
            .expect("Failed to read input directory")
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let p = entry.path();
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
        vec![path.clone()]
    };

    'outer: for file_path in &paths {
        let file = fs::File::open(file_path)
            .unwrap_or_else(|e| panic!("Failed to open {}: {}", file_path.display(), e));
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap_or_else(|e| {
            panic!(
                "Failed to read Parquet metadata from {}: {}",
                file_path.display(),
                e
            )
        });
        let reader = builder
            .with_batch_size(batch_size)
            .build()
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to build Parquet reader for {}: {}",
                    file_path.display(),
                    e
                )
            });
        for batch_result in reader {
            let batch = batch_result.unwrap_or_else(|e| {
                panic!("Failed to read batch from {}: {}", file_path.display(), e)
            });
            if batch.num_rows() == 0 {
                continue;
            }
            let remaining = limit - total_rows;
            if batch.num_rows() <= remaining {
                total_rows += batch.num_rows();
                batches.push(batch);
            } else {
                batches.push(batch.slice(0, remaining));
                total_rows += remaining;
            }
            if total_rows >= limit {
                break 'outer;
            }
        }
    }

    if batches.is_empty() {
        panic!("No data read from input");
    }

    println!(
        "Loaded {} batches ({} rows) from {} file(s)",
        batches.len(),
        format_number(total_rows),
        paths.len()
    );
    batches
}

fn generate_data(args: &Args) -> Vec<RecordBatch> {
    let mut fields = Vec::new();
    let mut col_idx = 0;

    // Int64 columns
    for _ in 0..args.gen_int_cols {
        fields.push(Field::new(
            format!("int_col_{col_idx}"),
            DataType::Int64,
            true,
        ));
        col_idx += 1;
    }
    // String columns
    for _ in 0..args.gen_string_cols {
        fields.push(Field::new(
            format!("str_col_{col_idx}"),
            DataType::Utf8,
            true,
        ));
        col_idx += 1;
    }
    // Decimal columns
    for _ in 0..args.gen_decimal_cols {
        fields.push(Field::new(
            format!("dec_col_{col_idx}"),
            DataType::Decimal128(18, 2),
            true,
        ));
        col_idx += 1;
    }
    // Date columns
    for _ in 0..args.gen_date_cols {
        fields.push(Field::new(
            format!("date_col_{col_idx}"),
            DataType::Date32,
            true,
        ));
        col_idx += 1;
    }

    let schema = Arc::new(Schema::new(fields));
    let mut batches = Vec::new();
    let mut rng = rand::rng();
    let mut remaining = args.gen_rows;

    while remaining > 0 {
        let batch_rows = remaining.min(args.batch_size);
        remaining -= batch_rows;

        let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

        // Int64 columns
        for _ in 0..args.gen_int_cols {
            let mut builder = Int64Builder::with_capacity(batch_rows);
            for _ in 0..batch_rows {
                if rng.random_range(0..100) < 5 {
                    builder.append_null();
                } else {
                    builder.append_value(rng.random_range(0..1_000_000i64));
                }
            }
            columns.push(Arc::new(builder.finish()));
        }
        // String columns
        for _ in 0..args.gen_string_cols {
            let mut builder =
                StringBuilder::with_capacity(batch_rows, batch_rows * args.gen_avg_string_len);
            for _ in 0..batch_rows {
                if rng.random_range(0..100) < 5 {
                    builder.append_null();
                } else {
                    let len = rng.random_range(1..args.gen_avg_string_len * 2);
                    let s: String = (0..len)
                        .map(|_| rng.random_range(b'a'..=b'z') as char)
                        .collect();
                    builder.append_value(&s);
                }
            }
            columns.push(Arc::new(builder.finish()));
        }
        // Decimal columns
        for _ in 0..args.gen_decimal_cols {
            let mut builder = Decimal128Builder::with_capacity(batch_rows)
                .with_precision_and_scale(18, 2)
                .unwrap();
            for _ in 0..batch_rows {
                if rng.random_range(0..100) < 5 {
                    builder.append_null();
                } else {
                    builder.append_value(rng.random_range(0..100_000_000i128));
                }
            }
            columns.push(Arc::new(builder.finish()));
        }
        // Date columns
        for _ in 0..args.gen_date_cols {
            let mut builder = Date32Builder::with_capacity(batch_rows);
            for _ in 0..batch_rows {
                if rng.random_range(0..100) < 5 {
                    builder.append_null();
                } else {
                    builder.append_value(rng.random_range(0..20000i32));
                }
            }
            columns.push(Arc::new(builder.finish()));
        }

        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        batches.push(batch);
    }

    println!(
        "Generated {} batches ({} rows)",
        batches.len(),
        args.gen_rows
    );
    batches
}

fn run_shuffle_write(
    batches: &[RecordBatch],
    schema: &SchemaRef,
    codec: &CompressionCodec,
    hash_col_indices: &[usize],
    args: &Args,
    data_file: &str,
    index_file: &str,
) -> f64 {
    let partitioning = build_partitioning(
        &args.partitioning,
        args.partitions,
        hash_col_indices,
        schema,
    );

    let partitions = &[batches.to_vec()];
    let exec = ShuffleWriterExec::try_new(
        Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(partitions, Arc::clone(schema), None).unwrap(),
        ))),
        partitioning,
        codec.clone(),
        data_file.to_string(),
        index_file.to_string(),
        false,
        args.write_buffer_size,
    )
    .expect("Failed to create ShuffleWriterExec");

    let config = SessionConfig::new().with_batch_size(args.batch_size);
    let mut runtime_builder = RuntimeEnvBuilder::new();
    if let Some(mem_limit) = args.memory_limit {
        runtime_builder = runtime_builder.with_memory_limit(mem_limit, 1.0);
    }
    let runtime_env = Arc::new(runtime_builder.build().unwrap());
    let ctx = SessionContext::new_with_config_rt(config, runtime_env);
    let task_ctx = ctx.task_ctx();

    let start = Instant::now();
    let stream = exec.execute(0, task_ctx).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(collect(stream)).unwrap();
    start.elapsed().as_secs_f64()
}

fn run_shuffle_read(
    data_file: &str,
    index_file: &str,
    num_partitions: usize,
    schema: &SchemaRef,
) -> f64 {
    let start = Instant::now();

    // Read index file to get partition offsets
    let index_bytes = fs::read(index_file).expect("Failed to read index file");
    let num_offsets = index_bytes.len() / 8;
    let offsets: Vec<i64> = (0..num_offsets)
        .map(|i| {
            let bytes: [u8; 8] = index_bytes[i * 8..(i + 1) * 8].try_into().unwrap();
            i64::from_le_bytes(bytes)
        })
        .collect();

    // Read data file
    let data_bytes = fs::read(data_file).expect("Failed to read data file");

    let mut total_rows = 0usize;
    let mut total_batches = 0usize;

    // Decode each partition's data
    for p in 0..num_partitions.min(offsets.len().saturating_sub(1)) {
        let start_offset = offsets[p] as usize;
        let end_offset = offsets[p + 1] as usize;

        if start_offset >= end_offset {
            continue; // Empty partition
        }

        // Read all blocks within this partition
        let mut offset = start_offset;
        while offset < end_offset {
            // First 8 bytes: block length (little-endian u64)
            let block_length =
                u64::from_le_bytes(data_bytes[offset..offset + 8].try_into().unwrap()) as usize;
            let block_start = offset + 8;
            let block_end = block_start + block_length;
            // read_shuffle_block expects data after the 16-byte header (length + field_count)
            let block_data = &data_bytes[block_start + 8..block_end];
            let batch =
                read_shuffle_block(block_data, schema).expect("Failed to decode shuffle block");
            total_rows += batch.num_rows();
            total_batches += 1;

            offset = block_end;
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "    read back {} rows in {} batches from {} partitions",
        format_number(total_rows),
        total_batches,
        num_partitions
    );
    elapsed
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

fn describe_schema(schema: &Schema) -> String {
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
