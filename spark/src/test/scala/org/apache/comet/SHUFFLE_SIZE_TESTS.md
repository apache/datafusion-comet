# Shuffle Size Comparison Tests

This directory contains tests to measure and compare shuffle file sizes between Spark's default shuffle and Comet's shuffle implementations.

## Background

Comet shuffle writes data in Arrow IPC format, which includes metadata overhead compared to Spark's Java serialization. This can result in larger shuffle files. These tests help measure the overhead and validate optimizations.

## Test Files

### 1. `ShuffleSizeValidationTest.scala`
A quick validation test that runs a simple groupBy query and compares shuffle sizes.

**Run it:**
```bash
./mvnw test -Dtest=ShuffleSizeValidationTest -pl spark
```

**Expected output:**
```
=== Shuffle Size Validation ===
Spark shuffle:      123,456 bytes  (     100 records)
Comet shuffle:      234,567 bytes  (     100 records)
Size ratio:     1.90x
Overhead:       +90.12%
```

### 2. `ShuffleSizeComparisonSuite.scala`
Comprehensive test suite that compares shuffle sizes across different scenarios:
- Simple integer groupBy
- String groupBy with high/low cardinality
- Wide tables (20 columns)
- Nested data (arrays and structs)
- Different partition counts

**Run all tests:**
```bash
./mvnw test -Dtest=ShuffleSizeComparisonSuite -pl spark
```

**Run a specific test:**
```bash
./mvnw test -Dtest=ShuffleSizeComparisonSuite#"compare shuffle sizes - simple integer groupBy" -pl spark
```

## Key Metrics

Each test reports:
- **Total bytes written**: Actual shuffle file size
- **Records written**: Number of rows shuffled
- **Bytes per record**: Average overhead per row
- **Overhead percentage**: Size increase compared to Spark

## Configuration Parameters to Experiment With

### 1. Batch Size
Controls how many rows are buffered before writing to disk. Larger batches reduce Arrow IPC overhead.

```bash
./mvnw test -Dtest=ShuffleSizeValidationTest -pl spark \
  -Dspark.comet.columnar.shuffle.batch.size=32768
```

Default: 8192
Recommended range: 8192 - 65536

### 2. Compression Codec
Different codecs have different compression ratios and speeds.

```bash
./mvnw test -Dtest=ShuffleSizeValidationTest -pl spark \
  -Dspark.comet.exec.shuffle.compression.codec=zstd
```

Options: `lz4` (default), `zstd`, `snappy`

### 3. Compression Level (ZSTD only)
Higher levels = better compression but slower.

```bash
./mvnw test -Dtest=ShuffleSizeValidationTest -pl spark \
  -Dspark.comet.exec.shuffle.compression.codec=zstd \
  -Dspark.comet.exec.shuffle.compression.zstd.level=9
```

Default: 1
Range: 1-22 (practical range: 1-9)

### 4. Dictionary Encoding (JVM shuffle only)
Controls when to use dictionary encoding for strings.

```bash
./mvnw test -Dtest=ShuffleSizeValidationTest -pl spark \
  -Dspark.comet.shuffle.preferDictionary.ratio=5.0
```

Default: 10.0 (use dictionary if total_values/distinct_values > 10)
Lower value = more aggressive dictionary encoding

## Example: Testing Different Configurations

Create a script to test multiple configurations:

```bash
#!/bin/bash

echo "Testing different batch sizes..."
for batch_size in 8192 16384 32768 65536; do
  echo "=== Batch size: $batch_size ==="
  ./mvnw test -Dtest=ShuffleSizeValidationTest -pl spark \
    -Dspark.comet.columnar.shuffle.batch.size=$batch_size
done

echo ""
echo "Testing different compression codecs..."
for codec in lz4 zstd snappy; do
  echo "=== Codec: $codec ==="
  ./mvnw test -Dtest=ShuffleSizeValidationTest -pl spark \
    -Dspark.comet.exec.shuffle.compression.codec=$codec
done

echo ""
echo "Testing different ZSTD compression levels..."
for level in 1 3 6 9; do
  echo "=== ZSTD Level: $level ==="
  ./mvnw test -Dtest=ShuffleSizeValidationTest -pl spark \
    -Dspark.comet.exec.shuffle.compression.codec=zstd \
    -Dspark.comet.exec.shuffle.compression.zstd.level=$level
done
```

## Understanding Arrow IPC Overhead

Arrow IPC format includes:
1. **Schema metadata** (~hundreds of bytes per batch)
2. **Buffer descriptors** (~24 bytes per column per batch)
3. **Alignment padding** (8-byte alignment)
4. **Dictionary metadata** (when dictionary encoding is used)

With default batch size of 8192 rows, schema overhead is repeated every 8192 rows.

**Example calculation:**
- Schema with 10 columns
- Metadata overhead: ~500 bytes per batch
- Buffer descriptors: 240 bytes per batch (10 cols × 24 bytes)
- Total overhead: ~740 bytes per batch
- With 1M rows: 1M / 8192 ≈ 122 batches × 740 bytes ≈ 90 KB metadata overhead

Increasing batch size to 32768 reduces this to ~23 KB.

## Expected Results

Based on the architecture analysis:

| Scenario | Expected Overhead |
|----------|------------------|
| Narrow table (few columns) | 50-100% |
| Wide table (20+ columns) | 30-70% |
| String-heavy (low cardinality) | 20-50% (with dictionary encoding) |
| String-heavy (high cardinality) | 80-150% |
| Nested data | 100-200% |

These can be significantly reduced with:
- Larger batch sizes
- Better compression (ZSTD with higher levels)
- Dictionary encoding for string columns

## Next Steps

After running these tests:

1. Identify which scenarios show the highest overhead
2. Experiment with different configurations
3. Consider code optimizations:
   - Reduce schema metadata size
   - Optimize buffer layout
   - Implement batch-level compression
4. Balance shuffle size vs. end-to-end performance (Comet's columnar format may still win overall)
