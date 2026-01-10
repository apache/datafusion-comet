#!/bin/bash
# Script to test different Comet shuffle configurations and compare sizes

set -e

echo "=================================="
echo "Comet Shuffle Size Analysis Script"
echo "=================================="
echo ""

# Function to run a single test with given parameters
run_test() {
  local test_name=$1
  shift
  echo ""
  echo ">>> Running: $test_name"
  echo ">>> Parameters: $*"
  ./mvnw test -q -Dtest=ShuffleSizeValidationTest -pl spark "$@" 2>&1 | grep -A 10 "Shuffle Size Validation" || true
}

# Test 1: Baseline comparison
echo ""
echo "=================================="
echo "1. BASELINE: Default Configuration"
echo "=================================="
run_test "Baseline" \
  -Dspark.comet.columnar.shuffle.batch.size=8192 \
  -Dspark.comet.exec.shuffle.compression.codec=lz4

# Test 2: Different batch sizes
echo ""
echo "=================================="
echo "2. BATCH SIZE COMPARISON"
echo "=================================="

for batch_size in 16384 32768 65536; do
  run_test "Batch size $batch_size" \
    -Dspark.comet.columnar.shuffle.batch.size=$batch_size \
    -Dspark.comet.exec.shuffle.compression.codec=lz4
done

# Test 3: Different compression codecs
echo ""
echo "=================================="
echo "3. COMPRESSION CODEC COMPARISON"
echo "=================================="

for codec in snappy zstd; do
  run_test "Codec $codec" \
    -Dspark.comet.columnar.shuffle.batch.size=8192 \
    -Dspark.comet.exec.shuffle.compression.codec=$codec
done

# Test 4: ZSTD compression levels
echo ""
echo "=================================="
echo "4. ZSTD COMPRESSION LEVEL COMPARISON"
echo "=================================="

for level in 3 6 9; do
  run_test "ZSTD level $level" \
    -Dspark.comet.columnar.shuffle.batch.size=8192 \
    -Dspark.comet.exec.shuffle.compression.codec=zstd \
    -Dspark.comet.exec.shuffle.compression.zstd.level=$level
done

# Test 5: Best configuration (large batch + high compression)
echo ""
echo "=================================="
echo "5. OPTIMIZED CONFIGURATION"
echo "=================================="

run_test "Optimized (large batch + ZSTD-6)" \
  -Dspark.comet.columnar.shuffle.batch.size=32768 \
  -Dspark.comet.exec.shuffle.compression.codec=zstd \
  -Dspark.comet.exec.shuffle.compression.zstd.level=6

echo ""
echo "=================================="
echo "Analysis Complete!"
echo "=================================="
echo ""
echo "Summary of tested configurations:"
echo "1. Baseline: batch_size=8192, codec=lz4"
echo "2. Batch sizes: 16384, 32768, 65536"
echo "3. Codecs: snappy, zstd"
echo "4. ZSTD levels: 3, 6, 9"
echo "5. Optimized: batch_size=32768, codec=zstd, level=6"
echo ""
echo "Look for the configuration with the lowest 'Overhead' percentage."
echo "Also consider the performance impact (higher compression = slower writes)."
