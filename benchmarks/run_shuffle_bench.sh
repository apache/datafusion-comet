#!/bin/bash
# Run shuffle benchmarks: buffered vs immediate, varying partition counts, 4GB memory limit

set -e

BENCH_BIN="./native/target/release/shuffle_bench"
INPUT="/opt/tpch/sf100/lineitem"
LIMIT=100000000
CODEC="lz4"
HASH_COLUMNS="0,3"
MEMORY_LIMIT=4294000000
RESULTS_DIR="/tmp/shuffle_bench_results_partitions"

mkdir -p "$RESULTS_DIR"

PARTITIONS=(200 400 800)
MODES=("buffered" "immediate")

CSV_FILE="$RESULTS_DIR/results.csv"
echo "mode,partitions,peak_memory_bytes,throughput_rows_per_sec,avg_time_secs" > "$CSV_FILE"

for mode in "${MODES[@]}"; do
  for parts in "${PARTITIONS[@]}"; do
    echo "=== Running: mode=$mode partitions=$parts ==="
    OUT_FILE="$RESULTS_DIR/${mode}_${parts}.txt"
    TIME_FILE="$RESULTS_DIR/${mode}_${parts}_time.txt"

    /usr/bin/time -l "$BENCH_BIN" \
      --input "$INPUT" \
      --limit "$LIMIT" \
      --partitions "$parts" \
      --codec "$CODEC" \
      --hash-columns "$HASH_COLUMNS" \
      --memory-limit "$MEMORY_LIMIT" \
      --mode "$mode" \
      > "$OUT_FILE" 2> "$TIME_FILE"

    peak_mem=$(grep "maximum resident set size" "$TIME_FILE" | awk '{print $1}')
    throughput=$(grep "throughput:" "$OUT_FILE" | sed 's/.*throughput: *//; s/ rows\/s.*//' | tr -d ',')
    avg_time=$(grep "avg time:" "$OUT_FILE" | sed 's/.*avg time: *//; s/s$//')

    echo "  Peak memory: $peak_mem bytes, Throughput: $throughput rows/s, Avg time: ${avg_time}s"
    echo "$mode,$parts,$peak_mem,$throughput,$avg_time" >> "$CSV_FILE"
  done
done

echo ""
echo "Results saved to $CSV_FILE"
cat "$CSV_FILE"
