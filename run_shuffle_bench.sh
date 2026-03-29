#!/usr/bin/env bash
set -euo pipefail

BENCH_BIN="./native/target/release/shuffle_bench"
INPUT="/opt/tpch/sf100/lineitem"
CODEC="lz4"
PARTITIONS=200
HASH_COLUMNS="0,3"
LIMIT=100000000
CSV_FILE="shuffle_bench_results.csv"

MEMORY_SIZES=("2g" "4g" "8g" "16g")

# Convert human-readable size to bytes
to_bytes() {
    local val="${1%[gGmM]}"
    local unit="${1: -1}"
    case "$unit" in
        g|G) echo $((val * 1024 * 1024 * 1024)) ;;
        m|M) echo $((val * 1024 * 1024)) ;;
        *) echo "$1" ;;
    esac
}

echo "memory_limit,memory_limit_bytes,peak_rss_bytes,peak_rss_mb,wall_time_secs,user_time_secs,sys_time_secs,bench_output" > "$CSV_FILE"

for mem in "${MEMORY_SIZES[@]}"; do
    mem_bytes=$(to_bytes "$mem")
    output_dir="/tmp/comet_shuffle_bench_${mem}"
    rm -rf "$output_dir"

    echo "=== Running with memory_limit=$mem ($mem_bytes bytes) ==="

    # Capture both time -l output (stderr) and benchmark stdout
    time_output_file=$(mktemp)
    bench_output_file=$(mktemp)

    /usr/bin/time -l "$BENCH_BIN" \
        --input "$INPUT" \
        --codec "$CODEC" \
        --partitions "$PARTITIONS" \
        --hash-columns "$HASH_COLUMNS" \
        --memory-limit "$mem_bytes" \
        --limit "$LIMIT" \
        --output-dir "$output_dir" \
        > "$bench_output_file" 2> "$time_output_file" || true

    # Parse time -l output (macOS format)
    wall_time=$(grep "real" "$time_output_file" | awk '{print $1}' || echo "N/A")
    user_time=$(grep "user" "$time_output_file" | awk '{print $1}' || echo "N/A")
    sys_time=$(grep "sys" "$time_output_file" | awk '{print $1}' || echo "N/A")
    peak_rss=$(grep "maximum resident set size" "$time_output_file" | awk '{print $1}' || echo "N/A")

    # macOS time -l reports RSS in bytes
    if [ "$peak_rss" != "N/A" ] && [ -n "$peak_rss" ]; then
        peak_rss_mb=$((peak_rss / 1024 / 1024))
    else
        peak_rss_mb="N/A"
    fi

    # Get benchmark output (last few lines, single line for CSV)
    bench_summary=$(cat "$bench_output_file" | tr '\n' ' | ' | sed 's/,/;/g' | sed 's/ | $//')

    echo "$mem,$mem_bytes,$peak_rss,$peak_rss_mb,$wall_time,$user_time,$sys_time,\"$bench_summary\"" >> "$CSV_FILE"

    echo "  Peak RSS: ${peak_rss_mb} MB"
    echo "  Wall time: ${wall_time}s"
    echo ""

    # Print full time output for reference
    echo "--- time -l output ---"
    cat "$time_output_file"
    echo "--- bench output ---"
    cat "$bench_output_file"
    echo "---"
    echo ""

    rm -f "$time_output_file" "$bench_output_file"
done

echo "Results saved to $CSV_FILE"
echo ""
cat "$CSV_FILE"
