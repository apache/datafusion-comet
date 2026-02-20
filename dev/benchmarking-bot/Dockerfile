# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Dockerfile for Comet benchmarks (TPC-H and microbenchmarks)
# Includes: JDK 17, Rust, Maven, Spark 3.5.x, Python

FROM eclipse-temurin:17-jdk-jammy

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    wget \
    build-essential \
    pkg-config \
    libssl-dev \
    python3 \
    python3-pip \
    unzip \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Install newer protoc (3.15+ required for proto3 optional fields)
ARG PROTOC_VERSION=25.1
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip \
    && unzip protoc-${PROTOC_VERSION}-linux-x86_64.zip -d /usr/local \
    && rm protoc-${PROTOC_VERSION}-linux-x86_64.zip

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install Maven
ARG MAVEN_VERSION=3.9.6
RUN wget -q https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
    && tar -xzf apache-maven-${MAVEN_VERSION}-bin.tar.gz -C /opt \
    && rm apache-maven-${MAVEN_VERSION}-bin.tar.gz \
    && ln -s /opt/apache-maven-${MAVEN_VERSION} /opt/maven
ENV MAVEN_HOME=/opt/maven
ENV PATH="${MAVEN_HOME}/bin:${PATH}"

# Install Spark
ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Install PySpark
RUN pip3 install --no-cache-dir pyspark==${SPARK_VERSION}

# Clone TPC-H queries from datafusion-benchmarks
RUN git clone --depth 1 https://github.com/apache/datafusion-benchmarks.git /tmp/datafusion-benchmarks \
    && mkdir -p /opt/tpch-queries \
    && cp -r /tmp/datafusion-benchmarks/tpch/queries/* /opt/tpch-queries/ \
    && rm -rf /tmp/datafusion-benchmarks

# Create non-root user
RUN useradd -m -u 1000 bench \
    && mkdir -p /app \
    && chown bench:bench /app

# Move cargo/rustup to shared locations accessible by bench user
RUN mv /root/.cargo /opt/cargo && mv /root/.rustup /opt/rustup \
    && chown -R bench:bench /opt/cargo /opt/rustup
ENV CARGO_HOME=/opt/cargo
ENV RUSTUP_HOME=/opt/rustup
ENV PATH="/opt/cargo/bin:${PATH}"

# Set working directory
WORKDIR /app

# Create entrypoint script
COPY <<'EOF' /app/entrypoint.sh
#!/bin/bash
set -e

# Environment variables expected:
# - PR_NUMBER: GitHub PR number to benchmark
# - BENCHMARK_MODE: "tpch" (default) or "micro"
# - GITHUB_TOKEN: (optional) GitHub token to post results as PR comment
#
# For TPC-H mode:
# - TPCH_DATA: Path to TPC-H data (default: /data/tpch/sf100)
# - TPCH_QUERIES: Path to TPC-H queries (default: /opt/tpch-queries)
# - ITERATIONS: Number of iterations (default: 1)
#
# For Micro mode:
# - MICRO_BENCHMARK: Benchmark class name (e.g., "CometStringExpressionBenchmark")

# Workaround: copy rustup/cargo to writable tmpfs to avoid overlayfs
# cross-device link errors when rustup updates toolchains at runtime
if [ -d /opt/rustup ]; then
    echo "=== Preparing Rust toolchain ==="
    cp -a /opt/rustup /tmp/rustup
    cp -a /opt/cargo /tmp/cargo
    export RUSTUP_HOME=/tmp/rustup
    export CARGO_HOME=/tmp/cargo
    export PATH="/tmp/cargo/bin:$PATH"
fi

if [ -z "$PR_NUMBER" ]; then
    echo "ERROR: PR_NUMBER environment variable is required"
    exit 1
fi

BENCHMARK_MODE="${BENCHMARK_MODE:-tpch}"

echo "=== Comet Benchmark ==="
echo "PR: #${PR_NUMBER}"
echo "Mode: ${BENCHMARK_MODE}"
echo ""

# Clone the repository
echo "=== Cloning apache/datafusion-comet ==="
git clone --depth 50 https://github.com/apache/datafusion-comet.git /app/comet
cd /app/comet

# Fetch and checkout PR
echo "=== Fetching PR #${PR_NUMBER} ==="
git fetch origin pull/${PR_NUMBER}/head:pr-${PR_NUMBER}
git checkout pr-${PR_NUMBER}
COMMIT_SHA=$(git rev-parse --short HEAD)
COMMIT_MSG=$(git log -1 --pretty=format:'%s')
echo "Commit: ${COMMIT_SHA} - ${COMMIT_MSG}"
echo ""

# Build Comet
echo "=== Building Comet (make release) ==="
make release

# Find the built JAR
COMET_JAR=$(find /app/comet/spark/target -name "comet-spark-spark*.jar" -not -name "*sources*" -not -name "*javadoc*" | head -1)
if [ -z "$COMET_JAR" ]; then
    echo "ERROR: Could not find Comet JAR in spark/target/"
    ls -la /app/comet/spark/target/
    exit 1
fi
echo "Found Comet JAR: ${COMET_JAR}"
export COMET_JAR

# Create output directory
mkdir -p /app/output

if [ "$BENCHMARK_MODE" = "micro" ]; then
    ###################
    # Microbenchmark mode
    ###################
    if [ -z "$MICRO_BENCHMARK" ]; then
        echo "ERROR: MICRO_BENCHMARK environment variable is required for micro mode"
        exit 1
    fi

    echo ""
    echo "=== Running Microbenchmark: ${MICRO_BENCHMARK} ==="

    # Run the microbenchmark with output generation
    export SPARK_GENERATE_BENCHMARK_FILES=1
    cd /app/comet

    # Capture output to file
    MICRO_OUTPUT="/app/output/micro-benchmark.txt"
    make benchmark-org.apache.spark.sql.benchmark.${MICRO_BENCHMARK} 2>&1 | tee "$MICRO_OUTPUT"

    echo ""
    echo "=== Microbenchmark Complete ==="

    # Post results to GitHub PR if token is provided
    if [ -n "$GITHUB_TOKEN" ]; then
        echo ""
        echo "=== Posting Results to GitHub PR ==="

        # Extract benchmark results and create markdown comment
        COMMENT_BODY=$(python3 << PYTHON
import re
import sys

benchmark_name = "${MICRO_BENCHMARK}"
commit_sha = "${COMMIT_SHA}"
commit_msg = "${COMMIT_MSG}"

# Read the benchmark output
with open("$MICRO_OUTPUT") as f:
    content = f.read()

lines = []
lines.append(f"## Comet Microbenchmark Results: {benchmark_name}")
lines.append("")
lines.append(f"**Commit:** \`{commit_sha}\` - {commit_msg}")
lines.append("")

# Extract benchmark result tables
# Format looks like:
# OpenJDK 64-Bit Server VM ...
# AMD Ryzen ...
# Benchmark Name:   Best Time(ms)   Avg Time(ms) ...
# --------------------------------------------------------
# Spark              82             83 ...
# Comet (Scan)       79             80 ...

result_tables = []
current_table = []
in_table = False

for line in content.split('\n'):
    # Detect start of a result table (JVM info line)
    if 'OpenJDK' in line or 'Java HotSpot' in line:
        if current_table:
            result_tables.append(current_table)
        current_table = [line]
        in_table = True
        continue

    if in_table:
        # End of table detection
        if line.startswith('Running benchmark:') or line.startswith('[INFO]') or line.startswith('make'):
            if current_table:
                result_tables.append(current_table)
                current_table = []
            in_table = False
            continue

        # Skip empty lines between tables
        if not line.strip() and len(current_table) > 5:
            result_tables.append(current_table)
            current_table = []
            in_table = False
            continue

        current_table.append(line)

# Don't forget the last table
if current_table:
    result_tables.append(current_table)

# Format results
if result_tables:
    lines.append("### Benchmark Results")
    lines.append("")
    lines.append("\`\`\`")

    # Include all tables (GitHub comments can be up to 65536 chars)
    total_lines = 0
    max_lines = 500

    for table in result_tables:
        if total_lines >= max_lines:
            lines.append(f"... (truncated, {len(result_tables)} total benchmarks)")
            break
        for tline in table:
            if total_lines >= max_lines:
                break
            lines.append(tline)
            total_lines += 1
        lines.append("")  # blank line between tables
        total_lines += 1

    lines.append("\`\`\`")
else:
    # Fallback: look for lines with "Best Time" or result data
    lines.append("### Raw Output (last 80 lines)")
    lines.append("")
    lines.append("\`\`\`")
    for line in content.split('\n')[-80:]:
        if line.strip() and not line.startswith('[INFO]'):
            lines.append(line)
    lines.append("\`\`\`")

lines.append("")
lines.append("---")
lines.append("*Automated benchmark run by cometbot*")

print("\n".join(lines))
PYTHON
)

        # Post comment to PR
        RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
            -H "Authorization: token $GITHUB_TOKEN" \
            -H "Accept: application/vnd.github.v3+json" \
            -H "Content-Type: application/json" \
            "https://api.github.com/repos/apache/datafusion-comet/issues/${PR_NUMBER}/comments" \
            -d "$(jq -n --arg body "$COMMENT_BODY" '{body: $body}')")

        HTTP_CODE=$(echo "$RESPONSE" | tail -1)
        BODY=$(echo "$RESPONSE" | sed '$d')

        if [ "$HTTP_CODE" = "201" ]; then
            COMMENT_URL=$(echo "$BODY" | jq -r '.html_url')
            echo "Successfully posted comment: $COMMENT_URL"
        else
            echo "Failed to post comment (HTTP $HTTP_CODE):"
            echo "$BODY"
        fi
    else
        echo ""
        echo "GITHUB_TOKEN not set, skipping PR comment"
    fi

else
    ###################
    # TPC-H mode (default)
    ###################
    TPCH_DATA="${TPCH_DATA:-/data/tpch/sf100}"
    ITERATIONS="${ITERATIONS:-1}"
    TPCH_QUERIES="${TPCH_QUERIES:-/opt/tpch-queries}"
    BASELINE_BRANCH="${BASELINE_BRANCH:-main}"

    echo "Data path: ${TPCH_DATA}"
    echo "Queries path: ${TPCH_QUERIES}"
    echo "Iterations: ${ITERATIONS}"
    echo "Baseline branch: ${BASELINE_BRANCH}"

    # Use SPARK_MASTER from environment (default to local[*] if not set)
    SPARK_MASTER="${SPARK_MASTER:-local[*]}"
    echo "Spark master: ${SPARK_MASTER}"

    # Parse COMET_CONFIGS (comma-separated key=value pairs)
    EXTRA_CONF_ARGS=""
    if [ -n "$COMET_CONFIGS" ]; then
        echo "Extra Comet configs: ${COMET_CONFIGS}"
        IFS=',' read -ra CONFIGS <<< "$COMET_CONFIGS"
        for conf in "${CONFIGS[@]}"; do
            if [ -n "$conf" ]; then
                EXTRA_CONF_ARGS="$EXTRA_CONF_ARGS --conf $conf"
            fi
        done
    fi

    # Function to run TPC-H benchmark with current build
    run_tpch() {
        local output_dir="$1"
        local label="$2"
        local jar
        jar=$(find /app/comet/spark/target -name "comet-spark-spark*.jar" -not -name "*sources*" -not -name "*javadoc*" | head -1)
        if [ -z "$jar" ]; then
            echo "ERROR: Could not find Comet JAR in spark/target/"
            ls -la /app/comet/spark/target/
            exit 1
        fi
        echo "Found Comet JAR for ${label}: ${jar}"

        mkdir -p "$output_dir"
        cd /app/comet/dev/benchmarks

        echo ""
        echo "=== Running TPC-H Benchmark (${label}) ==="
        $SPARK_HOME/bin/spark-submit \
            --master $SPARK_MASTER \
            --jars $jar \
            --driver-class-path $jar \
            --conf spark.driver.memory=32G \
            --conf spark.driver.cores=8 \
            --conf spark.memory.offHeap.enabled=true \
            --conf spark.memory.offHeap.size=24g \
            --conf spark.driver.extraClassPath=$jar \
            --conf spark.plugins=org.apache.spark.CometPlugin \
            --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
            --conf spark.comet.exec.replaceSortMergeJoin=true \
            --conf spark.comet.expression.Cast.allowIncompatible=true \
            $EXTRA_CONF_ARGS \
            tpcbench.py \
            --name comet \
            --benchmark tpch \
            --data $TPCH_DATA \
            --queries $TPCH_QUERIES \
            --output "$output_dir" \
            --iterations $ITERATIONS \
            --format parquet
    }

    # ---- Baseline benchmark ----
    echo ""
    echo "=== Checking out baseline branch: ${BASELINE_BRANCH} ==="
    cd /app/comet
    git fetch origin ${BASELINE_BRANCH}
    git checkout origin/${BASELINE_BRANCH}
    BASELINE_SHA=$(git rev-parse --short HEAD)
    echo "Baseline commit: ${BASELINE_SHA}"

    echo "=== Building baseline (make release) ==="
    make release

    run_tpch /app/output/baseline "baseline"

    BASELINE_RESULTS=$(ls /app/output/baseline/*.json | head -1)
    echo ""
    echo "Baseline results:"
    cat "$BASELINE_RESULTS"

    # ---- PR benchmark ----
    echo ""
    echo "=== Cleaning build for PR ==="
    cd /app/comet
    make clean

    echo "=== Checking out PR #${PR_NUMBER} ==="
    git checkout pr-${PR_NUMBER}
    echo "PR commit: ${COMMIT_SHA}"

    echo "=== Building PR (make release) ==="
    make release

    run_tpch /app/output/pr "PR #${PR_NUMBER}"

    PR_RESULTS=$(ls /app/output/pr/*.json | head -1)
    echo ""
    echo "PR results:"
    cat "$PR_RESULTS"

    echo ""
    echo "=== Benchmark Complete ==="

    # Post results to GitHub PR if token is provided
    if [ -n "$GITHUB_TOKEN" ]; then
        echo ""
        echo "=== Posting Results to GitHub PR ==="

        # Parse results and create markdown comparison table
        COMMENT_BODY=$(python3 << PYTHON
import json
import os

with open("$BASELINE_RESULTS") as f:
    baseline = json.load(f)
with open("$PR_RESULTS") as f:
    pr = json.load(f)

baseline_branch = "${BASELINE_BRANCH}"
baseline_sha = "${BASELINE_SHA}"
pr_sha = "${COMMIT_SHA}"
pr_msg = "${COMMIT_MSG}"

lines = []
lines.append("## Comet TPC-H Benchmark Results")
lines.append("")
lines.append(f"**Baseline:** \`{baseline_branch}\` (\`{baseline_sha}\`)")
lines.append(f"**PR:** \`{pr_sha}\` - {pr_msg}")
lines.append(f"**Scale Factor:** SF100")
lines.append(f"**Iterations:** ${ITERATIONS}")
lines.append("")
lines.append("### Query Times")
lines.append("")
lines.append("| Query | Baseline Avg (s) | Baseline Best (s) | PR Avg (s) | PR Best (s) | Change (Avg) | Change (Best) |")
lines.append("|-------|-----------------|-------------------|-----------|------------|-------------|--------------|")

baseline_avg_total = 0.0
baseline_best_total = 0.0
pr_avg_total = 0.0
pr_best_total = 0.0
for i in range(1, 23):
    key = str(i)
    b_val = baseline.get(key)
    p_val = pr.get(key)
    if b_val is None or p_val is None:
        lines.append(f"| Q{i} | N/A | N/A | N/A | N/A | | |")
        continue
    b_list = b_val if isinstance(b_val, list) else [b_val]
    p_list = p_val if isinstance(p_val, list) else [p_val]
    b_avg = sum(b_list) / len(b_list)
    b_best = min(b_list)
    p_avg = sum(p_list) / len(p_list)
    p_best = min(p_list)
    baseline_avg_total += b_avg
    baseline_best_total += b_best
    pr_avg_total += p_avg
    pr_best_total += p_best

    def fmt_change(base, pr_val):
        if base > 0:
            pct = (pr_val - base) / base * 100
            if pct < -5:
                icon = ":green_circle:"
            elif pct > 5:
                icon = ":red_circle:"
            else:
                icon = ":white_circle:"
            return f"{icon} {pct:+.1f}%"
        return ""

    avg_change = fmt_change(b_avg, p_avg)
    best_change = fmt_change(b_best, p_best)
    lines.append(f"| Q{i} | {b_avg:.2f} | {b_best:.2f} | {p_avg:.2f} | {p_best:.2f} | {avg_change} | {best_change} |")

# Total row
def fmt_total_change(base, pr_val):
    if base > 0:
        pct = (pr_val - base) / base * 100
        if pct < -5:
            icon = ":green_circle:"
        elif pct > 5:
            icon = ":red_circle:"
        else:
            icon = ":white_circle:"
        return f"{icon} {pct:+.1f}%"
    return ""

total_avg_change = fmt_total_change(baseline_avg_total, pr_avg_total)
total_best_change = fmt_total_change(baseline_best_total, pr_best_total)
lines.append(f"| **Total** | **{baseline_avg_total:.2f}** | **{baseline_best_total:.2f}** | **{pr_avg_total:.2f}** | **{pr_best_total:.2f}** | {total_avg_change} | {total_best_change} |")

lines.append("")

# Add collapsible Spark configuration section
spark_conf = pr.get("spark_conf", {})
spark_master = spark_conf.get("spark.master", "unknown")
driver_memory = spark_conf.get("spark.driver.memory", "unknown")
driver_cores = spark_conf.get("spark.driver.cores", "unknown")
executor_cores = spark_conf.get("spark.executor.cores", driver_cores)
executor_memory = spark_conf.get("spark.executor.memory", driver_memory)
offheap_enabled = spark_conf.get("spark.memory.offHeap.enabled", "false")
offheap_size = spark_conf.get("spark.memory.offHeap.size", "unknown")
shuffle_manager = spark_conf.get("spark.shuffle.manager", "unknown")
comet_replace_smj = spark_conf.get("spark.comet.exec.replaceSortMergeJoin", "unknown")

comet_configs_env = os.environ.get("COMET_CONFIGS", "")
user_configs = {}
if comet_configs_env:
    for conf in comet_configs_env.split(","):
        if "=" in conf:
            k, v = conf.split("=", 1)
            user_configs[k] = v

lines.append("<details>")
lines.append("<summary><b>Spark Configuration</b></summary>")
lines.append("")
lines.append("| Setting | Value |")
lines.append("|---------|-------|")
lines.append(f"| Spark Master | \`{spark_master}\` |")
lines.append(f"| Driver Memory | {driver_memory} |")
lines.append(f"| Driver Cores | {driver_cores} |")
lines.append(f"| Executor Memory | {executor_memory} |")
lines.append(f"| Executor Cores | {executor_cores} |")
lines.append(f"| Off-Heap Enabled | {offheap_enabled} |")
lines.append(f"| Off-Heap Size | {offheap_size} |")
lines.append(f"| Shuffle Manager | \`{shuffle_manager.split('.')[-1]}\` |")
lines.append(f"| Comet Replace SMJ | {comet_replace_smj} |")

if user_configs:
    lines.append("")
    lines.append("**User-specified configs:**")
    lines.append("")
    for k, v in user_configs.items():
        lines.append(f"| {k} | \`{v}\` |")

lines.append("")
lines.append("</details>")
lines.append("")
lines.append("---")
lines.append("*Automated benchmark run by cometbot*")

print("\n".join(lines))
PYTHON
)

        # Post comment to PR
        RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
            -H "Authorization: token $GITHUB_TOKEN" \
            -H "Accept: application/vnd.github.v3+json" \
            -H "Content-Type: application/json" \
            "https://api.github.com/repos/apache/datafusion-comet/issues/${PR_NUMBER}/comments" \
            -d "$(jq -n --arg body "$COMMENT_BODY" '{body: $body}')")

        HTTP_CODE=$(echo "$RESPONSE" | tail -1)
        BODY=$(echo "$RESPONSE" | sed '$d')

        if [ "$HTTP_CODE" = "201" ]; then
            COMMENT_URL=$(echo "$BODY" | jq -r '.html_url')
            echo "Successfully posted comment: $COMMENT_URL"
        else
            echo "Failed to post comment (HTTP $HTTP_CODE):"
            echo "$BODY"
        fi
    else
        echo ""
        echo "GITHUB_TOKEN not set, skipping PR comment"
    fi
fi
EOF

RUN chmod +x /app/entrypoint.sh

USER bench
ENTRYPOINT ["/app/entrypoint.sh"]
