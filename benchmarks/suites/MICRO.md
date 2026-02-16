<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Microbenchmark Suite

Runs expression-level microbenchmarks that generate a small in-memory dataset
and time individual SQL expressions. Currently supports the string expression
benchmark (ported from `CometStringExpressionBenchmark.scala`).

## Arguments

| Argument             | Required | Default | Description                                    |
| -------------------- | -------- | ------- | ---------------------------------------------- |
| `--benchmark`        | yes      |         | `string-expressions`                           |
| `--rows`             | no       | `1024`  | Number of rows for data generation             |
| `--iterations`       | no       | `3`     | Number of timed iterations per expression      |
| `--expression`       | no       |         | Run a single expression by name                |
| `--output`           | yes      |         | Directory for results JSON                     |
| `--name`             | auto     |         | Result file prefix (auto-injected by `run.py`) |
| `--profile`          | no       |         | Enable JVM metrics profiling                   |
| `--profile-interval` | no       | `2.0`   | Profiling poll interval in seconds             |

## Examples

### String expressions with Comet

```bash
python benchmarks/run.py --engine comet --profile local \
    -- micro --benchmark string-expressions --output . --iterations 3
```

### String expressions with vanilla Spark (baseline)

```bash
python benchmarks/run.py --engine spark --profile local \
    -- micro --benchmark string-expressions --output . --iterations 3
```

### String expressions with Gluten

```bash
python benchmarks/run.py --engine gluten --profile local \
    -- micro --benchmark string-expressions --output . --iterations 3
```

### Run a single expression

```bash
python benchmarks/run.py --engine comet --profile local \
    -- micro --benchmark string-expressions --output . --expression ascii
```

### Compare results across engines

```bash
# Run each engine
for engine in comet spark gluten; do
    python benchmarks/run.py --engine $engine --profile local \
        -- micro --benchmark string-expressions --output . --iterations 3
done

# Generate comparison chart
python -m benchmarks.analysis.compare \
    comet-string-expressions-*.json spark-string-expressions-*.json \
    --labels Comet Spark --benchmark string-expressions
```

## Output Format

Results are written as JSON with the filename `{name}-{benchmark}-{timestamp_millis}.json`:

```json
{
    "engine": "datafusion-comet",
    "benchmark": "string-expressions",
    "spark_conf": { ... },
    "ascii": [0.12, 0.10, 0.08],
    "bit_length": [0.05, 0.04, 0.04],
    "lower": [0.15, 0.11, 0.07],
    ...
}
```

Expression names are top-level keys, each mapping to a list of elapsed seconds
per iteration. This format is directly compatible with `analysis/compare.py`.

## Available Expressions (string-expressions)

ascii, bit_length, btrim, chr, concat, concat_ws, contains, endswith, initcap,
instr, length, like, lower, lpad, ltrim, octet_length, regexp_replace, repeat,
replace, reverse, rlike, rpad, rtrim, space, startswith, substring, translate,
trim, upper.
