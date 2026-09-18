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

# Micro Benchmark Results

This directory holds published results from the Comet micro benchmark suites in
`spark/src/test/scala/org/apache/spark/sql/benchmark`.

`RUN-INFO.md` records the machine, the Comet commit, and the per-suite status of the run that
produced the results files next to it. Results are only comparable across runs on the same
instance type, so replace the whole directory in one commit rather than updating individual files.

Only suites that ran to completion are published. A suite that failed or hit its timeout writes a
truncated results file, which would present a partial measurement set as a baseline, so those files
are left out. `RUN-INFO.md` still lists such suites with their `failed` or `timeout` status and
`not published` in place of a results file, so the gap is visible and can be filled by a later run.
A suite whose results are dropped for any other reason is recorded the same way.

Results are produced and collected with `benchmarks/micro/run.py`. See
[Micro Benchmarking on AWS EC2](../../../docs/source/contributor-guide/benchmarking_micro_ec2.md)
for the full workflow.

Note that `spark/benchmarks` is the working directory that Spark's benchmark framework writes to
and is not tracked by git. Only the files copied here by `run.py collect` are published.
