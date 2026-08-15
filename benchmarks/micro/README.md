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

# Micro Benchmark Runner

`run.py` installs the prerequisites, builds Comet in release mode, runs the micro benchmark suites
in `spark/src/test/scala/org/apache/spark/sql/benchmark`, and collects the results into
`benchmarks/results/micro` ready to be published in a pull request.

It is intended for a dedicated machine, typically an EC2 instance. See
[Micro Benchmarking on AWS EC2](../../docs/source/contributor-guide/benchmarking_micro_ec2.md) for
the full workflow.

The script has no dependencies beyond Python 3.9 and can be downloaded on its own, before the
repository is cloned:

```shell
curl -sSLO https://raw.githubusercontent.com/apache/datafusion-comet/main/benchmarks/micro/run.py
python3 run.py all
```

Subcommands:

| Command   | Purpose                                                        |
| --------- | -------------------------------------------------------------- |
| `setup`   | Install prerequisites, clone or update the repo, build release |
| `run`     | Run the suites, one JVM per suite                              |
| `collect` | Copy results into `benchmarks/results/micro` with run metadata |
| `publish` | Commit the results and optionally open a pull request          |
| `all`     | `setup` + `run` + `collect`                                    |

Run `python3 run.py <command> --help` for the options of each command.
