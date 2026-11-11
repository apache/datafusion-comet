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

from pathlib import Path

PARITY_GROUPS = [
    (
        "MergeRows serializer",
        [
            Path("spark/src/main/spark-3.5/org/apache/comet/serde/operator/CometMergeRows.scala"),
            Path("spark/src/main/spark-4.0/org/apache/comet/serde/operator/CometMergeRows.scala"),
        ],
        "Spark 3.5 and 4.0 intentionally share the same MergeRows serialization contract.",
    ),
    (
        "MergeRows registration shim",
        [
            Path("spark/src/main/spark-3.5/org/apache/comet/shims/ShimCometMergeRows.scala"),
            Path("spark/src/main/spark-4.0/org/apache/comet/shims/ShimCometMergeRows.scala"),
        ],
        "Spark 3.5 and 4.0 intentionally register MergeRows through identical shims.",
    ),
    (
        "MergeRows metrics shim without semantic counters",
        [
            Path("spark/src/main/spark-3.x/org/apache/comet/shims/MergeRowsMetricsShim.scala"),
            Path("spark/src/main/spark-4.0/org/apache/comet/shims/MergeRowsMetricsShim.scala"),
        ],
        "Spark 3.x and 4.0 do not publish semantic MergeRows counters.",
    ),
]


def main() -> int:
    failed = False
    for name, paths, reason in PARITY_GROUPS:
        reference = paths[0].read_bytes()
        for path in paths[1:]:
            if path.read_bytes() != reference:
                failed = True
                print(f"{name} files must remain byte-identical.")
                print(f"Reason: {reason}")
                print(f"Reference: {paths[0]}")
                print(f"Different: {path}")
    if failed:
        return 1

    print("Versioned source parity checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
