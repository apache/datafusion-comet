<!---
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

# Remote Shuffle with Celeborn

Applications using Apache Celeborn can use Comet's composite shuffle manager to retain ordinary
Spark/Celeborn shuffle while accelerating other operators with Comet.

Native shuffle also requires reliable completion tracking for in-flight payloads. Released Celeborn
0.6.0 and 0.7.0 clients do not provide the required guarantee, so these versions retain ordinary
Spark/Celeborn shuffle even when `spark.comet.shuffle.mode=native`. Native shuffle support for
these clients requires a safe Celeborn push-completion API. The following settings request
native shuffle when the client passes Comet's compatibility checks:

```properties
spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager
spark.comet.exec.enabled=true
spark.comet.shuffle.enabled=true
spark.comet.shuffle.mode=native
spark.celeborn.client.spark.stageRerun.enabled=true
```

Set the shuffle manager and Celeborn configuration before creating the Spark context. Celeborn is
an optional application dependency, not bundled with Comet: provide a compatible Celeborn Spark
client matching the application's Spark and Scala versions on both the driver and executors,
alongside the Comet JAR. Keep the application's existing Celeborn service configuration.

Native Celeborn shuffle requires explicit `spark.comet.shuffle.mode=native`. The default `auto`
mode and `jvm` mode retain ordinary Spark shuffle through the delegated Celeborn manager; they do
not select Comet's JVM columnar shuffle. Comet execution can still accelerate other operators.
With native mode enabled, exchanges with unsupported children, data types, or partitioning also
retain the ordinary Spark/Celeborn shuffle path. The local `CometShuffleManager` keeps its existing
native-to-columnar fallback behavior.

Stage reruns must remain enabled so failed or ambiguous map attempts can recover through a new
Celeborn shuffle generation. Native RSS does not support `spark.io.encryption.enabled=true`;
encrypted applications retain ordinary Spark/Celeborn shuffle instead. Do not disable encryption
required by the application to enable native RSS. Eligibility uses the manager's application-time
configuration, including Celeborn's effective defaults and legacy aliases, rather than later SQL
session overrides.

Celeborn's fallback policy remains application-owned. An effective
`spark.celeborn.client.spark.shuffle.fallback.policy=ALWAYS`, or an `AUTO` partition-count
threshold that the exchange reaches, keeps the exchange on Spark. Worker availability and quota
can still cause Celeborn to choose local fallback during registration. Once an exchange has been
planned as native, Comet rejects that local handle and fails the registration: native Arrow frames
cannot be passed to Spark's ordinary local shuffle writer. Set
`spark.celeborn.client.spark.shuffle.fallback.policy=NEVER` only if the application also wants
Celeborn to prohibit local fallback for ordinary Spark shuffles.

Native frames retain Comet's configured compression; the raw Celeborn client path bypasses
Celeborn's additional row compression and decompression. Use
`spark.comet.shuffle.rss.maxFrameBytes` and `spark.comet.shuffle.rss.maxInFlightBytes` to bound
encoded frame size and executor-side push admission. The defaults are 64 MiB and 512 MiB,
respectively. Admission includes Arrow encoding workspace as well as overlapping native, JNI,
and client frame copies. An uncompressed frame needs roughly seven times its size plus schema
and codec overhead. Compression reduces the transmitted bytes but still needs uncompressed
encoding workspace.

Comet splits large batches between rows. If a single row, its schema, or its encoding workspace
cannot fit the remote limits, Comet abandons the remote shuffle and materializes a replacement
using its local shuffle writer before downstream tasks can consume the exchange. The replacement
has a separate shuffle and scheduling identity, so late remote results cannot overwrite or skip
local map output, and remote stage failures cannot abort the replacement. Independent exchanges
can materialize concurrently; readers wait for their storage decisions before execution. Runtime
output statistics count only the selected destination. All reads and retries for the replacement
use local files and Spark's block transfer
service, including normal recovery after later fetch failures. Native operators and Comet's
Arrow shuffle format are preserved, and remote admission limits remain enforced. Once remote
output has been published, subsequent failures use the existing Spark/Celeborn recovery path;
Comet does not change that shuffle's destination. Local fallback uses executor disk. When `spark.dynamicAllocation.enabled=true`, native Celeborn shuffle requires
`spark.shuffle.service.enabled=true` or `spark.dynamicAllocation.shuffleTracking.enabled=true`
(the Spark default) so those files remain available. Applications using dynamic allocation with
both settings disabled retain ordinary Spark/Celeborn shuffle, even if remote reliable storage or
decommissioning enables dynamic allocation. Executor shutdown preserves fallback files for the
external shuffle service; explicit shuffle unregister retains the normal local cleanup behavior.
AQE reducer coalescing and mapper-range reads are supported, but Celeborn physical-skew chunk reads
are not.
