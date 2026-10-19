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

See the [Celeborn guide](../celeborn.md) for setup and client compatibility. The settings
below apply only to Comet's native remote shuffle, which is unavailable with the currently
released Celeborn 0.6.x and 0.7.x clients. They do not tune Celeborn's existing Spark shuffle
implementation.

| Setting                                    | Default | Purpose                                                                                             |
| ------------------------------------------ | ------- | --------------------------------------------------------------------------------------------------- |
| `spark.comet.shuffle.rss.maxFrameBytes`    | 64 MiB  | Maximum size of one complete encoded frame. The admission budget can reduce the effective limit.    |
| `spark.comet.shuffle.rss.maxInFlightBytes` | 512 MiB | Shared memory admission budget for map attempts using the same executor-side remote shuffle client. |

Admission includes Arrow encoding workspace and overlapping native, JNI, and client frame
copies. An ordinary uncompressed frame needs roughly seven times its size plus schema and
codec overhead. The default 512 MiB budget accommodates ordinary frames up to the default
64 MiB frame limit. Compression reduces transmitted bytes but still needs workspace for the
uncompressed data. This budget bounds shuffle-write admission, not total executor memory.

Comet splits batches between rows. If a row, schema, or encoding workspace cannot fit, Comet
replaces the exchange with its local native shuffle writer before downstream tasks consume
the output.
Increase the limiting setting if larger rows need to stay on the remote path, allowing for
encoding workspace as well as the encoded frame. Raising only `maxFrameBytes` may not help
when `maxInFlightBytes` is the limiting budget. Larger budgets also increase potential
executor memory use.

Native frames use Comet's [shuffle compression settings](shuffle.md#shuffle-compression).
The raw Celeborn path bypasses Celeborn's additional row compression and decompression, so
frames are not compressed twice.
