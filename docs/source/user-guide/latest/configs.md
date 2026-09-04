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

# Comet Configuration Settings

Comet provides the following configuration settings.

## Scan Configuration Settings

<!--BEGIN:CONFIG_TABLE[scan]-->
<!--END:CONFIG_TABLE-->

## Parquet Reader Configuration Settings

<!--BEGIN:CONFIG_TABLE[parquet]-->
<!--END:CONFIG_TABLE-->

## Query Execution Settings

<!--BEGIN:CONFIG_TABLE[exec]-->
<!--END:CONFIG_TABLE-->

## Viewing Explain Plan & Fallback Reasons

These settings can be used to determine which parts of the plan are accelerated by Comet and to see why some parts of the plan could not be supported by Comet.

<!--BEGIN:CONFIG_TABLE[exec_explain]-->
<!--END:CONFIG_TABLE-->

## Shuffle Configuration Settings

For native remote shuffle, `spark.comet.shuffle.rss.maxFrameBytes` limits one complete
encoded frame, while `spark.comet.shuffle.rss.maxInFlightBytes` limits the memory reserved
by map attempts sharing an executor's remote shuffle client. The reservation includes
encoding workspace and overlapping frame copies. An ordinary uncompressed frame needs
approximately seven times its size plus schema and transport overhead. The default 512 MiB
reservation budget accommodates ordinary frames up to the default 64 MiB frame limit.
Compressed frames still need workspace for their uncompressed data. Increase the reservation
budget when larger rows or schemas need more workspace.

If a row cannot fit the remote limits, Comet materializes a replacement shuffle using its
local writer before publishing the exchange to downstream tasks. The replacement has a separate
shuffle identity, so late remote map results cannot replace local output. Subsequent fetch
failures retain Spark's normal recovery behavior.
With dynamic allocation enabled, native Celeborn shuffle also requires either
`spark.shuffle.service.enabled=true` or `spark.dynamicAllocation.shuffleTracking.enabled=true`
(the Spark default) to preserve fallback files. Otherwise exchanges retain ordinary Spark/Celeborn
shuffle, including applications that rely only on remote reliable storage or decommissioning.

<!--BEGIN:CONFIG_TABLE[shuffle]-->
<!--END:CONFIG_TABLE-->

## Memory & Tuning Configuration Settings

<!--BEGIN:CONFIG_TABLE[tuning]-->
<!--END:CONFIG_TABLE-->

## Development & Testing Settings

<!--BEGIN:CONFIG_TABLE[testing]-->
<!--END:CONFIG_TABLE-->

## Enabling or Disabling Individual Operators

<!--BEGIN:CONFIG_TABLE[enable_exec]-->
<!--END:CONFIG_TABLE-->

## Enabling or Disabling Individual Scalar Expressions

<!--BEGIN:CONFIG_TABLE[enable_expr]-->
<!--END:CONFIG_TABLE-->

## Enabling or Disabling Individual Aggregate Expressions

<!--BEGIN:CONFIG_TABLE[enable_agg_expr]-->
<!--END:CONFIG_TABLE-->
