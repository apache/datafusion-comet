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

For Celeborn client compatibility and setup, see the [Celeborn guide](celeborn.md).
The [native remote shuffle tuning reference](tuning/celeborn.md) explains how
`spark.comet.shuffle.rss.maxFrameBytes` and `spark.comet.shuffle.rss.maxInFlightBytes`
interact, including encoding workspace and local fallback for oversized rows.

<!--BEGIN:CONFIG_TABLE[shuffle]-->
<!--END:CONFIG_TABLE-->

## Memory & Tuning Configuration Settings

<!--BEGIN:CONFIG_TABLE[tuning]-->
<!--END:CONFIG_TABLE-->

## Development & Testing Settings

These settings exist for Comet's own test suites and for debugging. They are **not covered by the
[versioning policy](../../about/versioning_policy.md#testing-and-internal-configurations-are-exempt)**:
their names, defaults, accepted values, and meanings may change in any release, including a patch
release, and any of them may be removed without a deprecation cycle. Do not set them in
production.

Comet also marks a handful of keys internal and deliberately leaves them off this page entirely.
They are maintainer escape hatches, not settings, and carry no guarantee of any kind. Absence from
this page does not by itself mean that, though: the per-expression
`spark.comet.expression.<Name>.allowIncompatible` opt-ins are documented in the
[compatibility guide](compatibility/index.md) rather than here, and the versioning policy covers
them like any other production setting.

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
