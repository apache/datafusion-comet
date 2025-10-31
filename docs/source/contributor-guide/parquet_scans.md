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

# Comet Parquet Scan Implementations

Comet currently has three distinct implementations of the Parquet scan operator. The configuration property
`spark.comet.scan.impl` is used to select an implementation. The default setting is `spark.comet.scan.impl=auto`, and
Comet will choose the most appropriate implementation based on the Parquet schema and other Comet configuration
settings. Most users should not need to change this setting. However, it is possible to force Comet to try and use
a particular implementation for all scan operations by setting this configuration property to one of the following
implementations.

| Implementation          | Description                                                                                                                                                                          |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `native_comet`          | This implementation provides strong compatibility with Spark but does not support complex types. This is the original scan implementation in Comet and may eventually be removed.    |
| `native_iceberg_compat` | This implementation delegates to DataFusion's `DataSourceExec` but uses a hybrid approach of JVM and native code. This scan is designed to be integrated with Iceberg in the future. |
| `native_datafusion`     | This experimental implementation delegates to DataFusion's `DataSourceExec` for full native execution. There are known compatibility issues when using this scan.                    |

The `native_datafusion` and `native_iceberg_compat` scans provide the following benefits over the `native_comet`
implementation:

- Leverages the DataFusion community's ongoing improvements to `DataSourceExec`
- Provides support for reading complex types (structs, arrays, and maps)
- Removes the use of reusable mutable-buffers in Comet, which is complex to maintain
- Improves performance

The `native_datafusion` and `native_iceberg_compat` scans share the following limitations:

- When reading Parquet files written by systems other than Spark that contain columns with the logical types `UINT_8`
  or `UINT_16`, Comet will produce different results than Spark because Spark does not preserve or understand these
  logical types. Arrow-based readers, such as DataFusion and Comet do respect these types and read the data as unsigned
  rather than signed. By default, Comet will fall back to `native_comet` when scanning Parquet files containing `byte` or `short`
  types (regardless of the logical type). This behavior can be disabled by setting
  `spark.comet.scan.allowIncompatible=true`.
- No support for default values that are nested types (e.g., maps, arrays, structs). Literal default values are supported.

The `native_datafusion` scan has some additional limitations:

- Bucketed scans are not supported
- No support for row indexes
- `PARQUET_FIELD_ID_READ_ENABLED` is not respected [#1758]
- There are failures in the Spark SQL test suite [#1545]
- Setting Spark configs `ignoreMissingFiles` or `ignoreCorruptFiles` to `true` is not compatible with Spark

[#1545]: https://github.com/apache/datafusion-comet/issues/1545
[#1758]: https://github.com/apache/datafusion-comet/issues/1758
