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

# Spark Version Compatibility

This page documents known issues and limitations specific to each supported Apache Spark version.

For general compatibility information that applies across all Spark versions, see the other pages in this
compatibility guide.

## Spark 3.4

Spark 3.4.3 is supported with Java 11/17 and Scala 2.12/2.13.

## Spark 3.5

Spark 3.5.8 is supported with Java 11/17 and Scala 2.12/2.13.

## Spark 4.0

Spark 4.0.2 is supported with Java 17 and Scala 2.13.

### Known Limitations

- **Collation support** ([#1947](https://github.com/apache/datafusion-comet/issues/1947),
  [#4051](https://github.com/apache/datafusion-comet/issues/4051)): Spark 4.0 introduced collation
  support. Non-default collated strings are not yet supported by Comet and will fall back to Spark.

## Spark 4.1 (Experimental)

Spark 4.1.1 is provided as experimental support with Java 17 and Scala 2.13.

```{warning}
Spark 4.1 support is experimental and intended for development and testing only. It should not be used
in production.
```

### Known Limitations

- **`NullType` columns in Parquet files**
  ([#4199](https://github.com/apache/datafusion-comet/issues/4199)): Spark encodes a `NullType`
  column as a Parquet `BOOLEAN` physical type annotated with `LogicalType::Unknown`. The Rust
  `parquet` crate that Comet depends on accepts `Unknown` only when paired with `INT32` and rejects
  any other physical type with `Parquet error: Cannot annotate Unknown from BOOLEAN for field '<name>'`.
  Any attempt to read a Parquet file that contains a `NullType` column fails at decode time before
  Comet's scan runs. Workaround: project the column away, cast it to a concrete type before
  persisting, or read the file with Comet disabled for that query.

## Spark 4.2 (Experimental)

Spark 4.2.0-preview4 is provided as experimental support with Java 17 and Scala 2.13.

```{warning}
Spark 4.2 support is experimental and targets a preview release of Spark. It is intended for early
evaluation only and should not be used in production.
```
