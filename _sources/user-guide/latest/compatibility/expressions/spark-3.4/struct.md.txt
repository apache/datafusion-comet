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

# Struct Expressions

<!--BEGIN:EXPR_COMPAT[struct]-->

## CreateNamedStruct

The following cases are not supported by Comet:

- `CreateNamedStruct` with duplicate field names is not supported

## JsonToStructs

By default, Comet runs a Spark-compatible implementation of `JsonToStructs`. Set `spark.comet.expression.JsonToStructs.allowIncompatible=true` to use Comet's faster native implementation instead, which has the following differences from Spark:

- Partially implemented and not comprehensively tested

## StructsToCsv

The following incompatibilities cause `StructsToCsv` to fall back to Spark by default. Set `spark.comet.expression.StructsToCsv.allowIncompatible=true` to enable Comet acceleration despite these differences.

- Date, Timestamp, TimestampNTZ, and Binary data types may produce different results (https://github.com/apache/datafusion-comet/issues/3232)

The following cases are not supported by Comet:

- Complex types (arrays, maps, structs) in the schema are not supported

## StructsToJson

By default, Comet runs a Spark-compatible implementation of `StructsToJson`. Set `spark.comet.expression.StructsToJson.allowIncompatible=true` to use Comet's faster native implementation instead, which has the following differences from Spark:

- Does not support `+Infinity` and `-Infinity` for numeric types (float, double). (https://github.com/apache/datafusion-comet/issues/3016)
<!--END:EXPR_COMPAT-->
