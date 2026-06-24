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

# String Expressions

<!--BEGIN:EXPR_COMPAT[string]-->

## BitLength

The following cases are not supported by Comet:

- `BinaryType` input is not supported

## Concat

By default, Comet runs a Spark-compatible implementation of `Concat`. Set `spark.comet.expression.Concat.allowIncompatible=true` to use Comet's faster native implementation instead, which has the following differences from Spark:

- concat does not support non-UTF8_BINARY collations (https://github.com/apache/datafusion-comet/issues/2190)

The following cases are not supported by Comet:

- CONCAT supports only string input parameters

## GetJsonObject

By default, Comet runs a Spark-compatible implementation of `GetJsonObject`. Set `spark.comet.expression.GetJsonObject.allowIncompatible=true` to use Comet's faster native implementation instead, which has the following differences from Spark:

- Spark allows single-quoted JSON and unescaped control characters which Comet does not support

## InitCap

By default, Comet runs a Spark-compatible implementation of `InitCap`. Set `spark.comet.expression.InitCap.allowIncompatible=true` to use Comet's faster native implementation instead, which has the following differences from Spark:

- Treats hyphen as a word separator (e.g. `robert rose-smith` produces `Robert Rose-Smith` instead of Spark's `Robert Rose-smith`) (https://github.com/apache/datafusion-comet/issues/1052)

## Left

The following cases are not supported by Comet:

- Only supports `BinaryType` and `StringType` input
- The length argument must be a literal value

## Length

The following cases are not supported by Comet:

- `BinaryType` input is not supported

## Lower

By default, Comet runs a Spark-compatible implementation of `Lower`. Set `spark.comet.caseConversion.enabled=true` to use Comet's faster native implementation instead, which has the following differences from Spark:

- Results can vary depending on locale and character set

## OctetLength

The following cases are not supported by Comet:

- `BinaryType` input is not supported

## RLike

By default, Comet runs a Spark-compatible implementation of `RLike`. Set `spark.comet.expression.RLike.allowIncompatible=true` to use Comet's faster native implementation instead, which has the following differences from Spark:

- Uses Rust regexp engine, which has different behavior to Java regexp engine

## RegExpReplace

By default, Comet runs a Spark-compatible implementation of `RegExpReplace`. Set `spark.comet.expression.RegExpReplace.allowIncompatible=true` to use Comet's faster native implementation instead, which has the following differences from Spark:

- Regexp pattern may not be compatible with Spark

## Reverse

By default, Comet runs a Spark-compatible implementation of `Reverse`. Set `spark.comet.expression.Reverse.allowIncompatible=true` to use Comet's faster native implementation instead, which has the following differences from Spark:

- reverse on array containing binary is not supported
- reverse does not support non-UTF8_BINARY collations (https://github.com/apache/datafusion-comet/issues/2190)

## Right

The following cases are not supported by Comet:

- Only supports `StringType` input

## StringLPad

The following cases are not supported by Comet:

- Scalar values are not supported for the `str` argument.
- Only scalar values are supported for the `pad` argument.

## StringRPad

The following cases are not supported by Comet:

- Scalar values are not supported for the `str` argument.
- Only scalar values are supported for the `pad` argument.

## StringRepeat

The following differences from Spark are always present and do not require any additional configuration:

- A negative argument for the number of times to repeat throws an exception instead of returning an empty string as Spark does

## StringReplace

By default, Comet runs a Spark-compatible implementation of `StringReplace`. Set `spark.comet.expression.StringReplace.allowIncompatible=true` to use Comet's faster native implementation instead, which has the following differences from Spark:

- Native and JVM results may differ for some inputs

## StringSplit

By default, Comet runs a Spark-compatible implementation of `StringSplit`. Set `spark.comet.expression.StringSplit.allowIncompatible=true` to use Comet's faster native implementation instead, which has the following differences from Spark:

- Regex engine differences between Java and Rust

## StringTranslate

The following incompatibilities cause `StringTranslate` to fall back to Spark by default. Set `spark.comet.expression.StringTranslate.allowIncompatible=true` to enable Comet acceleration despite these differences.

- DataFusion's translate iterates over Unicode graphemes (Spark uses code points) and substitutes U+0000 instead of treating it as a deletion sentinel

## Upper

By default, Comet runs a Spark-compatible implementation of `Upper`. Set `spark.comet.caseConversion.enabled=true` to use Comet's faster native implementation instead, which has the following differences from Spark:

- Results can vary depending on locale and character set
<!--END:EXPR_COMPAT-->
