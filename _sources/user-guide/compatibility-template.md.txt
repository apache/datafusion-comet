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

# Compatibility Guide

Comet aims to provide consistent results with the version of Apache Spark that is being used.

This guide offers information about areas of functionality where there are known differences.

## ANSI mode

Comet currently ignores ANSI mode in most cases, and therefore can produce different results than Spark. By default,
Comet will fall back to Spark if ANSI mode is enabled. To enable Comet to accelerate queries when ANSI mode is enabled,
specify `spark.comet.ansi.enabled=true` in the Spark configuration. Comet's ANSI support is experimental and should not
be used in production.

There is an [epic](https://github.com/apache/datafusion-comet/issues/313) where we are tracking the work to fully implement ANSI support.

## Cast

Cast operations in Comet fall into three levels of support:

- **Compatible**: The results match Apache Spark
- **Incompatible**: The results may match Apache Spark for some inputs, but there are known issues where some inputs
  will result in incorrect results or exceptions. The query stage will fall back to Spark by default. Setting
  `spark.comet.cast.allowIncompatible=true` will allow all incompatible casts to run natively in Comet, but this is not
  recommended for production use.
- **Unsupported**: Comet does not provide a native version of this cast expression and the query stage will fall back to
  Spark.

The following table shows the current cast operations supported by Comet. Any cast that does not appear in this
table (such as those involving complex types and timestamp_ntz, for example) are not supported by Comet.

<!--CAST_TABLE-->
