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

Comet currently delegates to Apache DataFusion for most cast operations, and this means that the behavior is not
guaranteed to be consistent with Spark.

There is an [epic](https://github.com/apache/datafusion-comet/issues/286) where we are tracking the work to implement Spark-compatible cast expressions.

### Cast from String to Timestamp

Casting from String to Timestamp is disabled by default due to incompatibilities with Spark, including timezone
issues, and can be enabled by setting `spark.comet.castStringToTimestamp=true`. See the
[tracking issue](https://github.com/apache/datafusion-comet/issues/328) for more information.
