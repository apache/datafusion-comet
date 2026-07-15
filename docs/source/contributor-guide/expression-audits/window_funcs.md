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

# window_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## avg (window)

- 4.1.1, audited 2026-06-25: `AVG(<decimal>)` over a window falls back to Spark on Spark 4.x. Spark wraps the result as `Cast(avg(UnscaledValue(v)) / pow10 as decimal)`, a shape `CometWindowExec.convert` does not unwrap (it recognizes only `Alias(WindowExpression)` and `Alias(MakeDecimal(WindowExpression))`), so the `AvgDecimal` native window branch in `process_agg_func` is effectively dead on Spark 4.x. Results stay correct via fallback; this is a coverage gap, not a correctness divergence. Tracked by https://github.com/apache/datafusion-comet/issues/4731. Non-decimal `AVG` runs natively. Sliding-frame decimal `AVG` is additionally guarded by the same fallback as decimal `SUM` (see below) so that it cannot hit the DataFusion built-in overflow divergence once the `Cast(Divide(...))` shape is recognized.

## lag

- Supported via `CometWindowExec` (operator level), not the expression serde.

## lead

- Supported via `CometWindowExec` (operator level), not the expression serde.

## sum (window)

- 4.1.1, audited 2026-06-25: `SUM(<decimal>)` over a sliding window frame (lower bound not `UNBOUNDED PRECEDING`) falls back to Spark. The native sliding path would use DataFusion's built-in `sum`, which wraps on overflow rather than returning Spark's NULL, and overflow cannot be detected at plan time, so the whole sliding decimal `SUM` case falls back. Ever-expanding frames use Comet's overflow-aware `SumDecimal` UDAF and run natively, matching Spark including overflow-to-NULL. Bigint sliding `SUM` overflow matches Spark (both wrap) and stays native.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
