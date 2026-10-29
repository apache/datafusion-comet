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

# Expression Tuning

## Lambdas and Higher-Order Functions

A higher-order function takes a lambda: `filter(arr, x -> ...)`, `transform`, `exists`, `forall`, `aggregate`,
`zip_with` and `map_filter`. How fast Comet runs one depends on whether it can run the lambda natively.

| Lambda                                                                                                                                                                    | How Comet runs it                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `filter(arr, x -> x IS NOT NULL)` (what `array_compact` expands to)                                                                                                       | Natively, as `array_compact`.                                                                                                                                                    |
| `filter` with a one-parameter lambda whose body uses only natively supported expressions, over an array of primitive, decimal, string, binary, date or timestamp elements | Natively. The lambda runs once over all the elements of a batch, vectorized, instead of once per element.                                                                        |
| Any other lambda, and every other higher-order function                                                                                                                   | Through the [JVM codegen dispatcher](../scala_java_udfs.md). Spark's interpreter evaluates the lambda for each element inside a Comet kernel, so expect Spark's speed, not more. |

The dispatcher has no advantage over Spark for a lambda, because Spark also evaluates a lambda one element at a time.
It adds the cost of handing each batch to the JVM and back. The rest of the query stays native either way.

To get the most out of Comet when a query uses lambdas:

- **Prefer a built-in array function to a lambda.** Most of them run natively with no per-element evaluation, for
  example `array_compact`, `array_remove`, `array_distinct` and `array_position`. See the
  [Supported Expressions](../expressions.md) tables for the caveats of each.
  `array_compact(arr)` returns what `filter(arr, x -> x IS NOT NULL)` does for any array, and
  `array_remove(array_compact(arr), '')` returns what `filter(arr, x -> nvl(x, '') != '')` does for an array of
  strings with the default collation.
- **Do not test a numeric array for nulls with a string comparison.** With ANSI mode off, Spark casts every element
  of `filter(int_arr, x -> nvl(x, '') != '')` to a string to compare it with `''`; with ANSI mode on the query does
  not analyze. Write `x IS NOT NULL`, or call `array_compact`.
- **Keep the lambda body to natively supported expressions.** A body that calls a regular expression function, a
  UDF or another lambda cannot be split from the dispatcher, so the whole lambda runs there. Use
  [`spark.comet.explain.codegen.enabled`](../understanding-comet-plans.md#sparkcometexplaincodegenenabled) to see which
  expressions of a query went through the dispatcher.
- **Under ANSI mode, keep `AND`, `OR` and `IN` away from operations that can raise.** Spark evaluates them lazily, so
  `x != 0 AND 100 / x > 1` never divides by zero, but the native lambda evaluates every operand for every element. A
  body that combines them with an operation that can raise, such as a division, an addition that can overflow or a
  cast, stays on the dispatcher. `CASE WHEN`, `IF` and `COALESCE` are lazy natively too, so
  `CASE WHEN x = 0 THEN false ELSE 100 / x > 1 END` runs natively.
- **Use a lambda of one parameter.** `filter(arr, (x, i) -> i > 0)`, which also receives the element's index, runs
  through the dispatcher.

`spark.comet.exec.nativeLambda.enabled=false` makes Comet run every lambda through the dispatcher.

### Example

`CometHigherOrderFunctionBenchmark` filters 1 million rows of ten-element arrays, in which every fifth element is
`NULL`. These are the best times of the Spark baseline and of Comet on an Apple M-series laptop, with
`spark.comet.exec.nativeLambda.enabled` off and at its default:

| Query                                                       | Spark  | Comet, nativeLambda off | Comet, default |
| ----------------------------------------------------------- | ------ | ----------------------- | -------------- |
| `filter(strs, x -> nvl(x, '') != '')` (string array)        | 616 ms | 547 ms                  | 307 ms         |
| `array_remove(array_compact(strs), '')` (same result)       | 587 ms | 252 ms                  | 252 ms         |
| `filter(ints, x -> nvl(x, '') != '')` (integer array)       | 508 ms | 456 ms                  | 331 ms         |
| `filter(ints, x -> x IS NOT NULL)`                          | 286 ms | 124 ms                  | 125 ms         |
| `filter(ints, x -> x > threshold)`, `threshold` is a column | 346 ms | 335 ms                  | 110 ms         |

Rewriting a lambda to built-in functions is still the fastest form, but a native lambda gets most of the way there.
