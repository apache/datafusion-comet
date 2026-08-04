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

# Expression Audits

This section records per-version audit notes for Spark expressions that have been audited for Comet compatibility. These are findings from auditing the Spark implementation across versions (3.4.3, 3.5.8, 4.0.1), not a statement of support status.

Two kinds of audit are recorded per expression:

- **Correctness audits** compare the Spark implementation across versions and note divergences and test-coverage gaps. Use the `audit-comet-expression` skill, which appends its findings to the relevant category page below.
- **Performance audits** record when a native expression has been performance-tuned, so contributors can see at a glance which expressions have already been optimized. Recorded as a dated `Performance (tuned ...)` line under the expression, they name the technique, the measured speedup, the linking PR, and the benchmark file. Use the `optimize-comet-expression` skill (see [Optimizing Scalar Expressions](../optimizing_expressions.md)), which appends this line after a change is benchmarked and merged.

For the authoritative list of which expressions Comet supports, see the user guide [Spark Expression Support](../../user-guide/latest/expressions.md).

```{toctree}
:maxdepth: 1

agg_funcs <agg_funcs>
array_funcs <array_funcs>
bitwise_funcs <bitwise_funcs>
collection_funcs <collection_funcs>
conditional_funcs <conditional_funcs>
conversion_funcs <conversion_funcs>
datetime_funcs <datetime_funcs>
generator_funcs <generator_funcs>
hash_funcs <hash_funcs>
json_funcs <json_funcs>
map_funcs <map_funcs>
math_funcs <math_funcs>
misc_funcs <misc_funcs>
predicate_funcs <predicate_funcs>
string_funcs <string_funcs>
struct_funcs <struct_funcs>
url_funcs <url_funcs>
window_funcs <window_funcs>
```
