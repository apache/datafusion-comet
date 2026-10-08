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

# Frequently Asked Questions

## Does Comet plan to add an API for vectorized Java/Scala UDFs similar to the Rust UDF API?

Not at the moment. Comet already runs ordinary Scala and Java UDFs in its pipeline with no code
changes, as described in [Scala UDF and Java UDF Support](../user-guide/latest/scala_java_udfs.md).
Its code generator compiles each call into a loop over a whole batch of rows, and the JVM inlines a
simple function into that loop.

We built a prototype of a vectorized API, in which a UDF receives each argument as an Arrow vector
and returns an Arrow vector for the whole batch
([#6697](https://github.com/apache/datafusion-comet/pull/6697)). Once some overheads in the code
generator were removed, rewriting a simple function of primitive values, such as `x + 1`, in
vectorized form saved about 3 nanoseconds per row. That was about a fifth of the time of a query
that did little besides scan one column. The gain did not justify what the API would ask of users:

- writing each function a second time, against Comet's interfaces rather than Spark's
- compiling it against the copy of Arrow Java that Comet relocates into its own jar, and rebuilding
  it for every Comet release
- handling nulls and Arrow buffer layouts by hand, with Arrow Java's bounds checks turned off

Improving the code generator speeds up existing UDFs without any rewrite, so that is where the work
is going instead.

A vectorized function could still pay off for work that a function of one row cannot express, such
as processing the UTF-8 bytes of a string column instead of one `String` per row, setup done once
per batch, or one call per batch into a library that works on batches. If you have a use case like
that, please describe it in [#6694](https://github.com/apache/datafusion-comet/issues/6694). A
function that needs native speed can be written in Rust with the experimental
[Rust UDF API](../user-guide/latest/rust_udfs.md).
