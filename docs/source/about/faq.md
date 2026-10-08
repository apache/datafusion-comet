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

Not at the moment. Comet already runs ordinary Scala and Java UDFs in its pipeline without code
changes, compiling each call into a loop over a whole batch that the JVM optimizes well. See
[Scala UDF and Java UDF Support](../user-guide/latest/scala_java_udfs.md).

A prototype ([#6697](https://github.com/apache/datafusion-comet/pull/6697)) found that, once the code
generator's overheads were reduced, a vectorized rewrite of a simple function such as `x + 1` saved
only about 3 ns per row. That does not justify asking users to rewrite their functions against
Comet's relocated Arrow classes and rebuild them for every Comet release, so the effort is going into
the code generator instead.

If you have a use case that a function of one row handles poorly, such as working on the UTF-8 bytes
of strings or calling a library that processes whole batches, please describe it in
[#6694](https://github.com/apache/datafusion-comet/issues/6694). For native speed, see the
experimental [Rust UDF API](../user-guide/latest/rust_udfs.md).
