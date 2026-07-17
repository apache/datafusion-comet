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

# struct_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## named_struct

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): baseline. `CreateNamedStruct(children)` with `NoThrow`; `children` is `Seq(name1, val1, name2, val2, ...)`. Names must be foldable `StringType` and distinct; non-null. `dataType` is the `StructType` of the resulting fields. Comet routes via `CometCreateNamedStruct`, builds a `CreateNamedStruct` proto, and the native `CreateNamedStruct` expression (in `native/spark-expr/src/struct_funcs/create_named_struct.rs`) emits a `StructArray`.
- Spark 4.0.1 (audited 2026-05-27): semantics unchanged; an `override def stateful: Boolean = true` flag is added for the optimizer. Parser-level Alias unwrapping (`case (a: Alias, _) => Seq(Literal(a.name), a)`) is transparent to Comet.
- Spark 4.1.1 (audited 2026-05-27): semantics unchanged; adds `override def contextIndependentFoldable: Boolean = children.forall(_.contextIndependentFoldable)` for the new constant-folding pass; no impact on Comet conversion.
- Comet limitation: a `CreateNamedStruct` whose names contain duplicates falls back to Spark. (Spark's analyzer also tolerates duplicates at the column-name level, but the proto layer would lose them, so the fallback is the safe choice.)

## struct

- Spark 3.4.3 (audited 2026-05-27): identical to 3.5.8.
- Spark 3.5.8 (audited 2026-05-27): the SQL function `struct(a, b, c)` is built by `CreateStruct.create`, which lowers to a `CreateNamedStruct` with synthetic field names `col1`, `col2`, etc. Comet handles both `struct` and `named_struct` via the same `CometCreateNamedStruct` path; the synthetic-name and explicit-name cases behave identically.
- Spark 4.0.1 (audited 2026-05-27): same lowering; identical to 3.5.8.
- Spark 4.1.1 (audited 2026-05-27): same lowering; identical to 3.5.8.

[Spark Expression Support]: ../../user-guide/latest/expressions.md
