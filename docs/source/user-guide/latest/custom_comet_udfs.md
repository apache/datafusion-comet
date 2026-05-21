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

# Custom Comet UDFs

Comet lets you register a user-supplied vectorized implementation of a Spark UDF. When the
registered name matches a `ScalaUDF` in your query, Comet routes the call to your vectorized
implementation on the native path instead of running Spark's row-at-a-time function. Other Comet
operators in the same plan stay native.

This is a more direct alternative to the [Janino codegen path](scala_java_udfs.md): you supply
the columnar implementation yourself, work with Arrow vectors directly, and Comet ships the data
to your code through the existing native UDF bridge.

This feature is experimental. The `CometUDF` trait and `CometUDFRegistry` API carry the
`@org.apache.spark.annotation.Unstable` annotation and may change.

## API

```scala
package com.example

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, ValueVector}
import org.apache.comet.udf.CometUDF

class PlusOneUdf extends CometUDF {
  private val allocator = new RootAllocator()

  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    val in = inputs(0).asInstanceOf[IntVector]
    val out = new IntVector("out", allocator)
    out.allocateNew(numRows)
    var i = 0
    while (i < numRows) {
      if (in.isNull(i)) out.setNull(i) else out.set(i, in.get(i) + 1)
      i += 1
    }
    out.setValueCount(numRows)
    out
  }
}
```

Register both the Spark UDF (for type binding and the row-based fallback) and the Comet UDF:

```scala
import org.apache.comet.udf.CometUDFRegistry

spark.udf.register("plus_one", (x: Int) => x + 1)
CometUDFRegistry.register("plus_one", classOf[com.example.PlusOneUdf])
```

Use it from SQL or DataFrame as you would any Spark UDF; Comet picks up the registered class at
plan time:

```sql
SELECT plus_one(value) FROM t
```

## Contract

The `CometUDF` trait:

```scala
trait CometUDF {
  def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector
}
```

- Vector arguments arrive at the row count of the current batch.
- Scalar (literal-folded) arguments arrive as length-1 vectors and must be read at index 0.
- The returned vector's length must match `numRows`.
- Implementations must have a public no-arg constructor.
- A fresh instance is created per Spark task attempt per class and reused for every batch within
  that task; instance fields may hold per-task state. Instances are dropped at task completion.
  Do not hold state that must persist across tasks.
- At most one thread calls `evaluate` on a given instance at a time, so per-task state does not
  require synchronization.

## Routing precedence

For each `ScalaUDF` Comet encounters:

1. If the UDF name has a `CometUDF` registered via `CometUDFRegistry`, Comet routes the call to
   the registered class.
2. Otherwise, if `spark.comet.exec.scalaUDF.codegen.enabled=true`, Comet uses the Janino codegen
   dispatcher.
3. Otherwise, the enclosing operator falls back to Spark.

## Cluster deployment

The class is loaded on each executor via the task's context classloader, so the jar containing
your `CometUDF` must be on the executor classpath (e.g. `spark.jars`, `--jars`, or shaded into
the application). Registration calls themselves are driver-side; executors receive the class
name through the serialized plan.

## Limitations

- Aggregate, table, Python, Pandas, and Hive UDFs are out of scope.
- The matching Spark UDF must be registered separately; without it, the function name will not
  bind during analysis.
- Return type and nullability come from the registered Spark UDF, not from the `CometUDF` class.
  Make sure your vectorized implementation produces a vector compatible with the declared
  Spark return type.
