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

# Custom JVM UDFs

Comet supports user-defined functions (UDFs) that operate on Arrow columnar data via the JVM UDF framework. This
allows UDFs to process entire batches of data at once rather than row-at-a-time, providing significant performance
improvements while maintaining full Spark compatibility.

## Overview

When Comet encounters a registered Spark UDF during query planning, it can route the UDF to a vectorized
JVM implementation that operates on Arrow vectors. This avoids the overhead of falling back to Spark's
row-at-a-time execution while keeping the implementation in Java/Scala.

The framework consists of:

- **`CometUDF`**: a trait your UDF class must implement, declaring its name, return type, optional input
  types, and the vectorized `evaluate` method.
- **`CometUdfRegistry`**: a registry that introspects your `CometUDF` class to record metadata for the serde
  layer.
- **`CometUdfBridge`**: the JNI bridge that native execution uses to invoke your UDF (no user interaction
  needed).

## Writing a CometUDF

Implement the `org.apache.comet.udf.CometUDF` trait. Comet relocates Apache Arrow into
`org.apache.comet.shaded.arrow.*` to avoid version conflicts with Spark's bundled Arrow,
so your implementation must import Arrow types from the shaded package. This is the
same package that the published `comet-spark` JAR exposes on your classpath.

### Java

```java
import org.apache.comet.shaded.arrow.vector.IntVector;
import org.apache.comet.shaded.arrow.vector.BitVector;
import org.apache.comet.shaded.arrow.vector.ValueVector;
import org.apache.comet.udf.CometUDF;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class IsPositiveUdf implements CometUDF {

    @Override
    public String name() { return "is_positive"; }

    @Override
    public DataType returnType() { return DataTypes.BooleanType; }

    @Override
    public boolean nullable() { return true; }

    @Override
    public ValueVector evaluate(ValueVector[] inputs) {
        IntVector input = (IntVector) inputs[0];
        int rowCount = input.getValueCount();

        BitVector result = new BitVector("result",
            org.apache.comet.package$.MODULE$.CometArrowAllocator());
        result.allocateNew(rowCount);

        for (int i = 0; i < rowCount; i++) {
            if (input.isNull(i)) {
                result.setNull(i);
            } else {
                result.set(i, input.get(i) > 0 ? 1 : 0);
            }
        }
        result.setValueCount(rowCount);
        return result;
    }
}
```

### Scala

```scala
import org.apache.comet.shaded.arrow.vector.{BitVector, IntVector, ValueVector}
import org.apache.comet.CometArrowAllocator
import org.apache.comet.udf.CometUDF
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType}

class IsPositiveUdf extends CometUDF {
  override def name: String = "is_positive"
  override def returnType: DataType = BooleanType
  override def nullable: Boolean = true

  // Optional: declare only if you plan to use registerColumnarOnly.
  override def inputTypes: Seq[DataType] = Seq(IntegerType)

  override def evaluate(inputs: Array[ValueVector]): ValueVector = {
    val input = inputs(0).asInstanceOf[IntVector]
    val rowCount = input.getValueCount
    val result = new BitVector("result", CometArrowAllocator)
    result.allocateNew(rowCount)
    var i = 0
    while (i < rowCount) {
      if (input.isNull(i)) result.setNull(i)
      else result.set(i, if (input.get(i) > 0) 1 else 0)
      i += 1
    }
    result.setValueCount(rowCount)
    result
  }
}
```

Key requirements:

- The class must have a **public no-arg constructor**.
- Arrow types must be imported from `org.apache.comet.shaded.arrow.*` (the relocated package).
- Input vectors arrive at the row count of the current batch.
- Scalar (literal) arguments arrive as length-1 vectors: read at index 0.
- The returned vector's length **must match** the longest input vector.
- Instances are cached per executor thread, so implementations should be **stateless**.
- `inputTypes` is only required for columnar-only registration (see Option 3 below).

## Registering a CometUDF

There are three ways to register a `CometUDF` with Comet, depending on whether you also want a
row-based Spark fallback.

### Option 1: Comet UDF only (existing Spark UDF)

If you already have a Spark UDF registered, just tell Comet about the accelerated implementation:

```scala
import org.apache.comet.udf.CometUdfRegistry

// Register the Spark UDF (row-at-a-time fallback)
spark.udf.register("is_positive", (x: Int) => x > 0)

// Tell Comet about the vectorized implementation
CometUdfRegistry.register(classOf[IsPositiveUdf])
```

### Option 2: Register both in one call

```scala
import org.apache.comet.udf.CometUdfRegistry

CometUdfRegistry.register(spark, classOf[IsPositiveUdf], (x: Int) => x > 0)
```

Convenience overloads exist for arities 1, 2, and 3. For higher arities, use Option 1 and call
`spark.udf.register` separately.

### Option 3: Columnar-only (no row-based equivalent)

If you do not want to write a row-based fallback, Comet can synthesize a stub Spark UDF that
throws `UnsupportedOperationException` if invoked row-at-a-time. The `CometUDF` must declare
`inputTypes` so the stub has the correct arity.

```scala
import org.apache.comet.udf.CometUdfRegistry

CometUdfRegistry.registerColumnarOnly(spark, classOf[IsPositiveUdf])
```

When Comet is enabled and the query is supported, the vectorized implementation runs natively.
If Comet falls back (e.g. an unsupported expression elsewhere in the plan), the stub is invoked
and the query fails with a clear error rather than silently degrading to row-at-a-time execution.

## How It Works

1. **Query planning**: When Comet's serde layer encounters a `ScalaUDF` expression with a name registered in
   `CometUdfRegistry`, it emits a `JvmScalarUdf` protobuf message instead of falling back to Spark.

2. **Native execution**: The Rust execution engine evaluates the UDF's input expressions to Arrow arrays, then
   calls back into the JVM via the Arrow C Data Interface (zero-copy FFI).

3. **JVM execution**: `CometUdfBridge` instantiates your `CometUDF` class (cached per thread), passes the input
   Arrow vectors, and exports the result vector back to native execution.

4. **Fallback**: If Comet is disabled or the UDF is not in the registry, Spark executes the UDF row-at-a-time
   using the originally registered Scala/Java function. Columnar-only UDFs raise an exception in this case.

## Packaging and Deployment

1. Package your `CometUDF` implementation in a JAR.
2. Include it on the Spark classpath via `--jars` or `spark.jars`.
3. Register the UDF as shown above (in your application code or via a Spark session extension).

The `CometUDF` class is resolved using the executor's context classloader, so user-supplied JARs added via
`spark.jars` or `--jars` are automatically visible.

## Limitations

- Only scalar UDFs are supported (not aggregate or table UDFs).
- The UDF must be registered by name: anonymous lambdas without a name cannot be intercepted.
- All input and output types must be representable as Arrow vectors.
- `registerColumnarOnly` currently supports arities 1 through 5.
