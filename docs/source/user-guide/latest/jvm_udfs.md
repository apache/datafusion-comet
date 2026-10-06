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

# Vectorized Java and Scala UDFs

Comet can run a scalar user-defined function written in Java or Scala that works on a whole batch
at a time: it receives each argument as an Arrow vector and returns the batch's results as one
Arrow vector.

This is different from [Scala UDF and Java UDF Support](scala_java_udfs.md), which covers
ordinary Spark UDFs. Comet runs those through a code generator that calls the function once per
row, converting every value to and from the function's Java or Scala types. A vectorized UDF is
called once per batch and reads and writes Arrow buffers directly.

> **Experimental.** This feature is experimental. `CometJvmUDF` and the `CometUDF` interface are
> not part of Comet's supported API: they fall under
> [everything else is internal](../../about/versioning_policy.md#everything-else-is-internal) in
> the [versioning policy](../../about/versioning_policy.md), so they may change or be removed in
> any release, including a patch release, with no deprecation cycle. Expect to rebuild your UDF
> against each Comet release you upgrade to. See [Limitations](#limitations) before adopting it.

## Choosing between an ordinary UDF and a vectorized UDF

|                                      | Ordinary Spark UDF                         | Vectorized Comet UDF                              |
| ------------------------------------ | ------------------------------------------ | ------------------------------------------------- |
| Written as                           | A function of one row's values             | A `CometUDF` class over Arrow vectors             |
| Called                               | Once per row                               | Once per batch                                    |
| Argument expressions run             | In the JVM, compiled together with the UDF | Natively, before the call                         |
| Null handling                        | A null primitive argument returns null     | Your code checks each value with `isNull`         |
| When Comet does not run the operator | Runs on Spark                              | Runs on Spark one row at a time, much more slowly |
| Compiled against                     | Spark                                      | Spark and Comet, rebuilt for each Comet release   |

An ordinary UDF needs no Comet-specific code, and Comet already runs it in its pipeline. The
code generator compiles the call into a loop that the JVM optimizes well, so rewriting a simple
function of primitive values in vectorized form gains little. Write a vectorized UDF for what a
function of one row cannot express:

- Work done once per batch rather than once per row, such as building a lookup structure from the
  batch's values.
- A different representation of the values, such as working on the UTF-8 bytes of a string
  column instead of one `String` per row.
- One call per batch into a library that already works on batches.

## Writing a UDF

A vectorized UDF is a class implementing `org.apache.comet.udf.CometUDF`:

```java
package com.example;

import org.apache.comet.shaded.arrow.vector.BigIntVector;
import org.apache.comet.shaded.arrow.vector.ValueVector;
import org.apache.comet.udf.CometUDF;

public class AddOne implements CometUDF {
  @Override
  public ValueVector evaluate(ValueVector[] inputs, int numRows) {
    BigIntVector in = (BigIntVector) inputs[0];
    BigIntVector out = new BigIntVector("add_one", in.getAllocator());
    out.allocateNew(numRows);
    for (int i = 0; i < numRows; i++) {
      // A literal argument arrives as a single value rather than one per row.
      int row = in.getValueCount() == numRows ? i : 0;
      if (in.isNull(row)) {
        out.setNull(i);
      } else {
        out.set(i, in.get(row) + 1);
      }
    }
    out.setValueCount(numRows);
    return out;
  }
}
```

Compile it against the Comet jar for your Spark and Scala versions, for example
`org.apache.datafusion:comet-spark-spark4.1_2.13`, in `provided` scope. Note the Arrow imports:
Comet's jar relocates Arrow Java to `org.apache.comet.shaded.arrow`, and `CometUDF.evaluate` is
declared over the relocated classes. A class written against stock `org.apache.arrow` does not
implement it. Use the relocated classes throughout, and expect to rebuild the UDF against each
Comet release, which can change the Arrow Java version behind them.

### The contract

- `evaluate` is called once per batch. An argument that is a column arrives as a vector of
  `numRows` values. An argument that is constant for the batch, such as a literal or the result
  of a scalar subquery, arrives as a vector holding one value, which applies to every row. Read
  it at index 0.
- Comet does not skip null rows. Check each argument with `isNull` and decide what the row
  returns.
- Comet turns off Arrow Java's bounds and null checks for the whole JVM
  (`arrow.enable_unsafe_memory_access=true` and `arrow.enable_null_check_for_get=false`), so a
  mistake does not throw. Reading past the end of a vector, such as reading a one-value argument
  at the row's index, reads whatever memory is there, and `get` on a null slot returns whatever
  the slot holds. Check `isNull` before `get`.
- The result must be a new vector holding exactly `numRows` values, whose Arrow type is that of
  the return type the UDF was registered with. A `LongType` result is a `BigIntVector`, a
  `StringType` result is a `VarCharVector`, a `TimestampType` result is a `TimeStampMicroTZVector`
  in the `UTC` timezone, and so on. Comet checks the result's type and fails the query naming both
  types if it differs. Differences that do not change the data are allowed: the names of a list's
  or map's child fields (Arrow Java names a list's element `$data$`, where Comet uses `item`), and
  the nullability of nested fields, as long as a field declared non-nullable holds no nulls.
- `evaluate` receives no allocator yet. Allocate the result from an argument's allocator, as
  `in.getAllocator()` does above.
- Do not close the argument vectors, or keep them or the result past the call. Comet closes the
  arguments when the call returns and hands the result to native execution.
- The class needs a public no-argument constructor. Comet creates one instance per class for each
  Spark task and reuses it for every batch in that task, so its fields can hold per-task state
  such as compiled patterns or scratch buffers. Native execution can call `evaluate` on the same
  instance from more than one thread at once, so synchronize access to any mutable field, or keep
  state local to the call.
- Where Spark evaluates the call itself (see below), it calls `evaluate` with `numRows = 1` on an
  instance of its own, sometimes on the driver while planning the query.
- `TaskContext.get()` returns the task's context, and the task's context ClassLoader is installed
  for the call, so classes from jars passed with `--jars` resolve. On the driver,
  `TaskContext.get()` returns `null`.
- An exception thrown from `evaluate` fails the task with its message.

## Registering a UDF

Register the class under a name with `CometJvmUDF.register`, declaring the argument types and the
return type Spark plans the call against:

```scala
import org.apache.spark.sql.types.LongType
import org.apache.comet.udf.CometJvmUDF

CometJvmUDF.register(spark, "add_one", classOf[com.example.AddOne], Seq(LongType), LongType)

spark.read.parquet("/path/to/events").createOrReplaceTempView("events")
spark.sql("SELECT add_one(id) FROM events").show()
```

The call runs natively only in an operator Comet runs, over input Comet reads, such as a Parquet
scan. Spark's `range()` and local data such as `VALUES` do not run natively by default, so over
those Spark evaluates the call itself.

From Java, pass the argument types as a `java.util.List` and the `deterministic` flag explicitly:

```java
CometJvmUDF.register(
    spark, "add_one", AddOne.class, List.of(DataTypes.LongType), DataTypes.LongType, true);
```

`register` checks on the driver that the class is concrete and public and has a public
no-argument constructor, and that Comet can carry the argument and return types natively.
Executors load the class by name, so it has to be on their classpath too, for example through
`--jars`.

The name becomes a temporary function of the session, just as with `spark.udf.register`: other
sessions do not see it, and registering another function under the same name replaces it.

The argument types are a signature every call must match. Spark's analyzer checks each call
against it, ignoring nullability, and inserts no casts, so a call with other argument types fails
analysis. Cast the arguments in the query instead, as in `add_one(cast(x AS BIGINT))`. The analyzer
compares struct field names the way it is configured to, which by default ignores case, and the
UDF receives the field names of the call's argument.

Pass `deterministic = false` for a function that can return different results for the same
arguments, so that Spark does not reorder or deduplicate its calls.

### Arguments run natively

Comet evaluates each argument natively and passes the UDF only the values. In `add_one(abs(x))`,
`abs` runs natively and only its result crosses into the JVM. An ordinary UDF instead runs its
whole argument tree in the JVM along with the function.

### When Spark evaluates the call

Wherever Spark evaluates the call itself, it runs the UDF on one row at a time: it writes the row's
arguments into one-row vectors, calls `evaluate` with `numRows = 1`, and reads back the single
value. The answer is the same as the native path's, only much slower. That happens:

- when Comet does not take the operator holding the call, for example because another expression
  in it is not supported. The query's extended explain output gives the reason.
- inside an expression that Comet runs in its JVM codegen dispatcher, such as an ordinary Scala or
  Java UDF called on a vectorized UDF's result, or a higher-order function like `transform`.
- while Spark plans the query: over local data, which the optimizer evaluates eagerly, in a filter
  on partition columns, which Spark evaluates to prune partitions, and in the sort keys of a global
  sort, which Spark evaluates on a sample of rows to choose range bounds.

## Limitations

- The API is experimental and may change in any release, and a UDF has to be rebuilt against each
  Comet release because it is compiled against Comet's relocated Arrow classes.
- `evaluate` receives no allocator, so a UDF borrows one from its arguments and a UDF without
  arguments has none to use
  ([#4174](https://github.com/apache/datafusion-comet/issues/4174)).
- Where Spark evaluates the call itself, the UDF runs one row at a time, which is much slower
  than the native path. Check the query's extended explain output to see whether Comet ran the
  operator holding the call.
- A call that blocks holds one of Comet's native execution threads for as long as it runs
  ([#6293](https://github.com/apache/datafusion-comet/issues/6293)).
