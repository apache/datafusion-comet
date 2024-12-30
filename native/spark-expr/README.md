<!--
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

# datafusion-comet-spark-expr: Spark-compatible Expressions

This crate provides Apache Spark-compatible expressions for use with DataFusion and is maintained as part of the 
[Apache DataFusion Comet](https://github.com/apache/datafusion-comet/) subproject.

## Expression location

The files are aimed to be organized in the same way Spark group its expressions.
You can see the grouping in [Spark Docs](https://spark.apache.org/docs/3.5.3/sql-ref-functions-builtin.html)
or in Spark source code marked with `ExpressionDescription` annotation. 

For example, for the following expression (taken from Spark source code):
```scala
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2, ...) - Returns a hash value of the arguments.",
  examples = """
    Examples:
      > SELECT _FUNC_('Spark', array(123), 2);
       -1321691492
  """,
  since = "2.0.0",
  group = "hash_funcs")
case class Murmur3Hash(children: Seq[Expression], seed: Int) extends HashExpression[Int] {
  // ...
}
```

the native implementation will be in the `hash_funcs/murmur3.rs` file.

Some expressions are not under a specific group like the `UnscaledValue` expression (taken from Spark source code):
```scala
case class UnscaledValue(child: Expression) extends UnaryExpression with NullIntolerant {
  // ...
}
```

In that case we will do our best to find a suitable group for it (for the example above, it would be under `math_funcs/internal/unscaled_value.rs`).
