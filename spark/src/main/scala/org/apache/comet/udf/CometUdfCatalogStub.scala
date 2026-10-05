/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.udf

import org.apache.spark.sql.api.java.{UDF0, UDF1, UDF2, UDF3, UDF4}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataType

/**
 * Builds the Spark catalog function that makes a UDF registered with Comet resolvable by name.
 *
 * The function only ever throws. Spark evaluates it only when Comet did not take the operator
 * holding the call, and failing there makes that fallback visible rather than silent.
 *
 * It is built from Spark's Java UDF interfaces, which declare no argument types, so Spark inserts
 * no casts for its arguments; `CometScalaUDF` checks them against the registered signature.
 */
private[udf] object CometUdfCatalogStub {

  /**
   * The most arguments a stub takes. Raising it is tracked by
   * [[https://github.com/apache/datafusion-comet/issues/6177]].
   */
  val MaxArity = 4

  /** The stub for a UDF named `name`. Fails for more than [[MaxArity]] arguments. */
  def apply(
      name: String,
      arity: Int,
      returnType: DataType,
      deterministic: Boolean): UserDefinedFunction = {
    def fail(): Any = throw new CometUdfNotEvaluatedException(name)
    val stub = arity match {
      case 0 => udf((() => fail()): UDF0[Any], returnType)
      case 1 => udf(((_: Any) => fail()): UDF1[Any, Any], returnType)
      case 2 => udf(((_: Any, _: Any) => fail()): UDF2[Any, Any, Any], returnType)
      case 3 =>
        udf(((_: Any, _: Any, _: Any) => fail()): UDF3[Any, Any, Any, Any], returnType)
      case 4 =>
        udf(
          ((_: Any, _: Any, _: Any, _: Any) => fail()): UDF4[Any, Any, Any, Any, Any],
          returnType)
      case n =>
        throw new IllegalArgumentException(
          s"UDF '$name' takes $n arguments, but Comet supports at most $MaxArity.")
    }
    if (deterministic) stub else stub.asNondeterministic()
  }
}
