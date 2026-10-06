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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.comet.CometUdfErrors
import org.apache.spark.sql.types.DataType

import org.apache.comet.serde.QueryPlanSerde
import org.apache.comet.shims.ShimSessionFunctionRegistry

/**
 * A call to a UDF registered with Comet. The function [[CometUdfCall.register]] installs in a
 * session builds one for each call it resolves. [[NativeUdfCall]] and [[JvmUdfCall]] differ in
 * what Comet runs for the call and in what `eval` does where Spark evaluates the call itself.
 *
 * `argumentTypes` is the registered signature, one type per child. Spark's analyzer checks each
 * call against it, disregarding nullability, and inserts no casts.
 */
trait CometUdfCall extends Expression with ExpectsInputTypes with CodegenFallback {

  /** The name the UDF was registered under. */
  def name: String

  def argumentTypes: Seq[DataType]

  /** Whether the UDF itself was registered as deterministic. */
  def udfDeterministic: Boolean

  override def inputTypes: Seq[DataType] = argumentTypes

  override def nullable: Boolean = true

  override lazy val deterministic: Boolean =
    udfDeterministic && children.forall(_.deterministic)

  override def prettyName: String = name

  override def toString: String = s"$name(${children.mkString(", ")})"
}

object CometUdfCall {

  /**
   * Install `name` as a temporary function of `spark`'s session, as `spark.udf.register` would:
   * other sessions do not see it, and registering another function under the same name replaces
   * it. `newCall` builds the call for the arguments of each call the analyzer resolves.
   *
   * Refuses a signature with a type Comet has no native representation for, since no call could
   * run. `source` goes into the function's `ExpressionInfo`, which accepts only Spark's own
   * source names, such as `scala_udf`.
   */
  def register(
      spark: SparkSession,
      name: String,
      inputTypes: Seq[DataType],
      returnType: DataType,
      source: String)(newCall: Seq[Expression] => CometUdfCall): Unit = {
    (inputTypes :+ returnType).find(QueryPlanSerde.serializeDataType(_).isEmpty).foreach { t =>
      throw new IllegalArgumentException(
        s"UDF '$name' cannot be registered with type ${t.catalogString}: Comet has no native " +
          "representation for it.")
    }
    ShimSessionFunctionRegistry
      .functionRegistry(spark)
      .createOrReplaceTempFunction(
        name,
        children => {
          // Checked here, as built-in functions check it, because the analyzer's type coercion
          // pairs arguments with types positionally before any check on the call would run.
          if (children.length != inputTypes.length) {
            throw CometUdfErrors.wrongNumArgs(name, inputTypes.length, children.length)
          }
          newCall(children)
        },
        source)
  }
}
