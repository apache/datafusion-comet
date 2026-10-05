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

import java.lang.reflect.Modifier

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType

/**
 * Entry point for registering vectorized scalar UDFs written in Java or Scala.
 *
 * A vectorized UDF implements [[CometUDF]]. Comet calls it once per batch, with each argument as
 * an Arrow vector, and it returns the whole batch's results as one Arrow vector. That differs
 * from an ordinary Spark UDF, which the codegen dispatcher calls once per row. Comet evaluates
 * the arguments natively, so in `add_one(abs(x))` only the result of `abs` crosses into the JVM.
 *
 * Comet's published jar relocates Arrow Java under `org.apache.comet.shaded.arrow`, so a UDF
 * compiled against that jar implements `evaluate` over the relocated vector classes and has to
 * use them throughout.
 *
 * This is an experimental API. It is deliberately not annotated
 * `org.apache.comet.annotation.Public`, so it sits outside the enumerated public API in Comet's
 * [[https://datafusion.apache.org/comet/about/versioning_policy.html versioning policy]] and
 * carries no compatibility guarantee: it may change or be removed in any release, including a
 * patch release, with no deprecation cycle.
 */
object CometJvmUDF {

  /**
   * Register `udfClass` as a scalar UDF named `name`, callable from SQL and the DataFrame API.
   *
   * Checks on the driver that the class can be instantiated the way executors will instantiate
   * it, then records it in Comet's registry and installs a Spark catalog function under `name`.
   * Executors load the class by name through the task's context ClassLoader, so it has to be
   * available to them as well, for example through `--jars`.
   *
   * `inputTypes` is the signature every call must match. Comet does not convert arguments to
   * these types: a call whose argument types differ, other than in nullability, is refused at
   * planning time with both signatures named, so cast the arguments in the query instead.
   * `returnType` is what Spark plans the call against, and the vector the UDF returns must have
   * its Arrow type.
   *
   * The catalog function throws if Spark evaluates it, which happens only when Comet does not
   * take the operator holding the call.
   */
  def register(
      spark: SparkSession,
      name: String,
      udfClass: Class[_ <: CometUDF],
      inputTypes: Seq[DataType],
      returnType: DataType,
      deterministic: Boolean = true): Unit = {
    validateUdfClass(udfClass)
    val stub = CometUdfCatalogStub(name, inputTypes.size, returnType, deterministic)
    CometUdfRegistry.register(name, JvmUdfMetadata(udfClass.getName, inputTypes, returnType))
    // Last, because this is the step that makes the name resolvable to Spark's analyzer. A call
    // planned against a resolvable name with no registry entry would go to the codegen
    // dispatcher, which would run the stub and fail.
    val _ = spark.udf.register(name, stub)
  }

  /** `register` for Java callers, taking the argument types as a `java.util.List`. */
  def register(
      spark: SparkSession,
      name: String,
      udfClass: Class[_ <: CometUDF],
      inputTypes: java.util.List[DataType],
      returnType: DataType,
      deterministic: Boolean): Unit =
    register(spark, name, udfClass, inputTypes.asScala.toList, returnType, deterministic)

  /**
   * Refuse a class that `CometUdfBridge` could not instantiate, so the mistake surfaces at
   * registration rather than as a failed task.
   */
  private def validateUdfClass(udfClass: Class[_]): Unit = {
    def refuse(reason: String): Nothing =
      throw new IllegalArgumentException(
        s"${udfClass.getName} cannot be registered as a Comet JVM UDF: $reason")
    // Reachable only through an unchecked cast, since the parameter type already demands it.
    if (!classOf[CometUDF].isAssignableFrom(udfClass)) {
      refuse(s"it does not implement ${classOf[CometUDF].getName}")
    }
    val modifiers = udfClass.getModifiers
    // An interface's modifiers include abstract too.
    if (Modifier.isAbstract(modifiers)) {
      refuse("it is abstract")
    }
    if (!Modifier.isPublic(modifiers)) {
      refuse("it is not public")
    }
    // An inner class's constructor takes its enclosing instance, so this also refuses one.
    if (!udfClass.getConstructors.exists(_.getParameterCount == 0)) {
      refuse("it has no public no-argument constructor")
    }
  }
}
