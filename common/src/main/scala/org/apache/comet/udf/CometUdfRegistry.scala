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

import java.util.concurrent.ConcurrentHashMap

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.{UDF1, UDF2, UDF3, UDF4, UDF5}
import org.apache.spark.sql.types.DataType

/**
 * Registry for user-defined CometUDF implementations. Spark UDF metadata (name, return type,
 * nullability) is read from the [[CometUDF]] class itself, so registration is a single call:
 *
 * {{{
 * // Comet-only (user has already called spark.udf.register elsewhere):
 * CometUdfRegistry.register(classOf[MyUdf])
 *
 * // Comet plus a row-based Spark fallback in one call:
 * CometUdfRegistry.register(spark, classOf[MyUdf], (x: Int) => x > 0)
 *
 * // Columnar-only: no row-based equivalent. Calling the UDF row-at-a-time
 * // (e.g. when Comet falls back) raises UnsupportedOperationException:
 * CometUdfRegistry.registerColumnarOnly(spark, classOf[MyUdf])
 * }}}
 */
object CometUdfRegistry {

  case class UdfEntry(className: String, returnType: DataType, nullable: Boolean)

  private val registry = new ConcurrentHashMap[String, UdfEntry]()

  /**
   * Register a CometUDF for use by Comet. The caller is responsible for separately registering a
   * row-based Spark UDF under the same name (e.g. via `spark.udf.register(name, fn)`); without
   * one, Spark will fail to bind the function unless it has a stub from [[registerColumnarOnly]].
   */
  def register(udfClass: Class[_ <: CometUDF]): Unit = {
    val udf = newInstance(udfClass)
    registry.put(udf.name, UdfEntry(udfClass.getName, udf.returnType, udf.nullable))
  }

  /** Register an arity-1 Spark UDF and the matching CometUDF in one call. */
  def register[A1: TypeTag, RT: TypeTag](
      spark: SparkSession,
      udfClass: Class[_ <: CometUDF],
      func: A1 => RT): Unit = {
    val udf = newInstance(udfClass)
    spark.udf.register(udf.name, func)
    registry.put(udf.name, UdfEntry(udfClass.getName, udf.returnType, udf.nullable))
  }

  /** Register an arity-2 Spark UDF and the matching CometUDF in one call. */
  def register[A1: TypeTag, A2: TypeTag, RT: TypeTag](
      spark: SparkSession,
      udfClass: Class[_ <: CometUDF],
      func: (A1, A2) => RT): Unit = {
    val udf = newInstance(udfClass)
    spark.udf.register(udf.name, func)
    registry.put(udf.name, UdfEntry(udfClass.getName, udf.returnType, udf.nullable))
  }

  /** Register an arity-3 Spark UDF and the matching CometUDF in one call. */
  def register[A1: TypeTag, A2: TypeTag, A3: TypeTag, RT: TypeTag](
      spark: SparkSession,
      udfClass: Class[_ <: CometUDF],
      func: (A1, A2, A3) => RT): Unit = {
    val udf = newInstance(udfClass)
    spark.udf.register(udf.name, func)
    registry.put(udf.name, UdfEntry(udfClass.getName, udf.returnType, udf.nullable))
  }

  /**
   * Register a CometUDF without a row-based Spark equivalent. A stub Spark UDF is synthesized so
   * Spark can bind the function name during analysis; calling the stub row-at-a-time (i.e. when
   * Comet is disabled or falls back) raises [[UnsupportedOperationException]].
   *
   * The CometUDF must declare [[CometUDF.inputTypes]] so that the synthesized stub has the
   * correct arity. Arities 1 through 5 are supported; declare more inputs only if you have a
   * concrete need (and extend the match below).
   */
  def registerColumnarOnly(spark: SparkSession, udfClass: Class[_ <: CometUDF]): Unit = {
    val udf = newInstance(udfClass)
    require(
      udf.inputTypes.nonEmpty,
      s"CometUDF '${udf.name}' must override inputTypes for columnar-only registration")
    registerStub(spark, udf)
    registry.put(udf.name, UdfEntry(udfClass.getName, udf.returnType, udf.nullable))
  }

  /** Look up a registered CometUDF by its Spark UDF name. */
  def get(name: String): Option[UdfEntry] = Option(registry.get(name))

  /** Remove a previously registered UDF. */
  def remove(name: String): Unit = registry.remove(name)

  /** Check whether a UDF name is registered. */
  def isRegistered(name: String): Boolean = registry.containsKey(name)

  // Visible for testing
  def size(): Int = registry.size()

  // Visible for testing
  def clear(): Unit = registry.clear()

  private def newInstance(cls: Class[_ <: CometUDF]): CometUDF =
    cls.getDeclaredConstructor().newInstance()

  private def registerStub(spark: SparkSession, udf: CometUDF): Unit = {
    val name = udf.name
    val rt = udf.returnType
    def fail(): Nothing = throw new UnsupportedOperationException(
      s"CometUDF '$name' is columnar-only and cannot be evaluated row-at-a-time. " +
        "Ensure Comet is enabled and supports this query.")
    udf.inputTypes.length match {
      case 1 =>
        spark.udf.register(
          name,
          new UDF1[AnyRef, AnyRef] { override def call(a: AnyRef): AnyRef = fail() },
          rt)
      case 2 =>
        spark.udf.register(
          name,
          new UDF2[AnyRef, AnyRef, AnyRef] {
            override def call(a: AnyRef, b: AnyRef): AnyRef = fail()
          },
          rt)
      case 3 =>
        spark.udf.register(
          name,
          new UDF3[AnyRef, AnyRef, AnyRef, AnyRef] {
            override def call(a: AnyRef, b: AnyRef, c: AnyRef): AnyRef = fail()
          },
          rt)
      case 4 =>
        spark.udf.register(
          name,
          new UDF4[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] {
            override def call(a: AnyRef, b: AnyRef, c: AnyRef, d: AnyRef): AnyRef = fail()
          },
          rt)
      case 5 =>
        spark.udf.register(
          name,
          new UDF5[AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef] {
            override def call(a: AnyRef, b: AnyRef, c: AnyRef, d: AnyRef, e: AnyRef): AnyRef =
              fail()
          },
          rt)
      case n =>
        throw new UnsupportedOperationException(
          s"Columnar-only registration is not yet supported for arity $n")
    }
  }
}
