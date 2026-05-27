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

import scala.util.Try

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataType

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

/**
 * Public entry point for registering Rust scalar UDFs with Comet.
 *
 * Two ABI flavors are supported transparently -- the cdylib registers each UDF under one of:
 *   - `c-abi`: pure C / Arrow C Data Interface (sedona-style)
 *   - `datafusion-ffi`: `FFI_ScalarUDF`
 *
 * The user always calls `register` / `registerAll`; the native side picks the right ABI by name.
 */
object CometRustUDF {

  /** Spark conf key under which registered Rust UDF entries are propagated to executors. */
  val RUST_UDFS_CONF_KEY = "spark.comet.rustUdfs"

  private val mapper: ObjectMapper = new ObjectMapper()

  /**
   * Register a single Rust UDF with an explicit signature.
   *
   * Validates the library on the driver (loads it, confirms a UDF named `name` exists). On
   * success a stub Spark catalog UDF is installed (so SQL/DataFrame name resolution succeeds),
   * the driver-side registry is updated, and `spark.comet.rustUdfs` is updated so executors see
   * the registration.
   */
  def register(
      spark: SparkSession,
      name: String,
      libraryPath: String,
      inputTypes: Seq[DataType],
      returnType: DataType,
      deterministic: Boolean = true): Unit = {
    val described = describeOne(libraryPath, name)
    require(described.name == name, s"unexpected name from native: ${described.name}")
    installCatalogStub(spark, name, inputTypes, returnType, deterministic)
    val meta = RustUdfMetadata(libraryPath, inputTypes, returnType, deterministic)
    CometRustUdfRegistry.instance.register(name, meta)
    propagateConf(spark)
  }

  // -------- internals --------

  private case class Described(name: String, abi: String)

  private def describeOne(libraryPath: String, name: String): Described = {
    val json =
      invokeBridge(() => CometRustUdfBridge.validateLibrary(libraryPath, name), libraryPath)
    parseDescribed(json)
  }

  private def invokeBridge(call: () => String, libraryPath: String): String = {
    Try(call()).recover { case t: Throwable => throw classifyNativeError(libraryPath, t) }.get
  }

  private def parseDescribed(json: String): Described = {
    val node = mapper.readTree(json).asInstanceOf[ObjectNode]
    Described(name = node.get("name").asText(), abi = node.get("abi").asText())
  }

  private def classifyNativeError(libraryPath: String, t: Throwable): RuntimeException = {
    val m = Option(t.getMessage).getOrElse("")
    if (m.contains("ABI") || m.contains("missing required symbol") ||
      m.contains("comet_udf_abi_version") || m.contains("exposes neither")) {
      new CometRustUdfAbiException(m)
    } else if (m.contains("not found in")) {
      new java.util.NoSuchElementException(m)
    } else {
      new CometRustUdfLoadException(s"failed to load $libraryPath: $m", t)
    }
  }

  private def installCatalogStub(
      spark: SparkSession,
      name: String,
      inputTypes: Seq[DataType],
      returnType: DataType,
      deterministic: Boolean): Unit = {
    val arity = inputTypes.size
    val u: UserDefinedFunction = arity match {
      case 0 =>
        udf(() => throw new CometRustUdfNotEvaluatedException(name), returnType)
      case 1 =>
        udf((_: Any) => throw new CometRustUdfNotEvaluatedException(name), returnType)
      case 2 =>
        udf((_: Any, _: Any) => throw new CometRustUdfNotEvaluatedException(name), returnType)
      case 3 =>
        udf(
          (_: Any, _: Any, _: Any) => throw new CometRustUdfNotEvaluatedException(name),
          returnType)
      case 4 =>
        udf(
          (_: Any, _: Any, _: Any, _: Any) => throw new CometRustUdfNotEvaluatedException(name),
          returnType)
      case n =>
        throw new IllegalArgumentException(
          s"Rust UDF '$name' arity $n not supported by stub. Reduce arity " +
            s"or open a feature request to extend stub coverage.")
    }
    val finalUdf = if (deterministic) u else u.asNondeterministic()
    spark.udf.register(name, finalUdf)
  }

  private def propagateConf(spark: SparkSession): Unit = {
    val snapshot = CometRustUdfRegistry.instance.snapshot
    val entries = snapshot.toSeq.map { case (name, meta) =>
      mapper.writeValueAsString(java.util.Map.of("name", name, "libraryPath", meta.libraryPath))
    }
    spark.conf.set(RUST_UDFS_CONF_KEY, entries.mkString(";"))
  }
}
