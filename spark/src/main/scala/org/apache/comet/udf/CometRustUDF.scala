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
 * Entry point for registering Rust scalar UDFs with Comet.
 *
 * The UDF cdylib is built against the `comet-udf-sdk` crate and exposes its functions through an
 * ABI built only on the Arrow C Data Interface, so a compiled UDF is not tied to Comet's
 * DataFusion version.
 *
 * This is an experimental API. It is deliberately not annotated
 * `org.apache.comet.annotation.Public`, so it sits outside the enumerated public API in Comet's
 * [[https://datafusion.apache.org/comet/about/versioning_policy.html versioning policy]] and
 * carries no compatibility guarantee: it may change or be removed in any release, including a
 * patch release, with no deprecation cycle.
 */
object CometRustUDF {

  private val mapper: ObjectMapper = new ObjectMapper()

  /**
   * Register a single Rust UDF with an explicit signature.
   *
   * Validates the library on the driver (loads it, confirms a UDF named `name` exists). On
   * success a stub Spark catalog UDF is installed (so SQL/DataFrame name resolution succeeds) and
   * the driver-side registry is updated.
   *
   * Executors do not consult the driver's registry: the library path travels with the plan in the
   * `RustUdfCall` proto, and each executor loads the library itself on first use. The path must
   * therefore be valid on every executor, not just the driver.
   *
   * `deterministic` must be `true`. Comet plans every imported kernel as immutable, so a
   * nondeterministic UDF cannot yet be expressed; passing `false` fails here rather than silently
   * planning the function as pure.
   */
  def register(
      spark: SparkSession,
      name: String,
      libraryPath: String,
      inputTypes: Seq[DataType],
      returnType: DataType,
      deterministic: Boolean = true): Unit = {
    if (!deterministic) {
      // The native signature is built once per library load with
      // Volatility::Immutable, while determinism is declared per registration, so the
      // flag cannot be honored without reworking how kernels are cached. Until then a
      // `false` here would let DataFusion constant-fold or CSE a call the user told us
      // was not safe to reuse.
      throw new IllegalArgumentException(
        s"Rust UDF '$name': deterministic = false is not supported yet. Comet plans Rust UDFs " +
          "as immutable, so a nondeterministic function may be constant-folded or eliminated " +
          "as a common subexpression. See https://github.com/apache/datafusion-comet/issues/5249")
    }
    val described = describeOne(libraryPath, name)
    require(described.name == name, s"unexpected name from native: ${described.name}")
    installCatalogStub(spark, name, inputTypes, returnType, deterministic)
    val meta = RustUdfMetadata(libraryPath, inputTypes, returnType, deterministic)
    CometRustUdfRegistry.instance.register(name, meta)
  }

  // -------- internals --------

  private case class Described(name: String)

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
    Described(name = node.get("name").asText())
  }

  private def classifyNativeError(libraryPath: String, t: Throwable): RuntimeException = {
    val m = Option(t.getMessage).getOrElse("")
    if (m.contains("ABI") || m.contains("missing required symbol") ||
      m.contains("comet_udf_abi_version") || m.contains("does not export")) {
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
            "or open a feature request to extend stub coverage.")
    }
    val finalUdf = if (deterministic) u else u.asNondeterministic()
    spark.udf.register(name, finalUdf)
  }
}
