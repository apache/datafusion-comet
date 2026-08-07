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
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataType

/**
 * Entry point for registering scalar UDFs that run as native code inside Comet.
 *
 * A UDF lives in a shared library that exports the Comet UDF C ABI. That ABI is parameterized
 * only by the Arrow C Data Interface and mentions no DataFusion or Rust types, so a compiled UDF
 * is not tied to Comet's DataFusion version, and the host does not care what language produced
 * the library. Only the Rust SDK (`comet-udf-sdk`) is supported and tested today; a C or C++
 * library that implemented the same ABI would load, but nothing ships a header for it. Hence the
 * neutral name here, rather than one that promises Rust specifically.
 *
 * This is an experimental API. It is deliberately not annotated
 * `org.apache.comet.annotation.Public`, so it sits outside the enumerated public API in Comet's
 * [[https://datafusion.apache.org/comet/about/versioning_policy.html versioning policy]] and
 * carries no compatibility guarantee: it may change or be removed in any release, including a
 * patch release, with no deprecation cycle.
 */
object CometNativeUDF {

  /**
   * Register a single native UDF with an explicit signature.
   *
   * Validates the library on the driver (loads it, confirms a UDF named `name` exists). On
   * success the driver-side registry is updated and a stub Spark catalog UDF is installed, in
   * that order, so SQL/DataFrame name resolution succeeds only once the plan can be serialized as
   * a `NativeScalarUdf`.
   *
   * Executors do not consult the driver's registry: the library path travels with the plan in the
   * `NativeScalarUdf` proto, and each executor loads the library itself on first use. The path
   * must therefore be valid on every executor, not just the driver.
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
        s"native UDF '$name': deterministic = false is not supported yet. Comet plans native " +
          "UDFs as immutable, so a nondeterministic function may be constant-folded or " +
          "eliminated as a common subexpression. " +
          "See https://github.com/apache/datafusion-comet/issues/5249")
    }
    validateLibrary(libraryPath, name)
    val meta = NativeUdfMetadata(libraryPath, inputTypes, returnType, deterministic)
    CometNativeUdfRegistry.instance.register(name, meta)
    // Last, because this is the step that makes the name resolvable to Spark's analyzer. A query
    // planned against a resolvable name that has no registry entry yet would route the call to the
    // JVM codegen dispatcher and hit the stub's "not evaluated" exception, so the registry entry
    // has to be in place first.
    installCatalogStub(spark, name, inputTypes, returnType, deterministic)
  }

  /**
   * Load the library on the driver and confirm it exposes a UDF named `name`, translating the
   * native failure into a typed exception.
   */
  private def validateLibrary(libraryPath: String, name: String): Unit = {
    try {
      CometNativeUdfBridge.validateLibrary(libraryPath, name)
    } catch {
      case t: Throwable => throw classifyNativeError(libraryPath, t)
    }
  }

  /**
   * Map a native loader failure onto a typed exception.
   *
   * The native side reports these as plain messages, so the mapping keys on the wording produced
   * by `LoaderError`'s `Display` impl (`native/core/src/execution/c_udf/loader.rs`) and by
   * `comet_native_udf_bridge.rs`. Each phrase below is matched in full rather than by a fragment
   * like "ABI", because every one of those messages interpolates the library path: a library
   * under a directory named `ABI` would otherwise have its "failed to open" reported as an ABI
   * mismatch. `CometNativeUdfSuite` pins each failure mode to the type it produces here, so a
   * reworded message on the native side fails a test rather than silently changing the exception
   * a caller sees.
   */
  private def classifyNativeError(libraryPath: String, t: Throwable): RuntimeException = {
    val m = Option(t.getMessage).getOrElse("")
    if (m.contains("missing required symbol") || m.contains("reports ABI v") ||
      m.contains("does not export")) {
      new CometNativeUdfAbiException(m)
    } else if (m.contains("' not found in ")) {
      new java.util.NoSuchElementException(m)
    } else {
      new CometNativeUdfLoadException(s"failed to load $libraryPath: $m", t)
    }
  }

  /**
   * Install a Spark catalog UDF under `name` so that SQL and DataFrame name resolution succeed.
   *
   * The stub only ever throws: a native UDF that reaches the JVM means Comet did not replace the
   * expression with a native call. Note that the closure Spark keeps is not the one passed here,
   * because `functions.udf` wraps the `UDFn` it is handed, which is why the serde cannot
   * recognize this registration by identity and has to match on the name alone. See
   * [[https://github.com/apache/datafusion-comet/issues/5295]].
   */
  private def installCatalogStub(
      spark: SparkSession,
      name: String,
      inputTypes: Seq[DataType],
      returnType: DataType,
      deterministic: Boolean): Unit = {
    val u: UserDefinedFunction = inputTypes.size match {
      case 0 =>
        udf(() => throw new CometNativeUdfNotEvaluatedException(name), returnType)
      case 1 =>
        udf((_: Any) => throw new CometNativeUdfNotEvaluatedException(name), returnType)
      case 2 =>
        udf((_: Any, _: Any) => throw new CometNativeUdfNotEvaluatedException(name), returnType)
      case 3 =>
        udf(
          (_: Any, _: Any, _: Any) => throw new CometNativeUdfNotEvaluatedException(name),
          returnType)
      case 4 =>
        udf(
          (_: Any, _: Any, _: Any, _: Any) => throw new CometNativeUdfNotEvaluatedException(name),
          returnType)
      case n =>
        throw new IllegalArgumentException(
          s"native UDF '$name' arity $n not supported by stub. Reduce arity " +
            "or open a feature request to extend stub coverage.")
    }
    val finalUdf = if (deterministic) u else u.asNondeterministic()
    spark.udf.register(name, finalUdf)
  }
}
