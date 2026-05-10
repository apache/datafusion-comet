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
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

/**
 * Public entry point for registering Rust scalar UDFs with Comet.
 *
 * See `docs/source/user-guide/latest/custom-rust-udfs.md` (added in Task 22) for an end-to-end
 * walkthrough.
 */
object CometRustUDF {

  private val mapper: ObjectMapper = new ObjectMapper()

  /**
   * Register a single Rust UDF with an explicit signature.
   *
   * Validates the library on the driver, throws if loading fails or the declared signature
   * differs from what the library reports. On success: a stub Spark catalog UDF is registered (so
   * SQL/DataFrame reference checks pass), the driver-side registry is updated, and the conf key
   * `spark.comet.rustUdfs` is updated so executors see the registration.
   */
  def register(
      spark: SparkSession,
      name: String,
      libraryPath: String,
      inputTypes: Seq[DataType],
      returnType: DataType,
      deterministic: Boolean = true): Unit = {
    val described = describeOne(libraryPath, name)
    requireSignatureMatch(name, described, inputTypes, returnType)
    installCatalogStub(spark, name, inputTypes, returnType, deterministic)
    val meta = RustUdfMetadata(libraryPath, inputTypes, returnType, deterministic)
    CometRustUdfRegistry.instance.register(name, meta)
    propagateConf(spark, name, meta)
  }

  /**
   * Register every UDF exposed by `libraryPath`, using each UDF's discovered signature. Returns
   * the list of names registered.
   */
  def registerAll(spark: SparkSession, libraryPath: String): Seq[String] = {
    val descs = describeAll(libraryPath)
    descs.map { d =>
      val args = d.args.map(parseSparkType)
      val ret = parseSparkType(d.returnType)
      installCatalogStub(spark, d.name, args, ret, deterministic = true)
      val meta = RustUdfMetadata(libraryPath, args, ret, deterministic = true)
      CometRustUdfRegistry.instance.register(d.name, meta)
      propagateConf(spark, d.name, meta)
      d.name
    }
  }

  // -------- internals --------

  private case class Described(
      name: String,
      args: Seq[String],
      returnType: String,
      volatility: Int)

  private def describeOne(libraryPath: String, name: String): Described = {
    val json =
      invokeBridge(() => CometRustUdfBridge.validateLibrary(libraryPath, name), libraryPath)
    parseDescribed(json)
  }

  private def describeAll(libraryPath: String): Seq[Described] = {
    val json = invokeBridge(() => CometRustUdfBridge.listUdfs(libraryPath), libraryPath)
    val arr = mapper.readTree(json)
    require(arr.isArray, s"listUdfs returned non-array JSON: $json")
    val out = scala.collection.mutable.ArrayBuffer[Described]()
    val it = arr.elements()
    while (it.hasNext) out += parseDescribed(it.next().toString)
    out.toSeq
  }

  private def invokeBridge(call: () => String, libraryPath: String): String = {
    Try(call()).recover { case t: Throwable => throw classifyNativeError(libraryPath, t) }.get
  }

  private def parseDescribed(json: String): Described = {
    val node = mapper.readTree(json).asInstanceOf[ObjectNode]
    val argsArr = node.get("args").asInstanceOf[ArrayNode]
    val args = scala.collection.mutable.ArrayBuffer[String]()
    val it = argsArr.elements()
    while (it.hasNext) args += it.next().asText()
    Described(
      name = node.get("name").asText(),
      args = args.toSeq,
      returnType = node.get("return_type").asText(),
      volatility = node.get("volatility").asInt())
  }

  private def classifyNativeError(libraryPath: String, t: Throwable): RuntimeException = {
    val m = Option(t.getMessage).getOrElse("")
    if (m.contains("ABI") || m.contains("missing required symbol")) {
      new CometRustUdfAbiException(m)
    } else if (m.contains("not found in")) {
      new java.util.NoSuchElementException(m)
    } else {
      new CometRustUdfLoadException(s"failed to load $libraryPath: $m", t)
    }
  }

  private def requireSignatureMatch(
      name: String,
      d: Described,
      declaredArgs: Seq[DataType],
      declaredReturn: DataType): Unit = {
    val nativeArgs = d.args.map(parseSparkType)
    val nativeReturn = parseSparkType(d.returnType)
    if (nativeArgs != declaredArgs) {
      throw new CometRustUdfSignatureException(
        s"UDF '$name' arg types: native=$nativeArgs, declared=$declaredArgs")
    }
    if (nativeReturn != declaredReturn) {
      throw new CometRustUdfSignatureException(
        s"UDF '$name' return type: native=$nativeReturn, declared=$declaredReturn")
    }
  }

  /** Parse a Comet-emitted Arrow type string into a Spark DataType. */
  private def parseSparkType(s: String): DataType = s match {
    case "Boolean" => BooleanType
    case "Int8" => ByteType
    case "Int16" => ShortType
    case "Int32" => IntegerType
    case "Int64" => LongType
    case "Float32" => FloatType
    case "Float64" => DoubleType
    case "Utf8" | "LargeUtf8" => StringType
    case "Binary" | "LargeBinary" => BinaryType
    case other =>
      // For complex/nested types: attempt to parse via Spark's DDL parser.
      // The mapping is approximate for v1 — primitives are exact;
      // nested types may need refinement in a follow-up.
      DataType.fromDDL(other)
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

  private def propagateConf(spark: SparkSession, name: String, meta: RustUdfMetadata): Unit = {
    val key = "spark.comet.rustUdfs"
    val existing = Option(spark.conf.getOption(key).orNull).getOrElse("")
    val entry =
      mapper.writeValueAsString(java.util.Map.of("name", name, "libraryPath", meta.libraryPath))
    val updated = if (existing.isEmpty) entry else s"$existing;$entry"
    spark.conf.set(key, updated)
  }
}
