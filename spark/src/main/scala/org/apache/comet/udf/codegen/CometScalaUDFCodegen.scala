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

package org.apache.comet.udf.codegen

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

import org.apache.comet.codegen.{CometBatchKernel, CometBatchKernelCodegen}
import org.apache.comet.codegen.CometBatchKernelCodegen.{ArrayColumnSpec, ArrowColumnSpec, MapColumnSpec, ScalarColumnSpec, StructColumnSpec, StructFieldSpec}
import org.apache.comet.udf.CometUDF

/**
 * Arrow-direct codegen dispatcher. For each `(bound expression, input Arrow schema)` pair,
 * compiles a specialized [[CometBatchKernel]] on first encounter, initializes it with the index
 * of the partition the calling native plan computes, and caches the live instance.
 *
 * Arg 0 is a `VarBinaryVector` scalar carrying the [[CometScalaUDFCodegen.digest]] of the
 * closure-serialized bound `Expression`; arg 1 is a `VarBinaryVector` scalar carrying the
 * serialized bytes themselves; args 2..N are the data columns the `BoundReference`s read in
 * ordinal order. A batch finds its kernel by the digest, so the serialized expression, several KB
 * for a Scala UDF's closure, is read only to compile a kernel on a cache miss.
 *
 * Caching hierarchy, broadest scope on the left:
 * {{{
 *   +----------------------------+  +----------------------------+  +----------------------------+
 *   | 1. JVM bytecode cache      |  | 2. Per-task dispatcher     |  | 3. Per-task kernel cache   |
 *   |    (Spark's CodeGenerator) |  |    (CometUdfBridge.        |  |    (kernelCache field)     |
 *   |                            |  |     INSTANCES)             |  |                            |
 *   +----------------------------+  +----------------------------+  +----------------------------+
 *   | Key:   generated Java      |  | Key:   task + UDF class    |  | Key:   bound expression +  |
 *   |        source              |  |                            |  |        input column shapes |
 *   | Value: compiled Java class |  | Value: dispatcher object   |  |        (+ native plan if   |
 *   | Scope: JVM, all queries    |  | Scope: one Spark task      |  |        nondeterministic)   |
 *   |        share it            |  |                            |  | Value: ready-to-run kernel |
 *   | Owner: Spark               |  | Owner: Comet               |  |        with state primed   |
 *   |                            |  |                            |  | Scope: one Spark task      |
 *   |                            |  |                            |  |        (lives inside 2),   |
 *   |                            |  |                            |  |        or one native plan  |
 *   |                            |  |                            |  |        if nondeterministic |
 *   |                            |  |                            |  | Owner: Comet               |
 *   +----------------------------+  +----------------------------+  +----------------------------+
 * }}}
 *
 * Stateful expressions (`Rand`, `MonotonicallyIncreasingID`) advance inside the per-plan kernel
 * across batches. `CometExecIterator.close` drops a plan's kernels through `releasePlan`.
 *
 * `evaluate` runs under `this.synchronized` because DataFusion operators like `HashJoinExec`
 * pipeline build/probe via `OnceAsync` (`tokio::spawn`), so multiple Tokio worker threads can
 * call back into one task's dispatcher. The kernel's per-batch instance fields would race
 * otherwise.
 *
 * TODO(udf-codegen-pool): if intra-task UDF parallelism shows up as a bottleneck, replace the
 * per-key kernel instance with a pool and externalize per-partition counters.
 */
class CometScalaUDFCodegen extends CometUDF with Logging {

  /**
   * Per-task cache keyed on the serialized expression's digest plus per-column specs. The
   * deserialized `boundExpr` carries mutable state (`NamedLambdaVariable.value` for HOFs,
   * `Rand`'s `XORShiftRandom`) that must not be shared across concurrent tasks running the same
   * query; keeping the cache per-task gives each task its own copy. A nondeterministic kernel is
   * seeded from the partition its plan computes, so its key also holds the plan: each plan a task
   * runs, such as each parent partition of a coalesce, gets its own, dropped by `releasePlan`
   * when the plan closes. Guarded by `this.synchronized`.
   */
  private val kernelCache
      : mutable.Map[CometScalaUDFCodegen.CacheKey, CometScalaUDFCodegen.CacheEntry] =
    mutable.HashMap.empty

  // Kernels shared by every plan stay until the task ends.
  override def releasePlan(planId: Long): Unit = this.synchronized {
    kernelCache.keys.filter(_.planId == planId).toList.foreach(kernelCache.remove)
  }

  /** Plan ids of the cached kernels, `NoPlan` for a shared one. */
  private[comet] def cachedPlanIds: List[Long] = this.synchronized {
    kernelCache.keysIterator.map(_.planId).toList.sorted
  }

  // Callers that bypass the bridge (unit tests, benchmarks) have no native plan.
  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector =
    evaluate(inputs, numRows, partitionIndex = 0, planId = CometScalaUDFCodegen.NoPlan)

  override def evaluate(
      inputs: Array[ValueVector],
      numRows: Int,
      partitionIndex: Int,
      planId: Long): ValueVector = {
    require(
      inputs.length >= 2,
      "CometScalaUDFCodegen requires at least 2 inputs (expression digest and serialized " +
        s"expression), got ${inputs.length}")
    val digest = binaryScalar(inputs(0), "expression digest at arg 0")
    require(
      digest.length == CometScalaUDFCodegen.DigestLength,
      s"CometScalaUDFCodegen requires a ${CometScalaUDFCodegen.DigestLength}-byte expression " +
        s"digest at arg 0, got ${digest.length} bytes")

    // TODO(dict-encoded): kernels assume materialized inputs. Dict-encoded vectors would fail the
    // cast in `specFor` below. Fix is to materialize at the dispatcher (via
    // `CDataDictionaryProvider`) or widen `emitTypedGetters` with a dict-index + lookup path.

    val numDataCols = inputs.length - 2
    val dataCols = new Array[ValueVector](numDataCols)
    val specs = new Array[ArrowColumnSpec](numDataCols)
    var di = 0
    while (di < numDataCols) {
      val v = inputs(di + 2)
      dataCols(di) = v
      specs(di) = specFor(v)
      di += 1
    }
    val n = numRows
    val specsSeq = specs.toIndexedSeq

    val key = CometScalaUDFCodegen.CacheKey(planId, ByteBuffer.wrap(digest), specsSeq)

    // Cache lookup and `process` run under one lock to serialize concurrent Tokio callers that
    // would otherwise race on the kernel's per-batch instance fields.
    this.synchronized {
      val entry = lookupOrCompile(key, inputs(1), specsSeq, partitionIndex)

      val out = CometBatchKernelCodegen.allocateOutput(
        entry.outputField,
        n,
        estimatedOutputBytes(entry.outputType, dataCols))
      try {
        entry.kernel.process(dataCols, out, n)
        out.setValueCount(n)
        out
      } catch {
        case t: Throwable =>
          try out.close()
          catch {
            case NonFatal(_) => ()
          }
          throw t
      }
    }
  }

  private def lookupOrCompile(
      key: CometScalaUDFCodegen.CacheKey,
      exprVec: ValueVector,
      specs: IndexedSeq[ArrowColumnSpec],
      partitionIndex: Int): CometScalaUDFCodegen.CacheEntry = {
    assert(Thread.holdsLock(this), "lookupOrCompile must run under this.synchronized")
    // A deterministic kernel never reads the partition index, so one instance under the planless
    // key serves every plan in the task. Only a kernel with a nondeterministic node is stored per
    // plan.
    val sharedKey = key.copy(planId = CometScalaUDFCodegen.NoPlan)
    kernelCache.get(sharedKey).orElse(kernelCache.get(key)) match {
      case Some(entry) =>
        CometScalaUDFCodegen.cacheHitCount.incrementAndGet()
        entry
      case None =>
        val bytes = binaryScalar(exprVec, "serialized expression at arg 1")
        val loader = Option(Thread.currentThread().getContextClassLoader)
          .getOrElse(classOf[Expression].getClassLoader)
        val boundExpr =
          try {
            SparkEnv.get.closureSerializer
              .newInstance()
              .deserialize[Expression](ByteBuffer.wrap(bytes), loader)
          } catch {
            case NonFatal(t) =>
              logError(
                "CometScalaUDFCodegen: closure-deserialize failed " +
                  s"(bytes=${bytes.length}, specs=$specs)",
                t)
              throw t
          }
        val compiled = CometBatchKernelCodegen.compile(boundExpr, specs)
        val kernel = compiled.newInstance()
        kernel.init(partitionIndex)
        val outputField = CometBatchKernelCodegen.toFfiArrowField(
          "codegen_result",
          boundExpr.dataType,
          boundExpr.nullable)
        val entry =
          CometScalaUDFCodegen.CacheEntry(compiled, kernel, boundExpr.dataType, outputField)
        // Walks the tree, because `deterministic` is not transitive everywhere. `Invoke` skips its
        // `targetObject`, which is where Spark 4's `make_valid_utf8` puts its input.
        val perPlan = boundExpr.exists(!_.deterministic)
        kernelCache.put(if (perPlan) key else sharedKey, entry)
        CometScalaUDFCodegen.compileCount.incrementAndGet()
        CometScalaUDFCodegen.recordCompiledSignature(specs, boundExpr.dataType)
        entry
    }
  }

  /** The value of a binary scalar argument, which arrives as a length-1 vector. */
  private def binaryScalar(v: ValueVector, what: String): Array[Byte] = {
    val vec = v.asInstanceOf[VarBinaryVector]
    require(
      vec.getValueCount >= 1 && !vec.isNull(0),
      s"CometScalaUDFCodegen requires a non-null $what")
    vec.get(0)
  }

  /**
   * Build the compile-time spec for one input Arrow vector. Recurses on complex types.
   *
   * The vector classes matched here must cover every Spark type
   * `CometBatchKernelCodegen.isSupportedDataType` admits, because that predicate is what
   * `canHandle` gates the plan on. A type accepted at plan time but unmatched here throws at
   * execute time, when the operator can no longer fall back to Spark.
   *
   * Top-level `nullable=true` is hardcoded: the cache key does not specialize on per-batch null
   * density. Schema-declared nullability still reaches the kernel via `BoundReference.nullable`
   * embedded in the serialized expression, which the key's digest covers, so
   * `BoundReference.doGenCode` elides its own `isNullAt` probe on non-null columns.
   * `StructFieldSpec.nullable` reads `field.isNullable` from Arrow metadata, which is a schema
   * property and therefore stable across batches.
   */
  private def specFor(v: ValueVector): ArrowColumnSpec = v match {
    case map: MapVector =>
      // MapVector extends ListVector, match it first.
      val struct = map.getDataVector.asInstanceOf[StructVector]
      val keyVec = struct.getChildByOrdinal(0).asInstanceOf[ValueVector]
      val valueVec = struct.getChildByOrdinal(1).asInstanceOf[ValueVector]
      MapColumnSpec(
        nullable = true,
        keySparkType = Utils.fromArrowField(keyVec.getField),
        valueSparkType = Utils.fromArrowField(valueVec.getField),
        key = specFor(keyVec),
        value = specFor(valueVec))
    case list: ListVector =>
      val child = list.getDataVector
      ArrayColumnSpec(nullable = true, Utils.fromArrowField(child.getField), specFor(child))
    case struct: StructVector =>
      val fieldSpecs = (0 until struct.size()).map { fi =>
        val childVec = struct.getChildByOrdinal(fi).asInstanceOf[ValueVector]
        val field = struct.getField.getChildren.get(fi)
        StructFieldSpec(
          name = field.getName,
          sparkType = Utils.fromArrowField(field),
          nullable = field.isNullable,
          child = specFor(childVec))
      }
      StructColumnSpec(nullable = true, fieldSpecs)
    case _: BitVector | _: TinyIntVector | _: SmallIntVector | _: IntVector | _: BigIntVector |
        _: Float4Vector | _: Float8Vector | _: DecimalVector | _: VarCharVector |
        _: VarBinaryVector | _: DateDayVector | _: DurationVector | _: TimeNanoVector |
        _: TimeStampMicroVector | _: TimeStampMicroTZVector | _: IntervalYearVector |
        _: IntervalMonthDayNanoVector =>
      ScalarColumnSpec(v.getClass.asInstanceOf[Class[_ <: ValueVector]], nullable = true)
    case other =>
      throw new UnsupportedOperationException(
        s"CometScalaUDFCodegen: unsupported Arrow vector ${other.getClass.getSimpleName}")
  }

  /**
   * Sum of variable-width input data buffer sizes as an upper bound for typical transform outputs
   * (replace, upper, lower, substring, concat). Underestimates are still corrected by `setSafe`;
   * this just reduces the odds of mid-loop reallocation.
   */
  private def estimatedOutputBytes(outputType: DataType, dataCols: Array[ValueVector]): Int = {
    outputType match {
      case _: StringType | _: BinaryType =>
        var sum = 0
        var i = 0
        while (i < dataCols.length) {
          dataCols(i) match {
            case v: BaseVariableWidthVector => sum += v.getDataBuffer.writerIndex().toInt
            case _ => // no size hint for fixed-width vector types
          }
          i += 1
        }
        sum
      case _ => -1
    }
  }
}

object CometScalaUDFCodegen {

  // JVM-wide counters across all per-task instances. Compile work is deduped JVM-wide via
  // `CodeGenerator.compile`'s source cache. These track this dispatcher's per-task cache activity.
  private val compileCount = new AtomicLong(0)
  private val cacheHitCount = new AtomicLong(0)

  // Append-only set of distinct compiled-kernel signatures. Lets tests assert specialization
  // shape (vector-class / dataType combinations the dispatcher emitted) and that composed
  // subtrees fuse into one kernel. Per-task caches are dropped on completion, leaving no other
  // place to observe the set across runs.
  private val compiledSignatures =
    Collections.synchronizedSet(
      new java.util.HashSet[(IndexedSeq[Class[_ <: ValueVector]], DataType)]())

  /** Snapshot of JVM-wide counters and distinct-signature count. */
  def stats(): DispatcherStats =
    DispatcherStats(compileCount.get(), cacheHitCount.get(), compiledSignatures.size())

  /** Reset counters; leaves the signature set intact. Tests only. */
  def resetStats(): Unit = {
    compileCount.set(0)
    cacheHitCount.set(0)
  }

  /**
   * Distinct compiled-kernel signatures: `(input vector classes in ordinal order, output Spark
   * DataType)`. `ArrowColumnSpec.nullable` is intentionally omitted so the signature reflects
   * what would specialize the kernel regardless of any future per-batch nullability variants.
   */
  def snapshotCompiledSignatures(): Set[(IndexedSeq[Class[_ <: ValueVector]], DataType)] = {
    import scala.jdk.CollectionConverters._
    compiledSignatures.synchronized {
      compiledSignatures.iterator().asScala.toSet
    }
  }

  private[codegen] def recordCompiledSignature(
      specs: IndexedSeq[ArrowColumnSpec],
      outputType: DataType): Unit = {
    val _ = compiledSignatures.add((specs.map(_.vectorClass), outputType))
  }

  /**
   * Plan id for kernels shared by every plan in a task, and for callers that bypass the bridge.
   */
  private[comet] val NoPlan = -1L

  /** Length in bytes of a [[digest]]. */
  val DigestLength = 32

  /**
   * Identifies a closure-serialized bound expression in the kernel cache. The serde computes it
   * once on the driver and ships it as arg 0, next to the bytes, so the per-batch lookup hashes
   * 32 bytes instead of the whole serialized expression. SHA-256 rather than a cheaper hash,
   * because a hit is trusted without comparing the bytes: two expressions whose digests collided
   * would share a kernel.
   */
  def digest(serialized: Array[Byte]): Array[Byte] =
    MessageDigest.getInstance("SHA-256").digest(serialized)

  /**
   * Cache key: calling native plan (`NoPlan` for a deterministic expression), the bound
   * expression's [[digest]] plus per-column compile-time invariants.
   */
  final case class CacheKey(planId: Long, digest: ByteBuffer, specs: IndexedSeq[ArrowColumnSpec])

  /** Snapshot of dispatcher cache counters and current size. */
  final case class DispatcherStats(compileCount: Long, cacheHitCount: Long, cacheSize: Int) {
    def hitRate: Double =
      if (totalLookups == 0) 0.0 else cacheHitCount.toDouble / totalLookups.toDouble

    def totalLookups: Long = compileCount + cacheHitCount
  }

  private case class CacheEntry(
      compiled: CometBatchKernelCodegen.CompiledKernel,
      kernel: CometBatchKernel,
      outputType: DataType,
      outputField: Field)
}
