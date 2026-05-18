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
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

import org.apache.comet.codegen.{CometBatchKernel, CometBatchKernelCodegen}
import org.apache.comet.codegen.CometBatchKernelCodegen.{ArrayColumnSpec, ArrowColumnSpec, MapColumnSpec, ScalarColumnSpec, StructColumnSpec, StructFieldSpec}
import org.apache.comet.udf.CometUDF

/**
 * Arrow-direct codegen dispatcher. For each `(bound expression, input Arrow schema)` pair,
 * compiles a specialized [[CometBatchKernel]] on first encounter and caches it.
 *
 * Arg 0 is a `VarBinaryVector` scalar carrying the closure-serialized bound `Expression` bytes.
 * Args 1..N are the data columns the `BoundReference`s read, in ordinal order. The bytes
 * self-describe the expression so the path works in cluster mode without executor-side state.
 *
 * Three lifetime scopes:
 *   - JVM-wide bytecode dedup via `CodeGenerator.compile`'s source-keyed Guava cache. Stateless.
 *   - Per-task: this instance, lifetime managed by `CometUdfBridge.INSTANCES` keyed on
 *     `taskAttemptId` and dropped via `TaskCompletionListener`. Holds [[kernelCache]], so the
 *     deserialized `boundExpr` (which carries mutable state like `NamedLambdaVariable.value` for
 *     HOFs) is not shared across concurrent tasks. Mirrors Spark's per-task closure-deserialize
 *     model.
 *   - Per-partition: [[activeKernel]] for kernel mutable state (`Rand`'s `XORShiftRandom`,
 *     `MonotonicallyIncreasingID`'s counter) that advances across batches and resets across
 *     partitions.
 *
 * Concurrency: [[evaluate]] takes `this.synchronized` for the cache lookup, kernel allocation,
 * and `process` call. A single Spark task can have multiple concurrent JNI callers because
 * DataFusion operators like `HashJoinExec` pipeline build/probe via `OnceAsync` (`tokio::spawn`),
 * so multiple Tokio worker threads call back into one task's dispatcher. The kernel keeps
 * per-batch state (`col0`, `rowIdx`) in instance fields, so concurrent `process` calls on a
 * shared kernel would race; the lock serializes them. Cross-task parallelism is unaffected.
 *
 * Spark's `BufferedRowIterator` is single-threaded per task by construction, so per-task
 * throughput here matches Spark's; probe-side work, the bulk of UDF eval, is serial in either.
 *
 * TODO(udf-codegen-pool): if intra-task UDF parallelism shows up as a bottleneck (large build
 * sides with heavy UDFs), replace the single `activeKernel` with a per-key kernel pool and
 * externalize per-partition stateful counters into the dispatcher.
 */
class CometScalaUDFCodegen extends CometUDF {

  /**
   * Per-task `(serialized-bytes, specs) -> compiled kernel + bound expression`. Per-task scope is
   * load-bearing for HOF correctness: HOFs mutate `NamedLambdaVariable.value` per element, and a
   * JVM-wide cache would race across concurrent tasks running the same query. Compile work stays
   * deduped JVM-wide via `CodeGenerator.compile`'s source cache; only the `boundExpr` Java object
   * is per-task.
   *
   * Guarded by `this.synchronized` in [[evaluate]].
   */
  private val kernelCache
      : mutable.Map[CometScalaUDFCodegen.CacheKey, CometScalaUDFCodegen.CacheEntry] =
    mutable.HashMap.empty

  // Active kernel state. Guarded by `this.synchronized` in [[evaluate]].
  private var activeKernel: CometBatchKernel = _
  private var activeKey: CometScalaUDFCodegen.CacheKey = _
  private var activePartition: Int = -1

  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
    require(
      inputs.length >= 1,
      "CometScalaUDFCodegen requires at least 1 input (serialized expression), " +
        s"got ${inputs.length}")
    val exprVec = inputs(0).asInstanceOf[VarBinaryVector]
    require(
      exprVec.getValueCount >= 1 && !exprVec.isNull(0),
      "CometScalaUDFCodegen requires non-null serialized expression bytes at arg 0")
    val bytes = exprVec.get(0)

    // TODO(dict-encoded): kernels assume materialized inputs; dict-encoded vectors would fail the
    // cast in `specFor` below. Fix is to materialize at the dispatcher (via
    // `CDataDictionaryProvider`) or widen `emitTypedGetters` with a dict-index + lookup path.

    val numDataCols = inputs.length - 1
    val dataCols = new Array[ValueVector](numDataCols)
    val specs = new Array[ArrowColumnSpec](numDataCols)
    var di = 0
    while (di < numDataCols) {
      val v = inputs(di + 1)
      dataCols(di) = v
      specs(di) = specFor(v)
      di += 1
    }
    val n = numRows
    val specsSeq = specs.toIndexedSeq

    val key = CometScalaUDFCodegen.CacheKey(ByteBuffer.wrap(bytes), specsSeq)

    // Cache lookup, kernel allocation, and `process` run under one lock to serialize concurrent
    // Tokio callers that would otherwise race on the kernel's per-batch instance fields.
    this.synchronized {
      val entry = lookupOrCompile(key, bytes, specsSeq)
      val partitionId = CometScalaUDFCodegen.currentPartitionIndex()
      val kernel = ensureKernel(entry.compiled, key, partitionId)

      val out = CometBatchKernelCodegen.allocateOutput(
        entry.outputField,
        n,
        estimatedOutputBytes(entry.outputType, dataCols))
      try {
        kernel.process(dataCols, out, n)
        out.setValueCount(n)
        out
      } catch {
        case t: Throwable =>
          try out.close()
          catch {
            case _: Throwable => ()
          }
          throw t
      }
    }
  }

  private def ensureKernel(
      compiled: CometBatchKernelCodegen.CompiledKernel,
      key: CometScalaUDFCodegen.CacheKey,
      partitionId: Int): CometBatchKernel = {
    if (activeKernel == null || activePartition != partitionId || activeKey != key) {
      activeKernel = compiled.newInstance()
      activeKernel.init(partitionId)
      activeKey = key
      activePartition = partitionId
    }
    activeKernel
  }

  private def lookupOrCompile(
      key: CometScalaUDFCodegen.CacheKey,
      bytes: Array[Byte],
      specs: IndexedSeq[ArrowColumnSpec]): CometScalaUDFCodegen.CacheEntry = {
    val existing = kernelCache.get(key)
    if (existing.isDefined) {
      CometScalaUDFCodegen.cacheHitCount.incrementAndGet()
      existing.get
    } else {
      val loader = Option(Thread.currentThread().getContextClassLoader)
        .getOrElse(classOf[Expression].getClassLoader)
      val rawExpr = SparkEnv.get.closureSerializer
        .newInstance()
        .deserialize[Expression](ByteBuffer.wrap(bytes), loader)
      val boundExpr = rewriteBoundReferences(rawExpr, specs)
      val compiled = CometBatchKernelCodegen.compile(boundExpr, specs)
      val outputField =
        Utils.toArrowField("codegen_result", boundExpr.dataType, nullable = true, "UTC")
      val entry = CometScalaUDFCodegen.CacheEntry(compiled, boundExpr.dataType, outputField)
      kernelCache.put(key, entry)
      CometScalaUDFCodegen.compileCount.incrementAndGet()
      CometScalaUDFCodegen.recordCompiledSignature(specs, boundExpr.dataType)
      entry
    }
  }

  /**
   * Walk the bound tree and tighten any `BoundReference(ord, dt, nullable=true)` to
   * `nullable=false` when the corresponding input column is non-nullable for this batch.
   */
  private def rewriteBoundReferences(
      expr: Expression,
      specs: IndexedSeq[ArrowColumnSpec]): Expression = {
    expr.transform {
      case BoundReference(ord, dt, true)
          if ord >= 0 && ord < specs.length && !specs(ord).nullable =>
        BoundReference(ord, dt, nullable = false)
      case other => other
    }
  }

  /**
   * Per-batch nullability, baked into the cache key. Different nullability compiles a different
   * kernel: the non-nullable variant emits `return false` from `isNullAt` and lets Spark's
   * `BoundReference.doGenCode` skip the null branch at source level.
   *
   * Workloads that flip nullability frequently can cache up to `2^numCols` kernel variants per
   * expression; common-case stable nullability stays at one.
   */
  private def nullable(v: ValueVector): Boolean = v.getNullCount != 0

  /**
   * Build the compile-time spec for one input Arrow vector. Recurses on complex types. Spark
   * `DataType`s on complex children come from [[Utils.fromArrowField]].
   */
  private def specFor(v: ValueVector): ArrowColumnSpec = v match {
    case map: MapVector =>
      // MapVector extends ListVector; match it first.
      val struct = map.getDataVector.asInstanceOf[StructVector]
      val keyVec = struct.getChildByOrdinal(0).asInstanceOf[ValueVector]
      val valueVec = struct.getChildByOrdinal(1).asInstanceOf[ValueVector]
      MapColumnSpec(
        nullable = nullable(map),
        keySparkType = Utils.fromArrowField(keyVec.getField),
        valueSparkType = Utils.fromArrowField(valueVec.getField),
        key = specFor(keyVec),
        value = specFor(valueVec))
    case list: ListVector =>
      val child = list.getDataVector
      ArrayColumnSpec(nullable(list), Utils.fromArrowField(child.getField), specFor(child))
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
      StructColumnSpec(nullable(struct), fieldSpecs)
    case _: BitVector | _: TinyIntVector | _: SmallIntVector | _: IntVector | _: BigIntVector |
        _: Float4Vector | _: Float8Vector | _: DecimalVector | _: VarCharVector |
        _: VarBinaryVector | _: DateDayVector | _: TimeStampMicroVector |
        _: TimeStampMicroTZVector =>
      ScalarColumnSpec(v.getClass.asInstanceOf[Class[_ <: ValueVector]], nullable(v))
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
  // `CodeGenerator.compile`'s source cache; these track this dispatcher's per-task cache activity.
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
   * DataType)`. Drops `ArrowColumnSpec.nullable` so a single assertion matches both nullability
   * variants of the same expression.
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
    compiledSignatures.add((specs.map(_.vectorClass), outputType))
  }

  /**
   * Partition index for the kernel's `init`. Expressions whose `doGenCode` calls
   * `addPartitionInitializationStatement` (`Rand`, `Randn`, `Uuid`) reseed mutable state from
   * this. Falls back to 0 when the dispatcher is exercised outside a Spark task (unit tests).
   */
  private def currentPartitionIndex(): Int =
    Option(TaskContext.get()).map(_.partitionId()).getOrElse(0)

  /**
   * Cache key: serialized expression bytes plus per-column compile-time invariants. `hashCode`
   * walks `bytesKey` per lookup, so for large ScalaUDF closures it scales with closure size.
   *
   * TODO(perf-cache-key): if hot, options are a driver-precomputed hash piggybacked through the
   * proto, per-instance last-key memoization, or a two-tier cache keyed on the generated source.
   */
  final case class CacheKey(bytesKey: ByteBuffer, specs: IndexedSeq[ArrowColumnSpec])

  /** Snapshot of dispatcher cache counters and current size. */
  final case class DispatcherStats(compileCount: Long, cacheHitCount: Long, cacheSize: Int) {
    def hitRate: Double =
      if (totalLookups == 0) 0.0 else cacheHitCount.toDouble / totalLookups.toDouble

    def totalLookups: Long = compileCount + cacheHitCount
  }

  private case class CacheEntry(
      compiled: CometBatchKernelCodegen.CompiledKernel,
      outputType: DataType,
      outputField: Field)
}
