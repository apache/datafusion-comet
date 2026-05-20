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
import scala.util.control.NonFatal

import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}

import org.apache.comet.codegen.{CometBatchKernel, CometBatchKernelCodegen}
import org.apache.comet.codegen.CometBatchKernelCodegen.{ArrayColumnSpec, ArrowColumnSpec, MapColumnSpec, ScalarColumnSpec, StructColumnSpec, StructFieldSpec}
import org.apache.comet.udf.CometUDF

/**
 * Arrow-direct codegen dispatcher. For each `(bound expression, input Arrow schema)` pair,
 * compiles a specialized [[CometBatchKernel]] on first encounter, initializes it with the task's
 * partition index, and caches the live instance.
 *
 * Arg 0 is a `VarBinaryVector` scalar carrying the closure-serialized bound `Expression` bytes;
 * args 1..N are the data columns the `BoundReference`s read in ordinal order.
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
 *   | Value: compiled Java class |  | Value: dispatcher object   |  | Value: ready-to-run kernel |
 *   | Scope: JVM, all queries    |  | Scope: one Spark task      |  |        with state primed   |
 *   |        share it            |  |                            |  | Scope: one Spark task      |
 *   | Owner: Spark               |  | Owner: Comet               |  |        (lives inside 2)    |
 *   |                            |  |                            |  | Owner: Comet               |
 *   +----------------------------+  +----------------------------+  +----------------------------+
 * }}}
 *
 * Stateful expressions (`Rand`, `MonotonicallyIncreasingID`) advance inside the per-task kernel
 * across batches.
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
   * Per-task cache keyed on serialized expression bytes plus per-column specs. The deserialized
   * `boundExpr` carries mutable state (`NamedLambdaVariable.value` for HOFs, `Rand`'s
   * `XORShiftRandom`) that must not be shared across concurrent tasks running the same query;
   * keeping the cache per-task gives each task its own copy. Guarded by `this.synchronized`.
   */
  private val kernelCache
      : mutable.Map[CometScalaUDFCodegen.CacheKey, CometScalaUDFCodegen.CacheEntry] =
    mutable.HashMap.empty

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

    // TODO(dict-encoded): kernels assume materialized inputs. Dict-encoded vectors would fail the
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

    // Cache lookup and `process` run under one lock to serialize concurrent Tokio callers that
    // would otherwise race on the kernel's per-batch instance fields.
    this.synchronized {
      val entry = lookupOrCompile(key, bytes, specsSeq)

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
      bytes: Array[Byte],
      specs: IndexedSeq[ArrowColumnSpec]): CometScalaUDFCodegen.CacheEntry = {
    assert(Thread.holdsLock(this), "lookupOrCompile must run under this.synchronized")
    kernelCache.get(key) match {
      case Some(entry) =>
        CometScalaUDFCodegen.cacheHitCount.incrementAndGet()
        entry
      case None =>
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
        kernel.init(CometScalaUDFCodegen.currentPartitionIndex())
        val outputField = CometBatchKernelCodegen.toFfiArrowField(
          "codegen_result",
          boundExpr.dataType,
          boundExpr.nullable)
        val entry =
          CometScalaUDFCodegen.CacheEntry(compiled, kernel, boundExpr.dataType, outputField)
        kernelCache.put(key, entry)
        CometScalaUDFCodegen.compileCount.incrementAndGet()
        CometScalaUDFCodegen.recordCompiledSignature(specs, boundExpr.dataType)
        entry
    }
  }

  /**
   * Build the compile-time spec for one input Arrow vector. Recurses on complex types.
   *
   * Top-level `nullable=true` is hardcoded: the cache key does not specialize on per-batch null
   * density. Schema-declared nullability still reaches the kernel via `BoundReference.nullable`
   * embedded in `bytesKey`, so `BoundReference.doGenCode` elides its own `isNullAt` probe on
   * non-null columns. `StructFieldSpec.nullable` reads `field.isNullable` from Arrow metadata,
   * which is a schema property and therefore stable across batches.
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
        _: VarBinaryVector | _: DateDayVector | _: TimeStampMicroVector |
        _: TimeStampMicroTZVector =>
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
      kernel: CometBatchKernel,
      outputType: DataType,
      outputField: Field)
}
