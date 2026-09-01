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

package org.apache.comet.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.comet.{CometCollectLimitExec, CometColumnarToRowExec, CometColumnarToRowViewExec, CometMapInBatchExec, CometNativeColumnarToRowExec, CometNativeWriteExec, CometPlan, CometSparkToColumnarExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.comet.shims.{MapInBatchInfo, ShimCometMapInBatch}
import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, WriteFilesExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.NativeOptIn
import org.apache.comet.shims.ShimSQLConf

// This rule is responsible for eliminating redundant transitions between row-based and
// columnar-based operators for Comet. Currently, three potential redundant transitions are:
// 1. `ColumnarToRowExec` on top of an ending `CometCollectLimitExec` operator, which is
//    redundant as `CometCollectLimitExec` already wraps a `ColumnarToRowExec` for row-based
//    output.
// 2. Consecutive operators of `CometSparkToColumnarExec` and `ColumnarToRowExec`.
// 3. AQE inserts an additional `CometSparkToColumnarExec` in addition to the one inserted in the
//    original plan.
//
// Note about the first case: The `ColumnarToRowExec` was added during
// ApplyColumnarRulesAndInsertTransitions' insertTransitions phase when Spark requests row-based
// output such as a `collect` call. It's correct to add a redundant `ColumnarToRowExec` for
// `CometExec`. However, for certain operators such as `CometCollectLimitExec` which overrides
// `executeCollect`, the redundant `ColumnarToRowExec` makes the override ineffective.
//
// Note about the second case: When `spark.comet.sparkToColumnar.enabled` is set, Comet will add
// `CometSparkToColumnarExec` on top of row-based operators first, but the downstream operator
// only takes row-based input as it's a vanilla Spark operator(as Comet cannot convert it for
// various reasons) or Spark requests row-based output such as a `collect` call. Spark will adds
// another `ColumnarToRowExec` on top of `CometSparkToColumnarExec`. In this case, the pair could
// be removed.
case class EliminateRedundantTransitions(session: SparkSession)
    extends Rule[SparkPlan]
    with ShimCometMapInBatch
    with ShimSQLConf {

  private lazy val showTransformations = CometConf.COMET_EXPLAIN_TRANSFORMATIONS.get()

  override def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = _apply(plan)
    if (showTransformations && !newPlan.fastEquals(plan)) {
      logInfo(s"""
           |=== Applying Rule $ruleName ===
           |${sideBySide(plan.treeString, newPlan.treeString).mkString("\n")}
           |""".stripMargin)
    }
    newPlan
  }

  private def _apply(plan: SparkPlan): SparkPlan = {
    val eliminatedPlan = plan transformUp {
      case ColumnarToRowExec(shuffleExchangeExec: CometShuffleExchangeExec)
          if plan.conf.adaptiveExecutionEnabled =>
        shuffleExchangeExec
      case ColumnarToRowExec(sparkToColumnar: CometSparkToColumnarExec) =>
        if (sparkToColumnar.child.supportsColumnar) {
          // For Spark Columnar to Comet Columnar, we should keep the ColumnarToRowExec
          ColumnarToRowExec(sparkToColumnar.child)
        } else {
          // For Spark Row to Comet Columnar, we should remove ColumnarToRowExec
          // and CometSparkToColumnarExec
          sparkToColumnar.child
        }
      // Remove unnecessary transition for native writes
      // Write should be final operation in the plan
      case ColumnarToRowExec(nativeWrite: CometNativeWriteExec) =>
        nativeWrite

      // Spark's file writers consume `InternalRow` and never need an `UnsafeRow`, so the
      // materializing transition below a write can be swapped for a zero-copy row view over the
      // Arrow batch. `transformUp` has already rewritten the child into one of the Comet
      // transitions by the time these arms are visited.
      //
      // `plannedWrite` (the default since Spark 3.4) puts the transition under `WriteFilesExec`;
      // with it disabled the write command executes its child directly. Both are handled, and both
      // are gated on the write being unpartitioned and unbucketed (`rowViewSafeForWrite`) and on
      // the schema containing a complex type (`rowView`).
      case w: WriteFilesExec
          if rowViewSafeForWrite(
            w.partitionColumns.nonEmpty,
            w.bucketSpec.isDefined,
            w.fileFormat.getClass.getName) =>
        rowView(w.child).map(c => w.withNewChildren(Seq(c))).getOrElse(w)
      case d @ DataWritingCommandExec(cmd: InsertIntoHadoopFsRelationCommand, _)
          if rowViewSafeForWrite(
            cmd.partitionColumns.nonEmpty || cmd.staticPartitions.nonEmpty,
            cmd.bucketSpec.isDefined,
            cmd.fileFormat.getClass.getName) =>
        rowView(d.child).map(c => d.withNewChildren(Seq(c))).getOrElse(d)

      case c @ ColumnarToRowExec(child) if hasCometNativeChild(child) =>
        val op = createColumnarToRowExec(child)
        if (c.logicalLink.isEmpty) {
          op.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
          op.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
        } else {
          c.logicalLink.foreach(op.setLogicalLink)
        }
        op
      case CometColumnarToRowExec(sparkToColumnar: CometSparkToColumnarExec) =>
        sparkToColumnar.child
      case CometNativeColumnarToRowExec(sparkToColumnar: CometSparkToColumnarExec) =>
        sparkToColumnar.child
      case CometSparkToColumnarExec(child: CometSparkToColumnarExec) => child
      // Replace MapInBatchExec (PythonMapInArrowExec / MapInArrowExec / MapInPandasExec) that has
      // a ColumnarToRow child with CometMapInBatchExec, eliminating the input and output
      // UnsafeProjection copies and keeping the stage columnar. The matchers are
      // version-shimmed: Spark 3.4 / 3.5 return None (they lack the required APIs) and Spark
      // 4.1+ matches the renamed `MapInArrowExec`.
      //
      // Falls back to vanilla Spark when `spark.sql.execution.arrow.useLargeVarTypes` is enabled:
      // Native Comet string/binary vectors use 4-byte offsets. The IPC schema follows these
      // vectors, so the stream is internally consistent, but the worker would receive string /
      // binary instead of the large_string / large_binary input types requested by this conf.
      // Keep the fallback to preserve Spark's input type contract.
      //
      // `EligibleMapInBatch` matches whenever the operator would run natively if the feature were
      // enabled. When it is disabled (the default) we leave the vanilla Spark operator in place
      // but annotate it with a non-fallback `[COMET-INFO]` hint so the user knows the native path
      // exists behind a config flag.
      case p @ EligibleMapInBatch(info, columnarChild) =>
        if (CometConf.COMET_PYARROW_UDF_ENABLED.get()) {
          CometMapInBatchExec(
            info.func,
            info.output,
            columnarChild,
            info.isBarrier,
            info.pythonEvalType)
        } else {
          withInfo(
            p,
            NativeOptIn.message(
              "PyArrow UDFs (mapInArrow/mapInPandas)",
              CometConf.COMET_PYARROW_UDF_ENABLED.key))
        }

      // Spark adds `RowToColumnar` under Comet columnar shuffle. But it's redundant as the
      // shuffle takes row-based input.
      case s @ CometShuffleExchangeExec(
            _,
            RowToColumnarExec(child),
            _,
            _,
            CometColumnarShuffle,
            _) =>
        s.withNewChildren(Seq(child))
    }

    eliminatedPlan match {
      case ColumnarToRowExec(child: CometCollectLimitExec) =>
        child
      case CometColumnarToRowExec(child: CometCollectLimitExec) =>
        child
      case CometNativeColumnarToRowExec(child: CometCollectLimitExec) =>
        child
      case other =>
        other
    }
  }

  private def hasCometNativeChild(op: SparkPlan): Boolean = {
    op match {
      case c: QueryStageExec => hasCometNativeChild(c.plan)
      case c: ReusedExchangeExec => hasCometNativeChild(c.child)
      case _ => op.exists(_.isInstanceOf[CometPlan])
    }
  }

  /**
   * Whether a write can consume the reused, mutable rows produced by
   * [[CometColumnarToRowViewExec]] rather than materialized `UnsafeRow`s.
   *
   * The row view is only correct for a consumer that finishes with a row before pulling the next
   * one. That holds for `SingleDirectoryDataWriter`, which is what `FileFormatWriter` picks when
   * there are no partition and no bucket columns; it writes the row straight through to the
   * `OutputWriter`, and `BasicWriteTaskStatsTracker.newRow` ignores the row entirely. The
   * partitioned and bucketed writers do not qualify:
   *
   *   - `FileFormatWriter` requires an ordering on the partition/bucket columns, so a `SortExec`
   *     sits between this transition and the writer, and `UnsafeExternalSorter` needs
   *     `UnsafeRow`.
   *   - `DynamicPartitionDataConcurrentWriter` spills through `UnsafeKVExternalSorter.insertKV`,
   *     which is typed on `UnsafeRow`.
   *
   * The format check keeps this to Spark's own `FileFormat` implementations. Their
   * `OutputWriter`s encode each row on the spot (Parquet through `ParquetWriteSupport`, ORC
   * through `OrcSerializer` into a `VectorizedRowBatch`, the text formats directly), whereas a
   * third-party format is free to buffer the `InternalRow` it is handed.
   */
  private def rowViewSafeForWrite(
      partitioned: Boolean,
      bucketed: Boolean,
      fileFormat: String): Boolean =
    CometConf.COMET_WRITE_ROW_VIEW_ENABLED.get() &&
      !partitioned && !bucketed &&
      fileFormat.startsWith("org.apache.spark.sql.execution.datasources.")

  /**
   * Rewrites a Comet columnar-to-row transition into the zero-copy row view. Returns `None` for
   * anything else, which leaves the plan untouched - notably for the `WriteFilesExec` under a
   * `DataWritingCommandExec`, and for a write whose input was never columnar to begin with.
   *
   * Also declines a schema of nothing but flat types. There the `UnsafeProjection` this replaces
   * is a generated fixed-width copy that measures at 0-2% of a Parquet write, which does not pay
   * for the reused-mutable-row hazard. The saving only becomes real once a struct, array or map
   * is in play, because then the projection has to build nested `UnsafeRow` / `UnsafeArrayData`
   * with offset-and-length bookkeeping that `ParquetWriteSupport` immediately walks back out. See
   * `CometParquetWriteBenchmark` for the measurements behind this cut-off.
   */
  private def rowView(plan: SparkPlan): Option[SparkPlan] = plan match {
    case CometColumnarToRowExec(child) if hasComplexType(child.schema) =>
      Some(CometColumnarToRowViewExec(child))
    case CometNativeColumnarToRowExec(child) if hasComplexType(child.schema) =>
      Some(CometColumnarToRowViewExec(child))
    case _ => None
  }

  /** Whether the schema has a struct, array or map anywhere in it. */
  private def hasComplexType(schema: StructType): Boolean = {
    def isComplex(dataType: DataType): Boolean = dataType match {
      case _: StructType | _: ArrayType | _: MapType => true
      case _ => false
    }
    schema.fields.exists(f => isComplex(f.dataType))
  }

  /**
   * If the given plan is a Comet ColumnarToRow transition, returns the columnar child the Python
   * UDF operator can consume directly. By the time this rule runs the earlier
   * `hasCometNativeChild` arm has already rewritten any `ColumnarToRowExec` over a Comet columnar
   * source to one of the Comet variants, so vanilla `ColumnarToRowExec` cannot reach here on a
   * Comet-driven plan and is intentionally not handled.
   */
  private def extractColumnarChild(plan: SparkPlan): Option[SparkPlan] = plan match {
    case CometColumnarToRowExec(child) => Some(child)
    case CometNativeColumnarToRowExec(child) => Some(child)
    // Chained `mapInArrow(udf1).mapInArrow(udf2)`: by the time the outer operator is visited
    // (transformUp is bottom-up) the inner one has already become a `CometMapInBatchExec`, which
    // is itself columnar. There is no row transition between them to strip, so consume its
    // columnar output directly. Its flattened output vectors are `CometVector`s, exactly what
    // `CometMapInBatchExec`'s input path expects.
    case child: CometMapInBatchExec => Some(child)
    case _ => None
  }

  /**
   * Matches the plans that could run natively as `CometMapInBatchExec`, independent of whether
   * the `spark.comet.exec.pyarrowUDF.enabled` feature flag is set. The `transformUp` arm reads
   * that flag to decide between rewriting the operator and merely annotating it with an opt-in
   * hint. Single extractor so the matchers run once per visited plan. Returns `(info,
   * columnarChild)` where `columnarChild` is the Comet columnar producer that
   * `CometMapInBatchExec` will consume directly. Returns `None` (and the arm misses) when
   * `useLargeVarTypes` forces the fallback, when the plan is not one of the version-shimmed
   * MapInArrow / MapInPandas operators, or when the child is not a Comet columnar-to-row
   * transition we can strip.
   */
  private object EligibleMapInBatch {
    def unapply(plan: SparkPlan): Option[(MapInBatchInfo, SparkPlan)] = {
      if (arrowUseLargeVarTypes(plan.conf)) {
        None
      } else {
        matchMapInArrow(plan)
          .orElse(matchMapInPandas(plan))
          .flatMap(info => extractColumnarChild(info.child).map(child => (info, child)))
      }
    }
  }

  /**
   * Creates an appropriate columnar to row transition operator.
   *
   * If native columnar to row conversion is enabled and the schema is supported, uses
   * CometNativeColumnarToRowExec. Otherwise falls back to CometColumnarToRowExec.
   */
  private def createColumnarToRowExec(child: SparkPlan): SparkPlan = {
    val schema = child.schema
    val useNative = CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.get() &&
      CometNativeColumnarToRowExec.supportsSchema(schema)

    if (useNative) {
      CometNativeColumnarToRowExec(child)
    } else {
      CometColumnarToRowExec(child)
    }
  }
}
