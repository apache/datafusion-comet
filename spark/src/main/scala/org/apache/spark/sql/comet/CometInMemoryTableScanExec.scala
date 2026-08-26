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

package org.apache.spark.sql.comet

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometConf
import org.apache.comet.serde.CometOperatorSerde
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

/**
 * Reads Spark cached table data when the cache was written by Comet's cache serializer.
 *
 * Spark stores cached data through `CachedBatchSerializer`. This node keeps the scan inside Comet
 * by asking the serializer to decode cached batches directly into `ColumnarBatch` output,
 * avoiding the extra Spark columnar-to-Comet columnar conversion used by the default path.
 *
 * `relationOutput` is the full schema stored in the cache. `scanOutput` is the subset requested
 * by this scan after pruning.
 */
case class CometInMemoryTableScanExec(
    originalPlan: InMemoryTableScanExec,
    serializer: CachedBatchSerializer,
    cachedBuffers: RDD[CachedBatch],
    relationOutput: Seq[Attribute],
    scanOutput: Seq[Attribute])
    extends CometExec
    with LeafExecNode {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  // For an empty-projection scan (`SELECT count(*)`) this is empty while `scanOutput` holds one
  // placeholder column, so the emitted batches are wider than the declared output. That is safe
  // because the only consumer of an empty-output scan is a count-style aggregate, which reads the
  // row count rather than any column; see `scanOutputFor` for why the scan cannot simply be empty.
  override def output: Seq[Attribute] = originalPlan.output

  // Use the serializer's vector types because the cached batch layout is owned by the serializer.
  override def vectorTypes: Option[Seq[String]] =
    serializer.vectorTypes(scanOutput, conf)

  // Apply Spark's cache batch filter before decoding. Spark's InMemoryTableScanExec does this in
  // filteredCachedBatches(), but that method is private. Reusing the serializer's buildFilter here
  // keeps Comet on the same stats-based pruning path instead of decoding every cached batch.
  //
  // Gated on conf.inMemoryPartitionPruning the same way Spark's filteredCachedBatches is, so
  // spark.sql.inMemoryColumnarStorage.partitionPruning=false disables pruning here too. Pruning is
  // normally a win, but the config exists to be able to turn it off -- for debugging a suspected
  // stats bug, for instance -- and silently ignoring it would make Comet diverge from Spark on a
  // knob a user reaching for it is specifically trying to control.
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")

    val filteredBuffers =
      if (originalPlan.predicates.nonEmpty && conf.inMemoryPartitionPruning) {
        val filter = serializer.buildFilter(originalPlan.predicates, relationOutput)
        cachedBuffers.mapPartitionsWithIndex(filter)
      } else {
        cachedBuffers
      }

    serializer
      .convertCachedBatchToColumnarBatch(filteredBuffers, relationOutput, scanOutput, conf)
      .map { cb =>
        numOutputRows += cb.numRows()
        cb
      }
  }
}

object CometInMemoryTableScanExec extends CometOperatorSerde[InMemoryTableScanExec] {

  override def enabledConfig: Option[org.apache.comet.ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED)

  override def convert(
      op: InMemoryTableScanExec,
      builder: OperatorOuterClass.Operator.Builder,
      childOp: Operator*): Option[Operator] = {

    val scanTypes = scanOutputFor(op).flatMap(attr => serializeDataType(attr.dataType))

    val scanBuilder = OperatorOuterClass.Scan
      .newBuilder()
      .setSource(op.getClass.getSimpleName)
      .addAllFields(scanTypes.asJava)

    Some(builder.setScan(scanBuilder).build())
  }

  // Reuse Spark's InMemoryRelation metadata so cache materialization, pruning, and storage
  // behavior remain controlled by Spark's cache manager.
  override def createExec(nativeOp: Operator, op: InMemoryTableScanExec): CometNativeExec = {
    val relation = op.relation

    CometScanWrapper(
      nativeOp,
      CometInMemoryTableScanExec(
        op,
        relation.cacheBuilder.serializer,
        relation.cacheBuilder.cachedColumnBuffers,
        relation.output,
        scanOutputFor(op)))
  }

  /**
   * Columns the cache scan asks the serializer to decode.
   *
   * An empty-output scan (`SELECT count(*)`) still needs a non-empty schema for native planning,
   * and the batches the node emits have to match that schema. Falling back to the whole cache
   * schema satisfies both, but the serializer decodes exactly what it is asked for, so the
   * cheapest query in the workload would decode every cached column. One column is enough: the
   * aggregate above an empty-output scan reads the row count and never a value, so pick the
   * cheapest to decode rather than all of them.
   *
   * `convert` and `createExec` must choose identically, or the native scan's declared schema and
   * the batches fed to it disagree.
   */
  private def scanOutputFor(op: InMemoryTableScanExec): Seq[Attribute] = {
    if (op.output.nonEmpty) {
      op.output
    } else if (op.relation.output.isEmpty) {
      Nil
    } else {
      Seq(op.relation.output.minBy(a => decodeCostRank(a.dataType)))
    }
  }

  // Rank by how much work decoding a column of this type costs, cheapest first. Fixed-width types
  // decode to a flat buffer; variable-width and nested ones carry offsets, children and possibly
  // dictionaries.
  private def decodeCostRank(dt: DataType): Int = dt match {
    case BooleanType | ByteType => 0
    case ShortType => 1
    case IntegerType | FloatType | DateType => 2
    case LongType | DoubleType | TimestampType | TimestampNTZType => 3
    case _: DecimalType => 4
    case _ => 5
  }
}
