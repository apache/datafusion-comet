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
import org.apache.spark.sql.columnar.CachedBatchSerializer
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.columnar.{CachedRDDBuilder, InMemoryTableScanExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
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
    cacheBuilder: CachedRDDBuilder,
    relationOutput: Seq[Attribute],
    scanOutput: Seq[Attribute])
    extends CometExec
    with LeafExecNode {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  // `scanOutput` always equals this, including when it is empty. An empty-output scan
  // (`SELECT count(*)`) emits genuinely zero-column batches carrying only a row count: widening it
  // to a placeholder column, or to the whole cache schema, makes the emitted batches disagree with
  // the declared output, and a consumer that reads by ordinal rather than by row count -- a join,
  // for instance -- then reads the wrong column.
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

    // Resolved here rather than at planning time. CachedRDDBuilder.cachedColumnBuffers is not a
    // metadata lookup: it builds the RDD by calling execute/executeColumnar on the cached plan,
    // so touching it while Comet is still planning the outer query runs jobs during planning --
    // visibly, an EXPLAIN of a query over an adaptively-cached relation would launch a job and
    // finalize that plan.
    val cachedBuffers = cacheBuilder.cachedColumnBuffers

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

    val scanTypes = op.output.flatMap(attr => serializeDataType(attr.dataType))

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
        relation.cacheBuilder,
        relation.output,
        op.output))
  }

}
