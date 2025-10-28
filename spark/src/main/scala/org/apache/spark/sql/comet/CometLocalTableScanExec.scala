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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.comet.execution.arrow.CometArrowConverters
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.base.Objects

import org.apache.comet.CometConf
import org.apache.comet.serde.OperatorOuterClass.Operator

case class CometLocalTableScanExec(
    override val nativeOp: Operator,
    originalPlan: LocalTableScanExec,
    @transient rows: Seq[InternalRow],
    override val output: Seq[Attribute],
    override val serializedPlanOpt: SerializedPlan)
    extends CometLeafExec {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  @transient private lazy val rdd: RDD[InternalRow] = {
    if (rows.isEmpty) {
      sparkContext.emptyRDD
    } else {
      val numSlices = math.min(rows.length, session.leafNodeDefaultParallelism)
      sparkContext.parallelize(rows, numSlices)
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val maxRecordsPerBatch = CometConf.COMET_BATCH_SIZE.get(conf)
    val timeZoneId = conf.sessionLocalTimeZone
    rdd.mapPartitionsInternal { sparkBatches =>
      val context = TaskContext.get()
      CometArrowConverters.rowToArrowBatchIter(
        sparkBatches,
        schema,
        maxRecordsPerBatch,
        timeZoneId,
        context)
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometLocalTableScanExec =>
        this.originalPlan == other.originalPlan &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(originalPlan, serializedPlanOpt)

}
