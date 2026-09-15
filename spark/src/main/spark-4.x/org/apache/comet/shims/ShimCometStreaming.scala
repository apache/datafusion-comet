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

package org.apache.comet.shims

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.execution.{SparkPlan, StreamSourceAwareSparkPlan}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanExecBase, MicroBatchScanExec}
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.iceberg.IcebergReflection

object ShimCometStreaming {
  def nativeExecutionEnabled(conf: SQLConf, plan: SparkPlan): Boolean =
    CometConf.COMET_STREAMING_EXEC_ENABLED.get(conf) && !plan.exists {
      case source: StreamSourceAwareSparkPlan =>
        source.getStream.exists {
          case _: Source | _: MicroBatchStream => false
          case _ => true
        }
      case _ => false
    }

  def isStateBoundary(plan: SparkPlan): Boolean = {
    // These classes moved from execution.streaming to operators.stateful in Spark 4.1.
    Set("StateStoreRestoreExec", "StateStoreSaveExec", "EventTimeWatermarkExec")
      .contains(plan.getClass.getSimpleName)
  }

  def isStreamingPlan(plan: SparkPlan): Boolean = plan.exists { p =>
    // No-data batches can have a streaming LocalRelation with no source attached.
    p.logicalLink.exists(_.isStreaming) || (p match {
      case source: StreamSourceAwareSparkPlan => source.getStream.isDefined
      case _ => false
    })
  }

  def transformIcebergScans(
      plan: SparkPlan,
      transform: DataSourceV2ScanExecBase => SparkPlan): SparkPlan = plan.transformUp {
    // A vendor CDF source can expose different task semantics. Only claim the Apache Iceberg
    // append-snapshot source whose offset-bounded FileScanTasks the native reader understands.
    case scan: MicroBatchScanExec
        if IcebergReflection.isIcebergScanClass(scan.scan.getClass.getName) &&
          scan.stream.getClass.getName == "org.apache.iceberg.spark.source.SparkMicroBatchStream" =>
      transform(scan)
  }

  def icebergTasks(scan: DataSourceV2ScanExecBase): Option[java.util.List[_]] = scan match {
    case microBatch: MicroBatchScanExec =>
      // inputPartitions is a lazy val on MicroBatchScanExec. Serialization uses the same
      // materialized partitions via inputRDD; never call scan.tasks() or scan.toBatch here.
      Some(microBatch.inputPartitions.flatMap { partition =>
        IcebergReflection.tasksFromInputPartition(partition).asScala
      }.asJava)
    case _ => IcebergReflection.getTasks(scan.scan)
  }

  def icebergScanHash(scan: DataSourceV2ScanExecBase): Int = scan match {
    case microBatch: MicroBatchScanExec =>
      (microBatch.stream, microBatch.start.json(), microBatch.end.json(), scan.scan.hashCode())
        .hashCode()
    case _ => scan.scan.hashCode()
  }
}
