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

package org.apache.comet.serde

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometScanExec, CometSinkPlaceHolder, CometSparkToColumnarExec}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{CoalesceExec, CollectLimitExec, SparkPlan, TakeOrderedAndProjectExec, UnionExec}
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.window.WindowExec

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.{isCometScan, withInfo}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{serializeDataType, supportedDataType}

object CometSink extends CometOperatorSerde[SparkPlan] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = None

  override def convert(
      op: SparkPlan,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    val supportedTypes =
      op.output.forall(a => supportedDataType(a.dataType, allowComplex = true))

    if (!supportedTypes) {
      withInfo(op, "Unsupported data type")
      return None
    }

    // These operators are source of Comet native execution chain
    val scanBuilder = OperatorOuterClass.Scan.newBuilder()
    val source = op.simpleStringWithNodeId()
    if (source.isEmpty) {
      scanBuilder.setSource(op.getClass.getSimpleName)
    } else {
      scanBuilder.setSource(source)
    }

    val ffiSafe = op match {
      case _ if isExchangeSink(op) =>
        // Source of broadcast exchange batches is ArrowStreamReader
        // Source of shuffle exchange batches is NativeBatchDecoderIterator
        true
      case scan: CometScanExec if scan.scanImpl == CometConf.SCAN_NATIVE_COMET =>
        // native_comet scan reuses mutable buffers
        false
      case scan: CometScanExec if scan.scanImpl == CometConf.SCAN_NATIVE_ICEBERG_COMPAT =>
        // native_iceberg_compat scan reuses mutable buffers for constant columns
        // https://github.com/apache/datafusion-comet/issues/2152
        false
      case _ =>
        false
    }
    scanBuilder.setArrowFfiSafe(ffiSafe)

    val scanTypes = op.output.flatten { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length == op.output.length) {
      scanBuilder.addAllFields(scanTypes.asJava)

      // Sink operators don't have children
      builder.clearChildren()

      Some(builder.setScan(scanBuilder).build())
    } else {
      // There are unsupported scan type
      withInfo(
        op,
        s"unsupported Comet operator: ${op.nodeName}, due to unsupported data types above")
      None
    }

  }

  /**
   * Whether the input Spark operator `op` can be considered as a Comet sink, i.e., the start of
   * native execution. If it is true, we'll wrap `op` with `CometScanWrapper` or
   * `CometSinkPlaceHolder` later in `CometSparkSessionExtensions` after `operator2proto` is
   * called.
   */
  def isCometSink(op: SparkPlan): Boolean = {
    if (isExchangeSink(op)) {
      return true
    }
    op match {
      case s if isCometScan(s) => true
      case _: CometSparkToColumnarExec => true
      case _: CometSinkPlaceHolder => true
      case _: CoalesceExec => true
      case _: CollectLimitExec => true
      case _: UnionExec => true
      case _: TakeOrderedAndProjectExec => true
      case _: WindowExec => true
      case _ => false
    }
  }

  private def isExchangeSink(op: SparkPlan): Boolean = {
    op match {
      case _: ShuffleExchangeExec => true
      case ShuffleQueryStageExec(_, _: CometShuffleExchangeExec, _) => true
      case ShuffleQueryStageExec(_, ReusedExchangeExec(_, _: CometShuffleExchangeExec), _) => true
      case BroadcastQueryStageExec(_, _: CometBroadcastExchangeExec, _) => true
      case BroadcastQueryStageExec(_, ReusedExchangeExec(_, _: CometBroadcastExchangeExec), _) =>
        true
      case _: BroadcastExchangeExec => true
      case _ => false
    }
  }
}
