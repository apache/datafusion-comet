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

package org.apache.comet.serde.operator

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.comet.{CometNativeExec, CometSinkPlaceHolder}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.ConfigEntry
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{serializeDataType, supportedDataType}

/**
 * CometSink is the base class for transformations from a Spark operator to a Comet operator where
 * the native plan is a ScanExec that will read data from the Comet operator running the JVM.
 */
abstract class CometSink[T <: SparkPlan] extends CometOperatorSerde[T] {

  /** Whether the data produced by the Comet operator is FFI safe */
  def isFfiSafe: Boolean = true

  override def enabledConfig: Option[ConfigEntry[Boolean]] = None

  override def convert(
      op: T,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    // [#4515 instrumentation]
    val log = org.slf4j.LoggerFactory.getLogger("[#4515]")
    log.warn(
      s"CometSink[${this.getClass.getSimpleName}].convert op=${op.getClass.getName} " +
        s"simpleString='${op.simpleStringWithNodeId()}' output=${op.output} " +
        s"output.size=${op.output.size}")
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
    scanBuilder.setArrowFfiSafe(isFfiSafe)

    val scanTypes = op.output.flatten { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length == op.output.length) {
      // [#4515 instrumentation] Log when we synthesize a Scan with zero declared columns.
      // The runtime JVM iterator may still produce columns (subquery output shrunk by
      // catalyst before serialization while the underlying RDD reflects the pre-shrink shape),
      // tripping the column-count guard in NativeUtil.exportBatch.
      if (scanTypes.isEmpty) {
        val log = org.slf4j.LoggerFactory.getLogger("[#4515]")
        // scalastyle:off line.size.limit
        val childInfo = op.children.zipWithIndex
          .map { case (c, i) =>
            val canonOut = scala.util
              .Try(c.canonicalized.output)
              .toOption
              .map(_.toString)
              .getOrElse("<canonicalize failed>")
            s"  child[$i] cls=${c.getClass.getName} simpleString='${c.simpleString(
                80)}' output=${c.output} outputSize=${c.output.size} identityHash=${System
                .identityHashCode(c)} canonicalized.output=$canonOut"
          }
          .mkString("\n")
        val opCanonOut = scala.util
          .Try(op.canonicalized.output)
          .toOption
          .map(_.toString)
          .getOrElse("<canonicalize failed>")
        val subqueryInfo = scala.util
          .Try(op.subqueries.map(s => s"${s.getClass.getName}(output=${s.output}, prepared=?)"))
          .toOption
          .getOrElse(Nil)
          .mkString("[", ", ", "]")
        val callerStack =
          new RuntimeException("[#4515] CometSink 0-col Scan caller").getStackTrace
            .take(20)
            .map(f => s"    at ${f}")
            .mkString("\n")
        log.warn(s"CometSink synthesizing 0-col Scan for op=${op.getClass.getName}\n" +
          s"  simpleString='${op.simpleStringWithNodeId()}'\n" +
          s"  op.output=${op.output} op.outputSet=${op.outputSet} op.references=${op.references}\n" +
          s"  op.canonicalized.output=$opCanonOut\n" +
          s"  op.subqueries=$subqueryInfo\n" +
          s"  op identityHash=${System.identityHashCode(op)}\n" +
          s"  children classes=${op.children.map(_.getClass.getSimpleName).mkString("[", ",", "]")}\n" +
          childInfo + "\n" +
          s"  caller stack:\n$callerStack\n" +
          s"  op tree:\n${op.treeString(verbose = true, addSuffix = false)}")
        // scalastyle:on line.size.limit
      }

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
}

object CometExchangeSink extends CometSink[SparkPlan] {

  override def convert(
      op: SparkPlan,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    // [#4515 instrumentation]
    val log = org.slf4j.LoggerFactory.getLogger("[#4515]")
    val isVanillaSparkExchange =
      op.getClass.getName == "org.apache.spark.sql.execution.exchange.ShuffleExchangeExec"
    log.warn(
      s"CometExchangeSink.convert op=${op.getClass.getName} " +
        s"simpleString='${op.simpleStringWithNodeId()}' output=${op.output} " +
        s"useShuffleScan=${shouldUseShuffleScan(op)} " +
        s"children=${op.children.map(_.getClass.getSimpleName).mkString("[", ",", "]")}")
    if (isVanillaSparkExchange) {
      val callerStack =
        new RuntimeException("[#4515] vanilla ShuffleExchangeExec caller").getStackTrace
          .take(20)
          .map(f => s"    at ${f}")
          .mkString("\n")
      log.warn(
        "  vanilla ShuffleExchangeExec being processed by CometExchangeSink:\n" +
          s"  output=${op.output}\n" +
          s"  caller stack:\n$callerStack\n" +
          s"  op tree:\n${op.treeString(verbose = true, addSuffix = false)}")
    }
    if (shouldUseShuffleScan(op)) {
      convertToShuffleScan(op, builder)
    } else {
      super.convert(op, builder, childOp: _*)
    }
  }

  private def shouldUseShuffleScan(op: SparkPlan): Boolean = {
    if (!CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.get()) return false

    // Extract the CometShuffleExchangeExec from the wrapper
    val shuffleExec = op match {
      case ShuffleQueryStageExec(_, s: CometShuffleExchangeExec, _) => Some(s)
      case ShuffleQueryStageExec(_, ReusedExchangeExec(_, s: CometShuffleExchangeExec), _) =>
        Some(s)
      case s: CometShuffleExchangeExec => Some(s)
      case _ => None
    }

    shuffleExec.isDefined
  }

  private def convertToShuffleScan(
      op: SparkPlan,
      builder: Operator.Builder): Option[OperatorOuterClass.Operator] = {
    val supportedTypes =
      op.output.forall(a => supportedDataType(a.dataType, allowComplex = true))

    if (!supportedTypes) {
      withInfo(op, "Unsupported data type for shuffle direct read")
      return None
    }

    val scanBuilder = OperatorOuterClass.ShuffleScan.newBuilder()
    val source = op.simpleStringWithNodeId()
    if (source.isEmpty) {
      scanBuilder.setSource(op.getClass.getSimpleName)
    } else {
      scanBuilder.setSource(source)
    }

    val scanTypes = op.output.flatMap { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length == op.output.length) {
      scanBuilder.addAllFields(scanTypes.asJava)
      builder.clearChildren()
      Some(builder.setShuffleScan(scanBuilder).build())
    } else {
      withInfo(op, s"unsupported data types in ${op.nodeName} for shuffle direct read")
      None
    }
  }

  override def createExec(nativeOp: Operator, op: SparkPlan): CometNativeExec =
    CometSinkPlaceHolder(nativeOp, op, op)
}
