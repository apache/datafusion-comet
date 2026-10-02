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

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{RangeExec, SparkPlan}

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Comet's version of Spark's `RangeExec`, which produces the rows of `spark.range` and SQL
 * `range()`. The values are generated in native code, by the native `RangeExec`. Each task
 * computes its own partition from the partition index, so this operator reports Spark's
 * partitioning, and the native plan it belongs to runs one task per slice with no JVM input.
 * Partitions, values and their order match Spark's generated code for `RangeExec`.
 */
case class CometRangeExec(
    override val nativeOp: Operator,
    @transient override val originalPlan: RangeExec,
    override val output: Seq[Attribute],
    override val serializedPlanOpt: SerializedPlan)
    extends CometLeafExec {

  override def outputPartitioning: Partitioning = originalPlan.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = originalPlan.outputOrdering

  override def simpleString(maxFields: Int): String = {
    s"$nodeName (${originalPlan.start}, ${originalPlan.end}, step=${originalPlan.step}, " +
      s"splits=${originalPlan.numSlices})"
  }

  override protected def doCanonicalize(): SparkPlan = {
    val canonical = originalPlan.canonicalized.asInstanceOf[RangeExec]
    CometRangeExec(nativeOp, canonical, canonical.output, SerializedPlan(None))
  }

  // `originalPlan` carries every parameter that decides the rows: start, end, step and the
  // number of slices.
  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometRangeExec =>
        this.originalPlan == other.originalPlan &&
        this.output == other.output &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(originalPlan, output)
}

object CometRangeExec extends CometOperatorSerde[RangeExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_RANGE_ENABLED)

  /**
   * Comet follows Spark's generated code for `RangeExec`. With whole-stage codegen disabled,
   * Spark runs the interpreted `RangeExec.doExecute` instead, and the two can return different
   * rows when the generated code's arithmetic overflows, so those ranges stay on Spark. So does a
   * range with fewer than one slice, which Spark fails when it runs and Comet would return empty.
   */
  override def getSupportLevel(op: RangeExec): SupportLevel = {
    if (!op.isEmptyRange && op.numSlices < 1) {
      Unsupported(Some(s"Spark fails a range with ${op.numSlices} slices"))
    } else if (!op.conf.wholeStageEnabled && mayOverflow(op)) {
      Unsupported(
        Some(
          "Spark's interpreted RangeExec, which runs when whole-stage codegen is disabled, " +
            "can return different rows for this range"))
    } else {
      Compatible()
    }
  }

  /**
   * Whether Spark's generated code for `RangeExec` can overflow for `op`, which is when it can
   * disagree with the interpreted `RangeExec`. It reads the element count as a long, which
   * truncates a count that does not fit, and walks each partition in batches of up to 1000 values
   * whose end wraps if the batch spans 2^63 or more.
   */
  private def mayOverflow(op: RangeExec): Boolean = {
    op.numElements > Long.MaxValue ||
    BigInt(op.step).abs * op.numElements.min(1000) >= (BigInt(1) << 63)
  }

  override def convert(
      op: RangeExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] = {
    val rangeScan = OperatorOuterClass.RangeScan
      .newBuilder()
      .setStart(op.start)
      .setStep(op.step)
      // Spark's generated code reads the element count as a long, so it is truncated the same way.
      .setNumElements(op.numElements.toLong)
      .setNumSlices(op.numSlices)
    Some(builder.setRangeScan(rangeScan).build())
  }

  override def createExec(nativeOp: Operator, op: RangeExec): CometNativeExec =
    CometRangeExec(nativeOp, op, op.output, SerializedPlan(None))
}
