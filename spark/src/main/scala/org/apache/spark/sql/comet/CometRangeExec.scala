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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{RangeExec, SparkPlan}

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Comet's version of Spark's `RangeExec`, which produces the rows of `spark.range` and SQL
 * `range()`. The values are generated in native code (`range_exec` in the native operators
 * crate). Each task computes its own partition from the partition index, so this operator reports
 * Spark's partitioning, and the native plan it belongs to runs one task per slice with no JVM
 * input. Partitions, values and their order match Spark's `RangeExec`. Where no native operator
 * consumes the range, `EliminateRedundantTransitions` restores Spark's `RangeExec`.
 */
case class CometRangeExec(
    override val nativeOp: Operator,
    @transient override val originalPlan: RangeExec,
    override val output: Seq[Attribute],
    override val serializedPlanOpt: SerializedPlan)
    extends CometLeafExec {

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
   * A range whose arithmetic may overflow stays on Spark. Spark's generated code for `RangeExec`
   * and its interpreted `RangeExec.doExecute`, which runs when whole-stage codegen is off or the
   * generated code is too large, can return different rows for it. So does a range with fewer
   * than one slice, which Spark fails when it runs and Comet would return empty.
   */
  override def getSupportLevel(op: RangeExec): SupportLevel = {
    if (op.isEmptyRange) {
      // Spark produces no partitions for it whatever its slice count, and so does Comet.
      Compatible()
    } else if (op.numSlices < 1) {
      Unsupported(Some(s"Spark fails a range with ${op.numSlices} slices"))
    } else if (mayOverflow(op)) {
      Unsupported(
        Some(
          "Spark can return different rows for a range whose arithmetic overflows, " +
            "depending on whether whole-stage codegen runs it"))
    } else {
      Compatible()
    }
  }

  /**
   * Whether Spark's generated code for `RangeExec` can overflow for `op`. It reads the element
   * count as a long, and walks each slice in batches of up to 1000 values, computing a batch's
   * end as its start plus the batch's span. The span wraps if it does not fit in a long. No slice
   * holds more than `ceil(numElements / numSlices)` values. Otherwise both of Spark's paths
   * produce the values `start + i * step` that Comet does.
   */
  private def mayOverflow(op: RangeExec): Boolean = {
    val maxSliceElements = (op.numElements + op.numSlices - 1) / op.numSlices
    !op.numElements.isValidLong || !(BigInt(op.step) * maxSliceElements.min(1000)).isValidLong
  }

  override def convert(
      op: RangeExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] = {
    val rangeScan = OperatorOuterClass.RangeScan
      .newBuilder()
      .setStart(op.start)
      .setStep(op.step)
      // getSupportLevel declines a count that does not fit in a long.
      .setNumElements(op.numElements.toLong)
      .setNumSlices(op.numSlices)
    Some(builder.setRangeScan(rangeScan).build())
  }

  override def createExec(nativeOp: Operator, op: RangeExec): CometNativeExec =
    CometRangeExec(nativeOp, op, op.output, SerializedPlan(None))
}
