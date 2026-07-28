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
import org.apache.spark.sql.execution.{SampleExec, SparkPlan}

import com.google.common.base.Objects

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator

object CometSampleExec extends CometOperatorSerde[SampleExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_SAMPLE_ENABLED)

  override def getSupportLevel(operator: SampleExec): SupportLevel = {
    if (operator.withReplacement) {
      // Sampling with replacement draws from a Poisson distribution seeded by
      // commons-math3 Well19937c, which the native side does not implement yet.
      // https://github.com/apache/datafusion-comet/issues/5109
      Unsupported(Some("Sampling with replacement is not supported"))
    } else {
      super.getSupportLevel(operator)
    }
  }

  override def convert(
      op: SampleExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    if (childOp.nonEmpty) {
      val sampleBuilder = OperatorOuterClass.Sample
        .newBuilder()
        .setLowerBound(op.lowerBound)
        .setUpperBound(op.upperBound)
        // The partition index is added to the seed on the native side.
        .setSeed(op.seed)
      Some(builder.setSample(sampleBuilder).build())
    } else {
      withFallbackReason(op, "No child operator")
      None
    }
  }

  override def createExec(nativeOp: Operator, op: SampleExec): CometNativeExec = {
    CometSampleExec(
      nativeOp,
      op,
      op.lowerBound,
      op.upperBound,
      op.seed,
      op.child,
      SerializedPlan(None))
  }
}

/**
 * Comet physical plan node for Spark `SampleExec`, for the case where sampling is performed
 * without replacement. The native operator reproduces Spark's per-row draw sequence, so it
 * selects the same rows as Spark for a given seed as long as the input rows arrive in the same
 * order.
 */
case class CometSampleExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    lowerBound: Double,
    upperBound: Double,
    seed: Long,
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(lowerBound, upperBound, seed, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometSampleExec =>
        this.output == other.output &&
        this.lowerBound == other.lowerBound &&
        this.upperBound == other.upperBound &&
        this.seed == other.seed &&
        this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(
      output,
      lowerBound: java.lang.Double,
      upperBound: java.lang.Double,
      seed: java.lang.Long,
      child)
}
