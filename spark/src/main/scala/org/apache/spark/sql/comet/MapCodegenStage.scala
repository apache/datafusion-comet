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

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

import org.apache.comet.serde.{ExprOuterClass, ProtoMessageVisitor}
import org.apache.comet.serde.OperatorOuterClass.Operator

/** The original Spark stage that determines when a map builder reads its settings. */
private[apache] final class MapCodegenStage private (stage: WholeStageCodegenExec) {
  def fits(limit: Int): Boolean = {
    // CodegenSupport.produce mutates the plan. Give each RDD construction its own copy.
    val copy = stage.session.withActive(stage.clone().asInstanceOf[WholeStageCodegenExec])
    val source = copy.doCodeGen()._2
    try {
      CodeGenerator.compile(source)._2.maxMethodCodeSize <= limit
    } catch {
      case NonFatal(_) if !Utils.isTesting && copy.conf.codegenFallback => false
    }
  }
}

private[apache] object MapCodegenStage {
  def prepare(plan: SparkPlan): SparkPlan = {
    val transitions = ApplyColumnarRulesAndInsertTransitions(Seq.empty, outputsColumnar = false)
    CollapseCodegenStages().apply(transitions.apply(plan))
  }

  def probe(stage: WholeStageCodegenExec): Option[MapCodegenStage] =
    isolate(stage.child).map { child =>
      new MapCodegenStage(stage.withNewChildren(Seq(child)).asInstanceOf[WholeStageCodegenExec])
    }

  // doCodeGen calls prepare recursively, which can start subqueries and broadcasts.
  // At an InputAdapter the stage only needs the input schema and column vector types.
  private def isolate(node: SparkPlan): Option[SparkPlan] = node match {
    case input: InputAdapter =>
      Some(InputAdapter(StageInput(input.output, input.vectorTypes, input.supportsColumnar)))
    case _ if node.subqueries.nonEmpty => None
    case _: ProjectExec | _: FilterExec | _: HashAggregateExec | _: ExpandExec | _: SortExec |
        _: LocalLimitExec | _: GlobalLimitExec | _: ColumnarToRowExec =>
      isolate(node.children.head).map(child => node.withNewChildren(Seq(child)))
    case _: RangeExec | _: LocalTableScanExec | _: RDDScanExec | _: RowDataSourceScanExec |
        _: FileSourceScanExec =>
      Some(node)
    case _ => None
  }

  private case class StageInput(
      output: Seq[Attribute],
      override val vectorTypes: Option[Seq[String]],
      override val supportsColumnar: Boolean)
      extends LeafExecNode {
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("Only used to generate stage code")
  }
}

// A generated builder captures its dedup policy on the driver. A sort or aggregate that
// falls back can insert the first key eagerly, changing which SQLConf supplies normalization.
private[apache] case class MapCodegenStagePolicy(
    @transient stage: Option[MapCodegenStage],
    @transient consumers: List[MapCodegenStage]) {
  def record(op: CometNativeExec): Unit = {
    // Spark's subsequent copyTagsFrom skips targets whose tags are already populated.
    op.copyTagsFrom(op.originalPlan)
    op.setTagValue(MapCodegenStagePolicy.Tag, this)
  }

  private def replacement(fits: MapCodegenStage => Boolean): Option[String] = {
    val generated = stage.exists(fits)
    val eager = !consumers.forall(fits)
    if (generated == stage.isDefined && !eager) None
    else {
      val name = if (generated) MapCodegenStagePolicy.Driver else MapCodegenStagePolicy.Task
      Some(if (eager) name + "_eager" else name)
    }
  }
}

private[apache] object MapCodegenStagePolicy {
  val Task = "spark_float_map"
  val Driver = "spark_float_map_driver"

  private val Tag = TreeNodeTag[MapCodegenStagePolicy]("comet.mapCodegenStagePolicy")

  private[comet] def resolve(root: CometNativeExec): Operator = {
    val limit = root.conf.hugeMethodLimit
    if (limit >= SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.defaultValue.get) return root.nativeOp

    val fits = mutable.Map.empty[MapCodegenStage, Boolean]
    def generated(stage: MapCodegenStage): Boolean =
      fits.getOrElseUpdate(stage, stage.fits(limit))
    val functions = mutable.Map.empty[Int, String]
    def collect(op: SparkPlan): Unit = op match {
      case native: CometNativeExec =>
        native.getTagValue(Tag).flatMap(_.replacement(generated)).foreach { name =>
          functions(native.nativeOp.getPlanId) = name
        }
        native.children.foreach(collect)
      case _ =>
    }
    collect(root)
    if (functions.isEmpty) return root.nativeOp

    // A native block can span several Spark codegen stages. Each proto operator keeps
    // its own plan id even if an aggregate expands into two operators with the same id.
    def rewrite(op: Operator.Builder): Unit = {
      functions.get(op.getPlanId).foreach { name =>
        val field = op.getDescriptorForType.findFieldByNumber(op.getOpStructCase.getNumber)
        ProtoMessageVisitor.visit(op.getFieldBuilder(field)) {
          case f: ExprOuterClass.ScalarFunc.Builder if f.getFunc == Driver || f.getFunc == Task =>
            val _ = f.setFunc(name)
          case _ =>
        }
      }
      op.getChildrenBuilderList.asScala.foreach(rewrite)
    }
    val builder = root.nativeOp.toBuilder
    rewrite(builder)
    builder.build()
  }
}
