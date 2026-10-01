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

package org.apache.comet.local

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.comet.CometScanExec
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.types._

import com.google.protobuf.Message

import org.apache.comet.CometConf
import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.rules.CometScanRule
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.OperatorOuterClass.{Filter, Operator, Projection}
import org.apache.comet.serde.QueryPlanSerde.exprToProto
import org.apache.comet.serde.operator.{partition2Proto, CometNativeScan}

/** Whole-query admission; reuse Comet serde, never task iterators or JVM expression callbacks. */
private[local] object LocalParquetPlanner {
  private def supportedType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
        StringType | BinaryType | DateType | TimestampType | TimestampNTZType | NullType =>
      true
    case _: DecimalType => true
    case _ => false
  }

  private[local] def supportedExpression(e: Expression): Boolean =
    supportedType(e.dataType) && e.deterministic && (e match {
      case _: AttributeReference | _: Literal | _: Alias | _: Add | _: Subtract | _: Multiply |
          _: Divide | _: Remainder | _: Cast | _: EqualTo | _: EqualNullSafe | _: GreaterThan |
          _: GreaterThanOrEqual | _: LessThan | _: LessThanOrEqual | _: IsNull | _: IsNotNull |
          _: And | _: Or | _: Not | _: UnaryMinus | _: CheckOverflow | _: If | _: CaseWhen =>
        e.children.forall(supportedExpression)
      case _ => false
    })

  // Serde can choose JVM codegen even for a supported Spark expression. Inspect the resulting
  // protobuf recursively as well, so no callback/task-bound expression enters this graph.
  private val nativeExpressions = Set(
    "LITERAL",
    "BOUND",
    "ADD",
    "SUBTRACT",
    "MULTIPLY",
    "DIVIDE",
    "REMAINDER",
    "CAST",
    "EQ",
    "NEQ",
    "GT",
    "GT_EQ",
    "LT",
    "LT_EQ",
    "IS_NULL",
    "IS_NOT_NULL",
    "AND",
    "OR",
    "NOT",
    "UNARY_MINUS",
    "CHECK_OVERFLOW",
    "EQNULLSAFE",
    "NEQNULLSAFE",
    "IF",
    "CASEWHEN")

  private[local] def nativeOnly(message: Message): Boolean = {
    val accepted = message match {
      case expression: Expr => nativeExpressions.contains(expression.getExprStructCase.name())
      case _ => true
    }
    accepted && message.getAllFields.asScala.forall { case (field, value) =>
      if (field.getJavaType != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) {
        true
      } else if (field.isRepeated) {
        value.asInstanceOf[java.util.List[Message]].asScala.forall(nativeOnly)
      } else nativeOnly(value.asInstanceOf[Message])
    }
  }

  private def expression(e: Expression, child: SparkPlan): Option[Expr] =
    if (supportedExpression(e)) exprToProto(e, child.output).filter(nativeOnly) else None

  def plan(
      root: SparkPlan,
      session: SparkSession,
      allowEmptyOutput: Boolean = false): Option[LocalParquetSpec] = {
    val batchSize = CometConf.COMET_BATCH_SIZE.get(root.conf)
    if (batchSize < 1 || batchSize > 65536 ||
      (root.output.isEmpty && !allowEmptyOutput) || root.output.size > 1024) {
      return None
    }
    var scan: Option[CometScanExec] = None
    def convert(node: SparkPlan): Option[Operator] = node match {
      case project: ProjectExec
          if project.projectList.nonEmpty &&
            CometConf.COMET_EXEC_PROJECT_ENABLED.get(project.conf) =>
        val expressions = project.projectList.map(expression(_, project.child))
        if (!expressions.forall(_.isDefined)) { None }
        else {
          convert(project.child).map { child =>
            Operator
              .newBuilder()
              .setPlanId(node.id)
              .addChildren(child)
              .setProjection(
                Projection.newBuilder().addAllProjectList(expressions.flatten.asJava))
              .build()
          }
        }
      case filter: FilterExec if CometConf.COMET_EXEC_FILTER_ENABLED.get(filter.conf) =>
        for {
          predicate <- expression(filter.condition, filter.child)
          child <- convert(filter.child)
        } yield Operator
          .newBuilder()
          .setPlanId(node.id)
          .addChildren(child)
          .setFilter(Filter.newBuilder().setPredicate(predicate))
          .build()
      case file: FileSourceScanExec =>
        val relation = file.relation
        val hadoop = session.sessionState.newHadoopConfWithOptions(relation.options)
        // Reject ordering/partition-sensitive and callback-based file features before reusing
        // the ordinary native scan checks. Copies keep fallback tags off the original query.
        if (!CometScanExec.isFileFormatSupported(relation.fileFormat) ||
          relation.bucketSpec.nonEmpty || file.outputOrdering.nonEmpty ||
          !relation.schema.fields.forall(f => supportedType(f.dataType)) ||
          file.output.exists(_.metadata.contains("__metadata_col")) ||
          file.fileConstantMetadataColumns.nonEmpty ||
          file.partitionFilters.exists(!supportedExpression(_)) ||
          file.dataFilters.exists(!supportedExpression(_)) ||
          relation.location.rootPaths.exists(p => p.toUri.getScheme != "file") ||
          CometParquetUtils.encryptionEnabled(hadoop) ||
          Option(hadoop.get("fs.file.impl"))
            .exists(_ != "org.apache.hadoop.fs.LocalFileSystem")) {
          None
        } else {
          CometScanRule(session).apply(file.copy()) match {
            case candidate: CometScanExec =>
              CometNativeScan
                .convert(candidate, Operator.newBuilder().setPlanId(node.id))
                .filter(op =>
                  nativeOnly(op) &&
                    op.getNativeScan.getCommon.getObjectStoreOptionsCount == 0)
                .map { op => scan = Some(candidate); op }
            case _ => None
          }
        }
      case _ => None
    }
    convert(root).flatMap { op =>
      scan.flatMap { source =>
        val partitions = source.getFilePartitions()
        if (partitions.size > 1024 || partitions.exists(
            _.files.exists(file => file.filePath.toUri.getScheme != "file"))) { None }
        else {
          Some(
            LocalParquetSpec(
              op.toByteArray,
              partitions
                .map(p => partition2Proto(p, source.relation.partitionSchema).toByteArray)
                .toArray,
              batchSize,
              root.output.size,
              CometConf.COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED.get(root.conf),
              Array.emptyByteArray,
              CometConf.COMET_EXEC_LOCAL_MEMORY_LIMIT.get(root.conf),
              CometConf.COMET_EXEC_LOCAL_SPILL_ENABLED.get(root.conf)))
        }
      }
    }
  }
}
