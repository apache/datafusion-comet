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

import scala.jdk.CollectionConverters._

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, NamedArgumentExpression, NamedExpression, PythonUDF}
import org.apache.spark.sql.execution.{PartitioningPreservingUnaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampNTZType}

import com.google.protobuf.ByteString

import org.apache.comet.{CometConf, ConfigEntry, NativeBase}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, QueryPlanSerde, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator

/** Native execution for Spark 4.1+ scalar `@arrow_udf` functions. */
object CometArrowEvalPythonExec extends CometOperatorSerde[ArrowEvalPythonExec] {

  // SparkContext adds this entry even when the user has not configured a Python
  // environment. Keep other overrides on Spark's worker path.
  private def hasUnsupportedEnvironment(env: java.util.Map[String, String]): Boolean =
    env != null && env.asScala.exists { case (key, value) =>
      key != "PYTHONHASHSEED" || value != "0"
    }

  private def hasCompatibleArrowSchema(dataType: DataType): Boolean = dataType match {
    case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
        _: FloatType | _: DoubleType | _: BinaryType | _: DateType | _: DecimalType |
        _: TimestampNTZType =>
      true
    // Spark's Arrow conversion accepts plain strings. Collated and constrained strings
    // may carry semantics that are not represented by Comet's Utf8 Arrow type.
    case s: StringType if s == StringType => true
    case _ => false
  }

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED)

  override def getSupportLevel(op: ArrowEvalPythonExec): SupportLevel = {
    if (!NativeBase.supportsPythonUdf()) {
      return Unsupported(Some("Native library lacks the python-udf feature"))
    }
    if (op.evalType != PythonEvalType.SQL_SCALAR_ARROW_UDF) {
      return Unsupported(Some("Only scalar @arrow_udf is supported"))
    }
    if (op.udfs.isEmpty || op.udfs.length != op.resultAttrs.length) {
      return Unsupported(Some("Arrow UDF functions and result attributes do not match"))
    }
    if (op.conf.arrowUseLargeVarTypes) {
      return Unsupported(Some("Arrow UDF large variable types are not supported in-process"))
    }
    if (op.conf.pythonUDFProfiler.nonEmpty) {
      return Unsupported(Some("Arrow UDF profiling is not supported in-process"))
    }
    if (op.udfs.exists(_.children.exists(expr => !hasCompatibleArrowSchema(expr.dataType))) ||
      op.resultAttrs.exists(attr => !hasCompatibleArrowSchema(attr.dataType))) {
      return Unsupported(Some("Arrow UDF type is outside the verified native Arrow schema set"))
    }
    op.udfs.collectFirst {
      case udf if udf.func.broadcastVars != null && !udf.func.broadcastVars.isEmpty =>
        "Arrow UDF broadcast variables are not supported in-process"
      case udf if udf.func.pythonIncludes != null && !udf.func.pythonIncludes.isEmpty =>
        "Arrow UDF Python includes are not supported in-process"
      case udf if hasUnsupportedEnvironment(udf.func.envVars) =>
        "Arrow UDF Python environment overrides are not supported in-process"
      case udf if udf.children.exists(_.find(_.isInstanceOf[PythonUDF]).nonEmpty) =>
        "Chained Arrow UDFs are not supported in-process"
    } match {
      case Some(reason) => Unsupported(Some(reason))
      case None => Compatible(None)
    }
  }

  override def convert(
      op: ArrowEvalPythonExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] = {
    if (childOp.length != 1) {
      withFallbackReason(op, "Arrow UDF requires one native child")
      return None
    }

    val functions = op.udfs.zip(op.resultAttrs).map { case (udf, attr) =>
      val args: Seq[(Expression, String)] = udf.children.map {
        case NamedArgumentExpression(key, value) => (value, key)
        case other => (other, "")
      }
      val argProtos = args.map { case (expr, _) =>
        QueryPlanSerde.exprToProto(expr, op.child.output)
      }
      val returnType = QueryPlanSerde.serializeDataType(attr.dataType)
      if (argProtos.exists(_.isEmpty) || returnType.isEmpty) {
        None
      } else {
        Some(
          OperatorOuterClass.ArrowPythonFunction
            .newBuilder()
            .setCommand(ByteString.copyFrom(udf.func.command.toArray))
            .addAllArgs(argProtos.map(_.get).asJava)
            .addAllArgNames(args.map(_._2).asJava)
            .setReturnType(returnType.get)
            .setReturnName(attr.name)
            .setPythonVersion(udf.func.pythonVer)
            .build())
      }
    }
    if (functions.exists(_.isEmpty)) {
      withFallbackReason(op, "Arrow UDF argument or return type cannot be serialized")
      None
    } else {
      val native = OperatorOuterClass.ArrowPythonUdf
        .newBuilder()
        .addAllFunctions(functions.map(_.get).asJava)
        .setMaxRecordsPerBatch(op.conf.arrowMaxRecordsPerBatch)
      Some(builder.setArrowPythonUdf(native).build())
    }
  }

  override def createExec(nativeOp: Operator, op: ArrowEvalPythonExec): CometNativeExec =
    CometArrowEvalPythonExec(
      nativeOp,
      op,
      op.output,
      op.resultAttrs,
      op.child,
      SerializedPlan(None))
}

case class CometArrowEvalPythonExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec
    with PartitioningPreservingUnaryExecNode {

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override protected def outputExpressions: Seq[NamedExpression] = output

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
