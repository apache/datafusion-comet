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

import scala.collection.JavaConverters.asScalaIteratorConverter

import org.apache.spark.{SparkConf, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral, JavaCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{CodegenSupport, ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.Utils

import org.apache.comet.CometConf

/**
 * This is currently an identical copy of Spark's ColumnarToRowExec except for removing the
 * code-gen features.
 *
 * This is moved into the Comet repo as the first step towards refactoring this to make the
 * interactions with CometVector more efficient to avoid some JNI overhead.
 */
case class CometColumnarToRowExec(child: SparkPlan)
    extends CometExec
    with ColumnarToRowTransition
    with CodegenSupport {
  // supportsColumnar requires to be only called on driver side, see also SPARK-37779.
  assert(Utils.isInRunningSparkTask || child.supportsColumnar)

  val sparkConf: SparkConf = SparkEnv.get.conf

  private def isNativeSupported(schema: StructType): Boolean = {
    schema.fields.foreach(field => {
      val dt = field.dataType
      dt match {
        case _: BooleanType | _: ByteType | _: ShortType | _: IntegerType | _: LongType |
            _: FloatType | _: DoubleType =>
          true
        case _ => return false
      }
    })
    true
  }

  private def canEnableNative: Boolean = {
    CometConf.COMET_EXEC_NATIVE_COLUMNAR_TO_ROW_ENABLED.get(conf) && isNativeSupported(
      child.schema)
  }

  override def supportCodegen: Boolean = !canEnableNative

  override def supportsColumnar: Boolean = false

  override def originalPlan: SparkPlan = child

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // `ColumnarToRowExec` processes the input RDD directly, which is kind of a leaf node in the
  // codegen stage and needs to do the limit check.
  protected override def canCheckLimitNotReached: Boolean = true

  private val prefetchTime: SQLMetric =
    SQLMetrics.createNanoTimingMetric(sparkContext, "time to prefetch vectors")

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
    "prefetchTime" -> prefetchTime)

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    // This avoids calling `output` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localOutput = this.output
    val timeZoneId = conf.sessionLocalTimeZone
    val schema = child.schema
    val rowRDD = child.executeColumnar().mapPartitionsInternal { batches =>
      val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
      batches.flatMap { batch =>
        numInputBatches += 1
        numOutputRows += batch.numRows()

        if (canEnableNative && batch.numCols() > 0 && !CometUnsafeRowIterators
            .hasDictionaryOrNullVector(batch)) {
          CometUnsafeRowIterators.columnarBatchToSparkRowIter(sparkConf, batch, TaskContext.get)
        } else {
          // This is the original Spark code that creates an iterator over `ColumnarBatch`
          // to provide `Iterator[InternalRow]`. The implementation uses a `ColumnarBatchRow`
          // instance that contains an array of `ColumnVector` which will be instances of
          // `CometVector`, which in turn is a wrapper around Arrow's `ValueVector`.
          batch.rowIterator().asScala.map(toUnsafe)
        }
      }
    }
    rowRDD
  }

  /**
   * Generate [[ColumnVector]] expressions for our parent to consume as rows. This is called once
   * per [[ColumnVector]] in the batch.
   */
  private def genCodeColumnVector(
      ctx: CodegenContext,
      columnVar: String,
      ordinal: String,
      dataType: DataType,
      nullable: Boolean): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)
    val value = CodeGenerator.getValueFromVector(columnVar, dataType, ordinal)
    val isNullVar = if (nullable) {
      JavaCode.isNullVariable(ctx.freshName("isNull"))
    } else {
      FalseLiteral
    }
    val valueVar = ctx.freshName("value")
    val str = s"columnVector[$columnVar, $ordinal, ${dataType.simpleString}]"
    val code = code"${ctx.registerComment(str)}" + (if (nullable) {
                                                      code"""
        boolean $isNullVar = $columnVar.isNullAt($ordinal);
        $javaType $valueVar = $isNullVar ? ${CodeGenerator.defaultValue(dataType)} : ($value);
      """
                                                    } else {
                                                      code"$javaType $valueVar = $value;"
                                                    })
    ExprCode(code, isNullVar, JavaCode.variable(valueVar, dataType))
  }

  /**
   * Produce code to process the input iterator as [[ColumnarBatch]]es. This produces an
   * [[org.apache.spark.sql.catalyst.expressions.UnsafeRow]] for each row in each batch.
   */
  override protected def doProduce(ctx: CodegenContext): String = {
    // PhysicalRDD always just has one input
    val input = ctx.addMutableState("scala.collection.Iterator", "input", v => s"$v = inputs[0];")

    // metrics
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val numInputBatches = metricTerm(ctx, "numInputBatches")

    val columnarBatchClz = classOf[ColumnarBatch].getName
    val batch = ctx.addMutableState(columnarBatchClz, "batch")

    val idx = ctx.addMutableState(CodeGenerator.JAVA_INT, "batchIdx") // init as batchIdx = 0
    val columnVectorClzs =
      child.vectorTypes.getOrElse(Seq.fill(output.indices.size)(classOf[ColumnVector].getName))
    val (colVars, columnAssigns) = columnVectorClzs.zipWithIndex.map {
      case (columnVectorClz, i) =>
        val colVarName = s"colInstance$i"
        val name = ctx.addMutableState(columnVectorClz, colVarName)
        (name, s"$name = ($columnVectorClz) $batch.column($i);")
    }.unzip

    val nextBatch = ctx.freshName("nextBatch")
    val nextBatchFuncName = ctx.addNewFunction(
      nextBatch,
      s"""
         |private void $nextBatch() throws java.io.IOException {
         |  if ($input.hasNext()) {
         |    $batch = ($columnarBatchClz)$input.next();
         |    $numInputBatches.add(1);
         |    $numOutputRows.add($batch.numRows());
         |    $idx = 0;
         |    ${columnAssigns.mkString("", "\n", "\n")}
         |  }
         |}""".stripMargin)

    ctx.currentVars = null
    val rowidx = ctx.freshName("rowIdx")
    val columnsBatchInput = (output zip colVars).map { case (attr, colVar) =>
      genCodeColumnVector(ctx, colVar, rowidx, attr.dataType, attr.nullable)
    }
    val localIdx = ctx.freshName("localIdx")
    val localEnd = ctx.freshName("localEnd")
    val numRows = ctx.freshName("numRows")
    val shouldStop = if (parent.needStopCheck) {
      s"if (shouldStop()) { $idx = $rowidx + 1; return; }"
    } else {
      "// shouldStop check is eliminated"
    }
    val out =
      s"""
       |if ($batch == null) {
       |  $nextBatchFuncName();
       |}
       |while ($limitNotReachedCond $batch != null) {
       |  int $numRows = $batch.numRows();
       |  int $localEnd = $numRows - $idx;
       |  for (int $localIdx = 0; $localIdx < $localEnd; $localIdx++) {
       |    int $rowidx = $idx + $localIdx;
       |    ${consume(ctx, columnsBatchInput).trim}
       |    $shouldStop
       |  }
       |  $idx = $numRows;
       |  $batch = null;
       |  $nextBatchFuncName();
       |}
     """.stripMargin
    out
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    Seq(child.executeColumnar().asInstanceOf[RDD[InternalRow]]) // Hack because of type erasure
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CometColumnarToRowExec =
    copy(child = newChild)
}
