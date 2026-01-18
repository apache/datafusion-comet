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

import java.util.UUID
import java.util.concurrent.{Future, TimeoutException, TimeUnit}

import scala.concurrent.Promise
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.{broadcast, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.util.{Utils => CometUtils}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{CodegenSupport, ColumnarToRowTransition, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.vectorized.{ConstantColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.{SparkFatalException, Utils}
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.vector.{CometListVector, CometMapVector, CometPlainVector}

/**
 * Copied from Spark `ColumnarToRowExec`. Comet needs the fix for SPARK-50235 but cannot wait for
 * the fix to be released in Spark versions. We copy the implementation here to apply the fix.
 */
case class CometColumnarToRowExec(child: SparkPlan)
    extends ColumnarToRowTransition
    with CometPlan
    with CodegenSupport {
  // supportsColumnar requires to be only called on driver side, see also SPARK-37779.
  assert(Utils.isInRunningSparkTask || child.supportsColumnar)

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // `ColumnarToRowExec` processes the input RDD directly, which is kind of a leaf node in the
  // codegen stage and needs to do the limit check.
  protected override def canCheckLimitNotReached: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"))

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    // This avoids calling `output` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localOutput = this.output
    child.executeColumnar().mapPartitionsInternal { batches =>
      val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
      batches.flatMap { batch =>
        numInputBatches += 1
        numOutputRows += batch.numRows()
        batch.rowIterator().asScala.map(toUnsafe)
      }
    }
  }

  @transient
  private lazy val promise = Promise[broadcast.Broadcast[Any]]()

  @transient
  private val timeout: Long = conf.broadcastTimeout

  private val runId: UUID = UUID.randomUUID

  private lazy val cometBroadcastExchange = findCometBroadcastExchange(child)

  @transient
  lazy val relationFuture: Future[broadcast.Broadcast[Any]] = {
    SQLExecution.withThreadLocalCaptured[broadcast.Broadcast[Any]](
      session,
      CometBroadcastExchangeExec.executionContext) {
      try {
        // Setup a job group here so later it may get cancelled by groupId if necessary.
        sparkContext.setJobGroup(
          runId.toString,
          s"CometColumnarToRow broadcast exchange (runId $runId)",
          interruptOnCancel = true)

        val numOutputRows = longMetric("numOutputRows")
        val numInputBatches = longMetric("numInputBatches")
        val localOutput = this.output
        val broadcastColumnar = child.executeBroadcast()
        val serializedBatches = broadcastColumnar.value.asInstanceOf[Array[ChunkedByteBuffer]]
        val toUnsafe = UnsafeProjection.create(localOutput, localOutput)
        val rows = serializedBatches.iterator
          .flatMap(CometUtils.decodeBatches(_, this.getClass.getSimpleName))
          .flatMap { batch =>
            numInputBatches += 1
            numOutputRows += batch.numRows()
            batch.rowIterator().asScala.map(toUnsafe)
          }

        val mode = cometBroadcastExchange.get.mode
        val relation = mode.transform(rows, Some(numOutputRows.value))
        val broadcasted = sparkContext.broadcastInternal(relation, serializedOnly = true)
        val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        promise.trySuccess(broadcasted)
        broadcasted
      } catch {
        // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
        // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
        // will catch this exception and re-throw the wrapped fatal throwable.
        case oe: OutOfMemoryError =>
          val ex = new SparkFatalException(oe)
          promise.tryFailure(ex)
          throw ex
        case e if !NonFatal(e) =>
          val ex = new SparkFatalException(e)
          promise.tryFailure(ex)
          throw ex
        case e: Throwable =>
          promise.tryFailure(e)
          throw e
      }
    }
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    if (cometBroadcastExchange.isEmpty) {
      throw new SparkException(
        "ColumnarToRowExec only supports doExecuteBroadcast when child contains a " +
          "CometBroadcastExchange, but got " + child)
    }

    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(runId.toString)
          relationFuture.cancel(true)
        }
        throw QueryExecutionErrors.executeBroadcastTimeoutError(timeout, Some(ex))
    }
  }

  private def findCometBroadcastExchange(op: SparkPlan): Option[CometBroadcastExchangeExec] = {
    op match {
      case b: CometBroadcastExchangeExec => Some(b)
      case b: BroadcastQueryStageExec => findCometBroadcastExchange(b.plan)
      case b: ReusedExchangeExec => findCometBroadcastExchange(b.child)
      case _ => op.children.collectFirst(Function.unlift(findCometBroadcastExchange))
    }
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
   * Generate optimized code for ArrayType columns using Comet's direct memory access. This caches
   * the offset buffer address and data vector per-batch to avoid repeated method calls per-row.
   */
  private def genCodeForCometArray(
      ctx: CodegenContext,
      columnVar: String,
      ordinal: String,
      offsetAddrVar: String,
      dataColVar: String,
      dataType: DataType,
      nullable: Boolean): ExprCode = {
    val columnarArrayClz = "org.apache.spark.sql.vectorized.ColumnarArray"
    val platformClz = "org.apache.spark.unsafe.Platform"

    val isNullVar = if (nullable) {
      JavaCode.isNullVariable(ctx.freshName("isNull"))
    } else {
      FalseLiteral
    }
    val valueVar = ctx.freshName("value")
    val startVar = ctx.freshName("start")
    val endVar = ctx.freshName("end")
    val lenVar = ctx.freshName("len")

    val str = s"cometArrayVector[$columnVar, $ordinal]"
    // scalastyle:off line.size.limit
    val code = code"${ctx.registerComment(str)}" + (if (nullable) {
                                                      code"""
        boolean $isNullVar = $columnVar.isNullAt($ordinal);
        $columnarArrayClz $valueVar = null;
        if (!$isNullVar) {
          int $startVar = $platformClz.getInt(null, $offsetAddrVar + (long) $ordinal * 4L);
          int $endVar = $platformClz.getInt(null, $offsetAddrVar + (long) ($ordinal + 1) * 4L);
          int $lenVar = $endVar - $startVar;
          $valueVar = new $columnarArrayClz($dataColVar, $startVar, $lenVar);
        }
      """
                                                    } else {
                                                      code"""
        int $startVar = $platformClz.getInt(null, $offsetAddrVar + (long) $ordinal * 4L);
        int $endVar = $platformClz.getInt(null, $offsetAddrVar + (long) ($ordinal + 1) * 4L);
        int $lenVar = $endVar - $startVar;
        $columnarArrayClz $valueVar = new $columnarArrayClz($dataColVar, $startVar, $lenVar);
      """
                                                    })
    // scalastyle:on line.size.limit
    ExprCode(code, isNullVar, JavaCode.variable(valueVar, dataType))
  }

  /**
   * Generate optimized code for MapType columns using Comet's direct memory access. This caches
   * the offset buffer address, keys vector, and values vector per-batch.
   */
  private def genCodeForCometMap(
      ctx: CodegenContext,
      columnVar: String,
      ordinal: String,
      offsetAddrVar: String,
      keysColVar: String,
      valuesColVar: String,
      dataType: DataType,
      nullable: Boolean): ExprCode = {
    val columnarMapClz = "org.apache.spark.sql.vectorized.ColumnarMap"
    val platformClz = "org.apache.spark.unsafe.Platform"

    val isNullVar = if (nullable) {
      JavaCode.isNullVariable(ctx.freshName("isNull"))
    } else {
      FalseLiteral
    }
    val valueVar = ctx.freshName("value")
    val startVar = ctx.freshName("start")
    val endVar = ctx.freshName("end")
    val lenVar = ctx.freshName("len")

    val str = s"cometMapVector[$columnVar, $ordinal]"
    // scalastyle:off line.size.limit
    val code = code"${ctx.registerComment(str)}" + (if (nullable) {
                                                      code"""
        boolean $isNullVar = $columnVar.isNullAt($ordinal);
        $columnarMapClz $valueVar = null;
        if (!$isNullVar) {
          int $startVar = $platformClz.getInt(null, $offsetAddrVar + (long) $ordinal * 4L);
          int $endVar = $platformClz.getInt(null, $offsetAddrVar + (long) ($ordinal + 1) * 4L);
          int $lenVar = $endVar - $startVar;
          $valueVar = new $columnarMapClz($keysColVar, $valuesColVar, $startVar, $lenVar);
        }
      """
                                                    } else {
                                                      code"""
        int $startVar = $platformClz.getInt(null, $offsetAddrVar + (long) $ordinal * 4L);
        int $endVar = $platformClz.getInt(null, $offsetAddrVar + (long) ($ordinal + 1) * 4L);
        int $lenVar = $endVar - $startVar;
        $columnarMapClz $valueVar = new $columnarMapClz($keysColVar, $valuesColVar, $startVar, $lenVar);
      """
                                                    })
    // scalastyle:on line.size.limit
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
    val columnVectorClz = classOf[ColumnVector].getName
    val cometListVectorClz = classOf[CometListVector].getName
    val cometMapVectorClz = classOf[CometMapVector].getName

    // For each column, create mutable state and assignment code.
    // For ArrayType and MapType, also create cached state for offset addresses and child vectors.
    case class ColumnInfo(
        colVar: String,
        assignCode: String,
        dataType: DataType,
        nullable: Boolean,
        // For ArrayType: (offsetAddrVar, dataColVar)
        arrayInfo: Option[(String, String)] = None,
        // For MapType: (offsetAddrVar, keysColVar, valuesColVar)
        mapInfo: Option[(String, String, String)] = None)

    val columnInfos = output.zipWithIndex.map { case (attr, i) =>
      val colVarName = ctx.addMutableState(columnVectorClzs(i), s"colInstance$i")
      val baseAssign = s"$colVarName = (${columnVectorClzs(i)}) $batch.column($i);"

      attr.dataType match {
        case _: ArrayType =>
          val offsetAddrVar = ctx.addMutableState("long", s"arrayOffsetAddr$i")
          val dataColVar = ctx.addMutableState(columnVectorClz, s"arrayDataCol$i")
          val extraAssign =
            s"""
               |if ($colVarName instanceof $cometListVectorClz) {
               |  $cometListVectorClz cometList$i = ($cometListVectorClz) $colVarName;
               |  $offsetAddrVar = cometList$i.getOffsetBufferAddress();
               |  $dataColVar = cometList$i.getDataColumnVector();
               |}
             """.stripMargin
          ColumnInfo(
            colVarName,
            baseAssign + extraAssign,
            attr.dataType,
            attr.nullable,
            arrayInfo = Some((offsetAddrVar, dataColVar)))

        case _: MapType =>
          val offsetAddrVar = ctx.addMutableState("long", s"mapOffsetAddr$i")
          val keysColVar = ctx.addMutableState(columnVectorClz, s"mapKeysCol$i")
          val valuesColVar = ctx.addMutableState(columnVectorClz, s"mapValuesCol$i")
          val extraAssign =
            s"""
               |if ($colVarName instanceof $cometMapVectorClz) {
               |  $cometMapVectorClz cometMap$i = ($cometMapVectorClz) $colVarName;
               |  $offsetAddrVar = cometMap$i.getOffsetBufferAddress();
               |  $keysColVar = cometMap$i.getKeysVector();
               |  $valuesColVar = cometMap$i.getValuesVector();
               |}
             """.stripMargin
          ColumnInfo(
            colVarName,
            baseAssign + extraAssign,
            attr.dataType,
            attr.nullable,
            mapInfo = Some((offsetAddrVar, keysColVar, valuesColVar)))

        case _ =>
          ColumnInfo(colVarName, baseAssign, attr.dataType, attr.nullable)
      }
    }

    val colVars = columnInfos.map(_.colVar)
    val columnAssigns = columnInfos.map(_.assignCode)

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
    val columnsBatchInput = columnInfos.map { info =>
      (info.arrayInfo, info.mapInfo) match {
        case (Some((offsetAddrVar, dataColVar)), _) =>
          // Use optimized code generation for ArrayType
          genCodeForCometArray(
            ctx,
            info.colVar,
            rowidx,
            offsetAddrVar,
            dataColVar,
            info.dataType,
            info.nullable)
        case (_, Some((offsetAddrVar, keysColVar, valuesColVar))) =>
          // Use optimized code generation for MapType
          genCodeForCometMap(
            ctx,
            info.colVar,
            rowidx,
            offsetAddrVar,
            keysColVar,
            valuesColVar,
            info.dataType,
            info.nullable)
        case _ =>
          // Use standard code generation for other types
          genCodeColumnVector(ctx, info.colVar, rowidx, info.dataType, info.nullable)
      }
    }
    val localIdx = ctx.freshName("localIdx")
    val localEnd = ctx.freshName("localEnd")
    val numRows = ctx.freshName("numRows")
    val shouldStop = if (parent.needStopCheck) {
      s"if (shouldStop()) { $idx = $rowidx + 1; return; }"
    } else {
      "// shouldStop check is eliminated"
    }

    val writableColumnVectorClz = classOf[WritableColumnVector].getName
    val constantColumnVectorClz = classOf[ConstantColumnVector].getName
    val cometPlainColumnVectorClz = classOf[CometPlainVector].getName

    // scalastyle:off line.size.limit
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
       |
       |  // Comet fix for SPARK-50235
       |  for (int i = 0; i < ${colVars.length}; i++) {
       |    if (!($batch.column(i) instanceof $writableColumnVectorClz || $batch.column(i) instanceof $constantColumnVectorClz || $batch.column(i) instanceof $cometPlainColumnVectorClz)) {
       |      $batch.column(i).close();
       |    } else if ($batch.column(i) instanceof $cometPlainColumnVectorClz) {
       |      $cometPlainColumnVectorClz cometPlainColumnVector = ($cometPlainColumnVectorClz) $batch.column(i);
       |      if (!cometPlainColumnVector.isReused()) {
       |        cometPlainColumnVector.close();
       |      }
       |    }
       |  }
       |
       |  $batch = null;
       |  $nextBatchFuncName();
       |}
       |// Comet fix for SPARK-50235: clean up resources
       |if ($batch != null) {
       |  $batch.close();
       |}
     """.stripMargin
    // scalastyle:on line.size.limit
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    Seq(child.executeColumnar().asInstanceOf[RDD[InternalRow]]) // Hack because of type erasure
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CometColumnarToRowExec =
    copy(child = newChild)
}
