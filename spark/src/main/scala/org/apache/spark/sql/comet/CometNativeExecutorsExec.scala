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

import java.util.IdentityHashMap

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, DynamicPruning, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning, SinglePartition}
import org.apache.spark.sql.comet.execution.shuffle.{CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, CommandResultExec, DeserializeToObjectExec, LeafExecNode, ScalarSubquery, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Message

import org.apache.comet.{CometConf, CometExecIterator, CometExplainInfo, CometRuntimeException, NativeExecutorsClient}
import org.apache.comet.serde.{ExprOuterClass, PartitioningOuterClass, QueryPlanSerde}
import org.apache.comet.serde.Config.ConfigMap
import org.apache.comet.serde.NativeExecutors.{QueryRequest, ResultEndpoint, ResultEndpoints, Stage, StageInput}
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.schema2Proto
import org.apache.comet.vector.NativeUtil

/**
 * Runs a whole query on a Comet Native Executor cluster instead of on Spark's executors. See
 * https://github.com/apache/datafusion-comet/issues/7288.
 *
 * This leaf replaces the plan Comet would otherwise execute. Planning only checks that the plan
 * can run on a cluster ([[CometNativeExecutors.validate]]). `doExecuteColumnar` builds the query,
 * which lists the files the scans read, submits it to the cluster's scheduler and waits for it on
 * the driver; each partition of the RDD it returns then reads one result partition from the
 * executor that holds it.
 *
 * @param nativePlan
 *   the plan Comet would execute, which [[CometNativeExecutors.buildQuery]] turns into the
 *   `QueryRequest` the cluster runs
 * @param subqueryResults
 *   the query's scalar subqueries. They are expressions of this node, so Spark runs them before
 *   it executes the node, and their results replace them in the query before it is submitted.
 */
case class CometNativeExecutorsExec(
    override val output: Seq[Attribute],
    nativePlan: SparkPlan,
    schedulerUrl: String,
    subqueryResults: Seq[Expression] = Nil)
    extends LeafExecNode {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(s"$nodeName only produces columnar output")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    NativeExecutorsClient.requireNativeSupport()
    val request = CometNativeExecutors.withSubqueryResults(
      CometNativeExecutors.buildQuery(nativePlan).toByteArray,
      subqueryResults.collect { case sub: ScalarSubquery => sub })
    val endpoints =
      ResultEndpoints.parseFrom(new NativeExecutorsClient().submit(schedulerUrl, request))
    new CometNativeExecutorsRDD(
      sparkContext,
      endpoints.getEndpointsList.asScala.toSeq,
      output.length)
  }

  override def stringArgs: Iterator[Any] =
    Iterator(output, schedulerUrl)
}

private class CometNativeExecutorsPartition(override val index: Int, val endpoint: ResultEndpoint)
    extends Partition

/**
 * The result partitions of a query that ran on a Comet Native Executor cluster, each read from
 * the executor that holds it.
 */
private class CometNativeExecutorsRDD(
    sc: SparkContext,
    endpoints: Seq[ResultEndpoint],
    numColumns: Int)
    extends RDD[ColumnarBatch](sc, Nil) {

  override protected def getPartitions: Array[Partition] =
    endpoints.zipWithIndex.map { case (endpoint, index) =>
      new CometNativeExecutorsPartition(index, endpoint): Partition
    }.toArray

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[CometNativeExecutorsPartition]
    // A Spark executor needs a libcomet built with the feature too.
    NativeExecutorsClient.requireNativeSupport()
    val client = new NativeExecutorsClient()
    val nativeUtil = new NativeUtil()
    // The task's completion listener closes nativeUtil, but only once there is one to register.
    val handle =
      try {
        client.openPartition(
          partition.endpoint.getLocation,
          partition.endpoint.getTicket.toByteArray)
      } catch {
        case t: Throwable =>
          nativeUtil.close()
          throw t
      }
    val batches = new Iterator[ColumnarBatch] with AutoCloseable {
      private var current: Option[ColumnarBatch] = None
      private var pending: Option[ColumnarBatch] = None
      private var closed = false

      override def hasNext: Boolean = {
        if (pending.isEmpty && !closed) {
          // The consumer has moved past the batch it last got.
          current.foreach(_.close())
          current = None
          pending = nativeUtil.getNextBatch(
            numColumns,
            (arrays, schemas) => client.nextBatch(handle, arrays, schemas))
          if (pending.isEmpty) {
            close()
          }
        }
        pending.isDefined
      }

      override def next(): ColumnarBatch = {
        if (!hasNext) {
          throw new NoSuchElementException("No more batches in this result partition")
        }
        current = pending
        pending = None
        current.get
      }

      override def close(): Unit = if (!closed) {
        closed = true
        try {
          current.foreach(_.close())
          current = None
          pending.foreach(_.close())
          pending = None
        } finally {
          // Each step runs even if an earlier one throws.
          try client.closePartition(handle)
          finally nativeUtil.close()
        }
      }
    }
    context.addTaskCompletionListener[Unit](_ => batches.close())
    batches
  }
}

/**
 * Plans queries for Comet Native Executors: turns the plan Comet would execute into the stage
 * graph a cluster runs, and refuses any query that cannot run entirely in the Comet pipeline.
 */
object CometNativeExecutors {

  /**
   * Messages of native plans that call back into the JVM, which a cluster doesn't have. A scalar
   * subquery's `Subquery` is not one of them: the driver runs the subquery and puts its result in
   * the plan before submitting it. The native planner could refuse them all instead
   * (https://github.com/apache/datafusion-comet/issues/7375).
   */
  private val jvmCallbacks = Set("JvmScalarUdf", "ArrowPythonUdf")

  /**
   * Whether Comet runs `plan` on a Comet Native Executor cluster. It must be a whole query: not a
   * command, not a stage AQE has split off, and not a plan already replaced. The caller checks
   * that Comet executes the plan at all.
   */
  def applies(conf: SQLConf, plan: SparkPlan, queryStagePrep: Boolean): Boolean =
    CometConf.COMET_NATIVE_EXECUTORS_ENABLED.get(conf) &&
      !(conf.adaptiveExecutionEnabled && !queryStagePrep && plan.isInstanceOf[Exchange]) &&
      !plan.exists {
        case _: CometNativeExecutorsExec | _: AdaptiveSparkPlanExec | _: QueryStageExec |
            _: CommandResultExec | _: ExecutedCommandExec | _: DataWritingCommandExec |
            _: V2CommandExec =>
          true
        case _ => false
      }

  /**
   * Replaces `converted`, the plan Comet would execute for `original`, with a leaf that runs it
   * on a Comet Native Executor cluster. An operator on top that only turns the query's result
   * into JVM objects, as `Dataset.rdd` adds, stays on the driver above the leaf.
   */
  def offload(original: SparkPlan, converted: SparkPlan): SparkPlan = {
    NativeExecutorsClient.requireNativeSupport()
    offloadPlan(original, converted)
  }

  private def offloadPlan(original: SparkPlan, converted: SparkPlan): SparkPlan =
    converted match {
      case toObjects: DeserializeToObjectExec =>
        toObjects.withNewChildren(Seq(offloadPlan(toObjects.child, toObjects.child)))
      case native =>
        validate(native)
        val exec = CometNativeExecutorsExec(
          native.output,
          native,
          CometConf.COMET_NATIVE_EXECUTORS_SCHEDULER_URL.get(),
          scalarSubqueries(native))
        native.logicalLink.orElse(original.logicalLink).foreach(exec.setLogicalLink)
        exec
    }

  /**
   * Checks that a cluster can run `plan`, the plan Comet would execute, without running anything:
   * it lists no files and resolves no subquery, so it is safe at planning time.
   *
   * @throws CometRuntimeException
   *   naming the first operator that cannot run on a cluster
   */
  def validate(plan: SparkPlan): Unit = stageGraph(plan, withFiles = false)

  /**
   * The query a Comet Native Executor cluster runs for `plan`, the plan Comet would execute: one
   * stage per block of Comet operators that run as one Rust plan, with an input for each exchange
   * that feeds the block. A stage that reads files has a plan for each partition, naming its
   * files, so this lists them and resolves the subqueries in the scans' filters. It belongs at
   * execution time.
   *
   * @throws CometRuntimeException
   *   naming the first operator that cannot run on a cluster
   */
  def buildQuery(plan: SparkPlan): QueryRequest = {
    val stages = stageGraph(plan, withFiles = true)
    QueryRequest
      .newBuilder()
      .putAllConf(nativeConf())
      .addAllStages(stages.asJava)
      .build()
  }

  private def stageGraph(plan: SparkPlan, withFiles: Boolean): Seq[Stage] = {
    val builder = new StageGraphBuilder(plan, withFiles)
    val root = plan match {
      case ColumnarToRowExec(child) => child
      case CometColumnarToRowExec(child) => child
      case CometNativeColumnarToRowExec(child) => child
      case other => other
    }
    builder.stageOf(root)
    builder.stages.toSeq
  }

  /** The scalar subqueries of `plan`'s expressions, through exchanges, each once. */
  private[comet] def scalarSubqueries(plan: SparkPlan): Seq[Expression] = {
    val found = ArrayBuffer.empty[ScalarSubquery]
    def walk(node: SparkPlan): Unit = {
      node.expressions.foreach(_.foreach {
        case sub: ScalarSubquery if !found.exists(_.exprId == sub.exprId) => found += sub
        case _ =>
      })
      node match {
        case reused: ReusedExchangeExec => walk(reused.child)
        case _ => node.children.foreach(walk)
      }
    }
    walk(plan)
    found.toSeq
  }

  /**
   * `query` with each scalar subquery's `Subquery` expression replaced by a literal of its
   * result. The subqueries must have run. Sending the results alongside the plan would spare
   * rewriting it (https://github.com/apache/datafusion-comet/issues/7374).
   */
  def withSubqueryResults(query: Array[Byte], subqueries: Seq[ScalarSubquery]): Array[Byte] = {
    if (subqueries.isEmpty) {
      return query
    }
    val literals: Map[Long, ExprOuterClass.Expr] = subqueries.map { sub =>
      // The result is already in Catalyst's internal form.
      val literal = Literal(sub.eval(null), sub.dataType)
      val expr = QueryPlanSerde
        .exprToProto(literal, Seq.empty)
        .getOrElse(throw new CometRuntimeException(
          s"Comet Native Executors cannot pass the result of $sub to the cluster"))
      sub.exprId.id -> expr
    }.toMap
    val replace: PartialFunction[Message, Message] = {
      case expr: ExprOuterClass.Expr if expr.hasSubquery =>
        literals.getOrElse(
          expr.getSubquery.getId,
          throw new CometRuntimeException(
            s"Comet Native Executors found no result for subquery ${expr.getSubquery.getId}"))
    }
    val request = QueryRequest.parseFrom(query).toBuilder
    (0 until request.getStagesCount).foreach { i =>
      val stage = request.getStages(i)
      val plan = transformMessages(stage.getPlan)(replace)
      // A partition's plan is the stage's with its files added, so it holds a subquery only
      // when the stage's plan does.
      if (plan ne stage.getPlan) {
        val replaced = stage.toBuilder.setPlan(plan.asInstanceOf[Operator])
        (0 until stage.getPartitionPlansCount).foreach { p =>
          val partitionPlan = Operator.parseFrom(stage.getPartitionPlans(p))
          replaced.setPartitionPlans(p, transformMessages(partitionPlan)(replace).toByteString)
        }
        request.setStages(i, replaced)
      }
    }
    request.build().toByteArray
  }

  /**
   * The messages directly inside `message`, each with how to put a replacement for it in a
   * builder of `message`.
   */
  private def childMessages(
      message: Message): Iterator[(Message, (Message.Builder, Message) => Unit)] =
    message.getAllFields.asScala.iterator
      .filter { case (field, _) => field.getJavaType == FieldDescriptor.JavaType.MESSAGE }
      .flatMap { case (field, value) =>
        if (field.isRepeated) {
          value.asInstanceOf[java.util.List[Message]].asScala.iterator.zipWithIndex.map {
            case (child, index) =>
              (
                child,
                (builder: Message.Builder, m: Message) => {
                  builder.setRepeatedField(field, index, m)
                  ()
                })
          }
        } else {
          Iterator(
            (
              value.asInstanceOf[Message],
              (builder: Message.Builder, m: Message) => {
                builder.setField(field, m)
                ()
              }))
        }
      }

  /**
   * `message` with every message inside it that `pf` matches replaced by what `pf` returns. A
   * message nothing inside changed is returned as is.
   */
  private def transformMessages(message: Message)(
      pf: PartialFunction[Message, Message]): Message =
    pf.applyOrElse(
      message,
      (m: Message) => {
        var builder: Message.Builder = null
        childMessages(m).foreach { case (child, set) =>
          val replaced = transformMessages(child)(pf)
          if (replaced ne child) {
            if (builder == null) {
              builder = m.toBuilder
            }
            set(builder, replaced)
          }
        }
        if (builder == null) m else builder.build()
      })

  /** The configs a Spark task would pass to native code with a plan. */
  private def nativeConf(): java.util.Map[String, String] = {
    val conf = new java.util.HashMap[String, String](
      ConfigMap.parseFrom(CometExecIterator.serializeCometSQLConfs()).getEntriesMap)
    // A Spark task passes the batch size apart from the other configs.
    conf.put(CometConf.COMET_BATCH_SIZE.key, CometConf.COMET_BATCH_SIZE.get().toString)
    conf
  }

  private val singlePartition = PartitioningOuterClass.Partitioning
    .newBuilder()
    .setSinglePartition(PartitioningOuterClass.SinglePartition.newBuilder())
    .build()

  /** The planning data of a partition that reads no files. */
  private val noFiles = OperatorOuterClass.NativeScan
    .newBuilder()
    .setFilePartition(OperatorOuterClass.SparkFilePartition.getDefaultInstance)
    .build()
    .toByteArray

  private class StageGraphBuilder(query: SparkPlan, withFiles: Boolean) {
    val stages: ArrayBuffer[Stage] = ArrayBuffer.empty
    private val stageIndexes = new IdentityHashMap[SparkPlan, Integer]()

    /**
     * The index of the stage that runs the block of Comet operators rooted at `plan`, adding it
     * first.
     */
    def stageOf(plan: SparkPlan): Int = {
      val known = stageIndexes.get(plan)
      if (known != null) {
        return known
      }
      val block = plan match {
        case take: CometTakeOrderedAndProjectExec => return takeOrderedStage(take)
        case native: CometNativeExec if native.serializedPlanOpt.isDefined => native
        case other => fail(other, "it is not a Comet operator that runs in Rust")
      }
      val scans = ArrayBuffer.empty[CometNativeScanExec]
      val inputs = inputsOf(block, scans).map {
        case exchange: CometShuffleExchangeExec =>
          stageInput(exchange.child)(_.setPartitioning(partitioningOf(exchange)))
        case broadcast => stageInput(broadcast.children.head)(_.setBroadcast(true))
      }
      val blockPlan = Operator.parseFrom(block.serializedPlanOpt.plan.get)

      // A block that reads files runs one copy per partition, each naming its own files.
      val (common, perPartition) =
        if (withFiles) filesOf(block, scans.toSeq)
        else (Map.empty[String, Array[Byte]], Map.empty[String, Array[Array[Byte]]])
      val partitionPlans = if (perPartition.isEmpty) {
        Seq.empty
      } else {
        val base = new PlanDataInjector.CachedPlanData(blockPlan)
        val counts = perPartition.map { case (key, data) => key -> data.length }
        require(
          counts.values.toSet.size == 1,
          s"The scans of one block must have the same number of partitions, but found $counts")
        (0 until counts.values.head).map { i =>
          val partitionData = perPartition.map { case (key, data) => key -> data(i) }
          PlanDataInjector.injectPlanData(base, common, partitionData).toByteString
        }
      }
      // The cluster reads it only for a stage that reads no exchange. A stage that does has as
      // many partitions as its exchanges deliver.
      val numPartitions =
        if (partitionPlans.nonEmpty) partitionPlans.length
        else block.outputPartitioning.numPartitions
      addStage(plan, blockPlan, inputs, partitionPlans, numPartitions)
    }

    /**
     * The planning data of the scans in `block`. A block whose scans list no files at all, such
     * as a table with no data or a partition filter that prunes everything, gets one empty
     * partition for each, as Spark would return no rows from them.
     */
    private def filesOf(block: CometNativeExec, scans: Seq[CometNativeScanExec])
        : (Map[String, Array[Byte]], Map[String, Array[Array[Byte]]]) = {
      val found @ (_, perPartition) = PlanDataInjector.findAllPlanData(block)
      if (perPartition.nonEmpty || scans.isEmpty) {
        found
      } else {
        (
          scans.map(scan => scan.sourceKey -> scan.commonData).toMap,
          scans.map(scan => scan.sourceKey -> Array(noFiles)).toMap)
      }
    }

    /** An input of a stage that reads the stage running `producer`, as `configure` says. */
    private def stageInput(producer: SparkPlan)(
        configure: StageInput.Builder => StageInput.Builder): StageInput =
      configure(StageInput.newBuilder().setProducer(stageOf(producer))).build()

    /**
     * A stage for `take` that reads its child's whole output as one partition and keeps the first
     * rows in order, as Comet's final top-K plan does after its shuffle to one partition. A top-K
     * of each partition first would send fewer rows through the exchange
     * (https://github.com/apache/datafusion-comet/issues/7365).
     */
    private def takeOrderedStage(take: CometTakeOrderedAndProjectExec): Int = {
      val input = stageInput(take.child)(_.setPartitioning(singlePartition))
      // More than one input partition, so that the plan sorts across all of them.
      val blockPlan = take
        .finalNativePlan(inputPartitions = 2)
        .getOrElse(fail(take, "its top-K plan cannot be built"))
      addStage(take, blockPlan, Seq(input), Seq.empty, 1)
    }

    private def addStage(
        plan: SparkPlan,
        blockPlan: Operator,
        inputs: Seq[StageInput],
        partitionPlans: Seq[ByteString],
        numPartitions: Int): Int = {
      findJvmCallback(blockPlan).foreach { name =>
        fail(plan, s"its native plan calls back into the JVM ($name)")
      }
      // Declared as Comet's shuffle writer declares its input, since the cluster casts the
      // block's output to it as that writer does.
      val outputSchema = schema2Proto(
        plan.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
      val stage = Stage
        .newBuilder()
        .setPlan(blockPlan)
        .addAllInputs(inputs.asJava)
        .addAllPartitionPlans(partitionPlans.asJava)
        .setNumPartitions(numPartitions)
        .addAllOutputSchema(outputSchema.asJava)
      stages += stage.build()
      val index = stages.length - 1
      stageIndexes.put(plan, index)
      index
    }

    /**
     * The exchanges that feed `block`, in the order its native plan's input leaves read them:
     * native shuffles and broadcasts, with a reused exchange resolved to the one it reuses. Adds
     * the block's native Parquet scans to `scans`.
     */
    private def inputsOf(
        block: CometNativeExec,
        scans: ArrayBuffer[CometNativeScanExec]): Seq[SparkPlan] = {
      val inputs = ArrayBuffer.empty[SparkPlan]
      def walk(plan: SparkPlan): Unit = plan match {
        case reused: ReusedExchangeExec =>
          walk(reused.child)
        case exchange: CometShuffleExchangeExec if exchange.shuffleType == CometNativeShuffle =>
          inputs += exchange
        case broadcast: CometBroadcastExchangeExec =>
          inputs += broadcast
        // These read or generate their rows inside the block.
        case scan: CometNativeScanExec =>
          rejectDynamicPruning(scan, scan.partitionFilters)
          scans += scan
        case scan: CometIcebergNativeScanExec =>
          rejectDynamicPruning(scan, scan.runtimeFilters)
        case _: CometRangeExec =>
        // These stand for rows that the JVM feeds to the plan, which a cluster has no way to do.
        case _: CometScanWrapper | _: CometSinkPlaceHolder =>
          fail(plan, "its input comes from the JVM, which a cluster cannot read")
        case _: CometNativeWriteExec | _: CometWriteFilesExec | _: CometIcebergWriteExec =>
          fail(
            plan,
            "Comet Native Executors do not support writes yet " +
              "(https://github.com/apache/datafusion-comet/issues/7371)")
        // Any other scan: its files are not described to the cluster.
        case leaf: CometLeafExec =>
          fail(leaf, "Comet Native Executors do not support it yet")
        // Part of the block, even when it has a serialized plan of its own: the conversion
        // serializes some operators before their parent joins them into a larger block.
        case native: CometNativeExec =>
          native.children.foreach(walk)
        case other => fail(other, "Comet Native Executors do not support it yet")
      }
      walk(block)
      inputs.toSeq
    }

    /**
     * A scan that prunes partitions with a dynamic filter lists its files only once the filter's
     * subquery has run, which the driver cannot do before it builds the query. That includes the
     * placeholder AQE leaves in a filter until it plans the subquery.
     */
    private def rejectDynamicPruning(scan: SparkPlan, filters: Seq[Expression]): Unit = {
      val dynamic = CometScanUtils
        .filterUnusedDynamicPruningExpressions(filters)
        .exists(_.exists {
          case _: DynamicPruningExpression | _: DynamicPruning => true
          case _ => false
        })
      if (dynamic) {
        fail(
          scan,
          "it prunes partitions with a dynamic filter (dynamic partition pruning), which " +
            "Comet Native Executors do not support yet " +
            "(https://github.com/apache/datafusion-comet/issues/7370)")
      }
    }

    private def partitioningOf(
        exchange: CometShuffleExchangeExec): PartitioningOuterClass.Partitioning = {
      def partitioning = PartitioningOuterClass.Partitioning.newBuilder()
      exchange.outputPartitioning match {
        case HashPartitioning(expressions, numPartitions) =>
          val keys = expressions.map {
            // A cluster hashes columns of the stage's input.
            case key: Attribute =>
              QueryPlanSerde
                .exprToProto(key, exchange.child.output)
                .getOrElse(fail(exchange, s"its partitioning key $key is not supported by Comet"))
            case key =>
              fail(
                exchange,
                s"its partitioning key $key is not a column, which a cluster requires")
          }
          partitioning
            .setHashPartition(
              PartitioningOuterClass.HashPartition
                .newBuilder()
                .addAllHashExpression(keys.asJava)
                .setNumPartitions(numPartitions))
            .build()
        case RangePartitioning(ordering, numPartitions) =>
          // Without bounds, since Spark would sample for them: the cluster reads the exchange as
          // one partition, which the block then sorts.
          val sortOrders = ordering.map { order =>
            QueryPlanSerde
              .exprToProto(order, exchange.child.output)
              .getOrElse(fail(exchange, s"its sort key $order is not supported by Comet"))
          }
          partitioning
            .setRangePartition(
              PartitioningOuterClass.RangePartition
                .newBuilder()
                .addAllSortOrders(sortOrders.asJava)
                .setNumPartitions(numPartitions))
            .build()
        case SinglePartition => singlePartition
        case other =>
          fail(exchange, s"Comet Native Executors do not support $other exchanges yet")
      }
    }

    private def fail(node: SparkPlan, reason: String): Nothing = {
      val fallbackReasons = node
        .getTagValue(CometExplainInfo.FALLBACK_REASONS)
        .getOrElse(Set.empty[String])
      val because = if (fallbackReasons.isEmpty) {
        ""
      } else {
        fallbackReasons.toSeq.sorted.mkString(" Comet could not convert it: ", "; ", ".")
      }
      throw new CometRuntimeException(
        s"Comet Native Executors cannot run this query, because of ${node.nodeName}: " +
          s"$reason.$because\n${query.treeString}")
    }
  }

  /** `message` and every message inside it, depth first. */
  private def messagesIn(message: Message): Iterator[Message] =
    Iterator(message) ++ childMessages(message).flatMap { case (child, _) => messagesIn(child) }

  /** The first message in `message` that calls back into the JVM, if any. */
  private def findJvmCallback(message: Message): Option[String] =
    messagesIn(message).map(_.getDescriptorForType.getName).find(jvmCallbacks.contains)
}
