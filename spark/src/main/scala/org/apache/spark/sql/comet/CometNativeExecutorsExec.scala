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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning, SinglePartition}
import org.apache.spark.sql.comet.execution.shuffle.{CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, DeserializeToObjectExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.protobuf.Descriptors.FieldDescriptor
import com.google.protobuf.Message

import org.apache.comet.{CometConf, CometExecIterator, CometExplainInfo, CometRuntimeException, NativeExecutorsClient}
import org.apache.comet.serde.{PartitioningOuterClass, QueryPlanSerde}
import org.apache.comet.serde.Config.ConfigMap
import org.apache.comet.serde.NativeExecutors.{QueryRequest, ResultEndpoints, Stage, StageInput}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.schema2Proto
import org.apache.comet.vector.NativeUtil

/**
 * Runs a whole query on a Comet Native Executor cluster instead of on Spark's executors. See
 * https://github.com/apache/datafusion-comet/issues/7288.
 *
 * This leaf replaces the plan Comet would otherwise execute. `doExecuteColumnar` submits the
 * query to the cluster's scheduler and waits for it on the driver; each partition of the RDD it
 * returns then reads one result partition from the executor that holds it.
 *
 * @param query
 *   the serialized `QueryRequest` built by [[CometNativeExecutors.buildQuery]]
 */
case class CometNativeExecutorsExec(
    override val output: Seq[Attribute],
    query: Array[Byte],
    schedulerUrl: String)
    extends LeafExecNode {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(s"$nodeName only produces columnar output")

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val endpoints =
      ResultEndpoints.parseFrom(new NativeExecutorsClient().submit(schedulerUrl, query))
    val partitions = endpoints.getEndpointsList.asScala.map { endpoint =>
      (endpoint.getLocation, endpoint.getTicket.toByteArray)
    }
    new CometNativeExecutorsRDD(sparkContext, partitions.toSeq, output.length)
  }

  override def stringArgs: Iterator[Any] =
    Iterator(output, s"${query.length} bytes", schedulerUrl)
}

private class CometNativeExecutorsPartition(
    override val index: Int,
    val location: String,
    val ticket: Array[Byte])
    extends Partition

/**
 * The result partitions of a query that ran on a Comet Native Executor cluster, each read from
 * the executor that holds it.
 */
private class CometNativeExecutorsRDD(
    sc: SparkContext,
    endpoints: Seq[(String, Array[Byte])],
    numColumns: Int)
    extends RDD[ColumnarBatch](sc, Nil) {

  override protected def getPartitions: Array[Partition] =
    endpoints.zipWithIndex.map { case ((location, ticket), index) =>
      new CometNativeExecutorsPartition(index, location, ticket): Partition
    }.toArray

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[CometNativeExecutorsPartition]
    val client = new NativeExecutorsClient()
    val nativeUtil = new NativeUtil()
    val handle = client.openPartition(partition.location, partition.ticket)
    val batches = new Iterator[ColumnarBatch] with AutoCloseable {
      private var current: Option[ColumnarBatch] = None
      private var pending: Option[ColumnarBatch] = None
      private var finished = false
      private var closed = false

      override def hasNext: Boolean = {
        if (pending.isEmpty && !finished) {
          // The consumer has moved past the batch it last got.
          current.foreach(_.close())
          current = None
          pending = nativeUtil.getNextBatch(
            numColumns,
            (arrays, schemas) => client.nextBatch(handle, arrays, schemas))
          if (pending.isEmpty) {
            finished = true
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
        current.foreach(_.close())
        current = None
        pending.foreach(_.close())
        pending = None
        client.closePartition(handle)
        nativeUtil.close()
      }
    }
    context.addTaskCompletionListener[Unit](_ => batches.close())
    batches
  }
}

/**
 * Plans queries for Comet Native Executors: turns the plan Comet would execute into the stage
 * graph a cluster runs, and refuses any query that cannot run entirely natively.
 */
object CometNativeExecutors {

  /** Messages of native plans that call back into the JVM, which a cluster doesn't have. */
  private val jvmCallbacks = Set("JvmScalarUdf", "Subquery", "ArrowPythonUdf")

  /**
   * Whether Comet runs `plan` on a Comet Native Executor cluster. It must be a whole query: not a
   * command, not a stage AQE has split off, and not a plan already replaced.
   */
  def applies(conf: SQLConf, plan: SparkPlan, queryStagePrep: Boolean): Boolean =
    CometConf.COMET_NATIVE_EXECUTORS_ENABLED.get(conf) &&
      !(conf.adaptiveExecutionEnabled && !queryStagePrep && plan.isInstanceOf[Exchange]) &&
      !plan.exists {
        case _: CometNativeExecutorsExec | _: AdaptiveSparkPlanExec | _: QueryStageExec |
            _: ExecutedCommandExec | _: DataWritingCommandExec | _: V2CommandExec =>
          true
        case _ => false
      }

  /**
   * Replaces `converted`, the plan Comet would execute for `original`, with a leaf that runs it
   * on a Comet Native Executor cluster. An operator on top that only turns the query's result
   * into JVM objects, as `Dataset.rdd` adds, stays on the driver above the leaf.
   */
  def offload(original: SparkPlan, converted: SparkPlan): SparkPlan = converted match {
    case toObjects: DeserializeToObjectExec =>
      toObjects.withNewChildren(Seq(offload(toObjects.child, toObjects.child)))
    case native =>
      val exec = CometNativeExecutorsExec(
        native.output,
        buildQuery(native).toByteArray,
        CometConf.COMET_NATIVE_EXECUTORS_SCHEDULER_URL.get())
      native.logicalLink.orElse(original.logicalLink).foreach(exec.setLogicalLink)
      exec
  }

  /**
   * The query a Comet Native Executor cluster runs for `plan`, the plan Comet would execute: one
   * stage per native block, with an input for each exchange that feeds the block.
   *
   * @throws CometRuntimeException
   *   naming the first operator that cannot run on a cluster
   */
  def buildQuery(plan: SparkPlan): QueryRequest = {
    val builder = new StageGraphBuilder(plan)
    val root = plan match {
      case ColumnarToRowExec(child) => child
      case CometColumnarToRowExec(child) => child
      case CometNativeColumnarToRowExec(child) => child
      case other => other
    }
    builder.stageOf(root)
    QueryRequest
      .newBuilder()
      .putAllConf(nativeConf())
      .addAllStages(builder.stages.asJava)
      .build()
  }

  /** The configs a Spark task would pass to native code with a plan. */
  private def nativeConf(): java.util.Map[String, String] = {
    val conf = new java.util.HashMap[String, String](
      ConfigMap.parseFrom(CometExecIterator.serializeCometSQLConfs()).getEntriesMap)
    // A Spark task passes the batch size apart from the other configs.
    conf.put(CometConf.COMET_BATCH_SIZE.key, CometConf.COMET_BATCH_SIZE.get().toString)
    conf
  }

  private class StageGraphBuilder(query: SparkPlan) {
    val stages: ArrayBuffer[Stage] = ArrayBuffer.empty
    private val stageIndexes = new IdentityHashMap[SparkPlan, Integer]()

    /** The index of the stage that runs the native block rooted at `plan`, adding it first. */
    def stageOf(plan: SparkPlan): Int = {
      val known = stageIndexes.get(plan)
      if (known != null) {
        return known
      }
      val block = plan match {
        case native: CometNativeExec if native.serializedPlanOpt.isDefined => native
        case other => fail(other, "it does not run natively")
      }
      val exchanges = inputsOf(block)
      val inputs = exchanges.map { exchange =>
        StageInput
          .newBuilder()
          .setProducer(stageOf(exchange.child))
          .setPartitioning(partitioningOf(exchange))
          .build()
      }
      val blockPlan = Operator.parseFrom(block.serializedPlanOpt.plan.get)
      findJvmCallback(blockPlan).foreach { name =>
        fail(block, s"its native plan calls back into the JVM ($name)")
      }

      // A block that reads files runs one copy per partition, each naming its own files.
      val (common, perPartition) = PlanDataInjector.findAllPlanData(block)
      val partitionPlans = if (perPartition.isEmpty) {
        Seq.empty
      } else {
        val base = new PlanDataInjector.CachedPlanData(blockPlan)
        (0 until perPartition.values.head.length).map { i =>
          val partitionData = perPartition.map { case (key, data) => key -> data(i) }
          PlanDataInjector.injectPlanData(base, common, partitionData).toByteString
        }
      }
      // The cluster reads it only for a stage that reads no exchange. A stage that does has as
      // many partitions as its exchanges deliver.
      val numPartitions =
        if (partitionPlans.nonEmpty) partitionPlans.length
        else block.outputPartitioning.numPartitions
      // Declared as Comet's shuffle writer declares its input, since the cluster casts the
      // block's output to it as that writer does.
      val outputSchema = schema2Proto(
        block.output.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

      stages += Stage
        .newBuilder()
        .setPlan(blockPlan)
        .addAllInputs(inputs.asJava)
        .addAllPartitionPlans(partitionPlans.asJava)
        .setNumPartitions(numPartitions)
        .addAllOutputSchema(outputSchema.asJava)
        .build()
      val index = stages.length - 1
      stageIndexes.put(plan, index)
      index
    }

    /**
     * The exchanges that feed `block`, in the order its native plan's input leaves read them.
     */
    private def inputsOf(block: CometNativeExec): Seq[CometShuffleExchangeExec] = {
      val inputs = ArrayBuffer.empty[CometShuffleExchangeExec]
      def walk(plan: SparkPlan): Unit = plan match {
        case exchange: CometShuffleExchangeExec if exchange.shuffleType == CometNativeShuffle =>
          inputs += exchange
        case _: CometNativeScanExec | _: CometIcebergNativeScanExec | _: CometRangeExec =>
        // Read inside the block.
        // Part of the block, even when it has a serialized plan of its own: the conversion
        // serializes some operators before their parent joins them into a larger block.
        case native: CometNativeExec =>
          native.children.foreach(walk)
        case other => fail(other, "Comet Native Executors do not support it yet")
      }
      walk(block)
      inputs.toSeq
    }

    private def partitioningOf(
        exchange: CometShuffleExchangeExec): PartitioningOuterClass.Partitioning = {
      val partitioning = PartitioningOuterClass.Partitioning.newBuilder()
      exchange.outputPartitioning match {
        case HashPartitioning(expressions, numPartitions) =>
          val keys = expressions.map { key =>
            QueryPlanSerde
              .exprToProto(key, exchange.child.output)
              .getOrElse(fail(exchange, s"its partitioning key $key cannot run natively"))
          }
          partitioning.setHashPartition(
            PartitioningOuterClass.HashPartition
              .newBuilder()
              .addAllHashExpression(keys.asJava)
              .setNumPartitions(numPartitions))
        case RangePartitioning(ordering, numPartitions) =>
          // Without bounds: the cluster decides how to range-partition.
          val sortOrders = ordering.map { order =>
            QueryPlanSerde
              .exprToProto(order, exchange.child.output)
              .getOrElse(fail(exchange, s"its sort key $order cannot run natively"))
          }
          partitioning.setRangePartition(
            PartitioningOuterClass.RangePartition
              .newBuilder()
              .addAllSortOrders(sortOrders.asJava)
              .setNumPartitions(numPartitions))
        case SinglePartition =>
          partitioning.setSinglePartition(PartitioningOuterClass.SinglePartition.newBuilder())
        case other =>
          fail(exchange, s"Comet Native Executors do not support $other exchanges yet")
      }
      partitioning.build()
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

  /** The first message in `message` that calls back into the JVM, if any. */
  private def findJvmCallback(message: Message): Option[String] = {
    val name = message.getDescriptorForType.getName
    if (jvmCallbacks.contains(name)) {
      Some(name)
    } else {
      val found = message.getAllFields.asScala.iterator.flatMap { case (field, value) =>
        if (field.getJavaType != FieldDescriptor.JavaType.MESSAGE) {
          Iterator.empty
        } else if (field.isRepeated) {
          value.asInstanceOf[java.util.List[Message]].asScala.iterator.flatMap(findJvmCallback)
        } else {
          findJvmCallback(value.asInstanceOf[Message]).iterator
        }
      }
      if (found.hasNext) Some(found.next()) else None
    }
  }
}
