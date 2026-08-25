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

package org.apache.spark.sql.comet.execution.shuffle

import java.io.IOException
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.scheduler.OutputCommitCoordinator
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.RpcUtils

import org.apache.comet.CometConf
import org.apache.comet.shuffle.{CelebornShufflePusherFactory, RecordingCelebornShuffleClient}

/**
 * Exercises a real Spark map task, native RSS planning, JNI callback, and Celeborn map lifecycle.
 */
class CometCelebornNativeShuffleWriterSuite extends CometTestBase {

  import testImplicits._

  private final class LocalFallbackShuffleManager extends ShuffleManager {
    var unregisteredShuffle: Option[Int] = None

    override def registerShuffle[K, V, C](
        shuffleId: Int,
        dependency: ShuffleDependency[K, V, C]): ShuffleHandle =
      new BaseShuffleHandle(shuffleId, dependency)

    override def getWriter[K, V](
        handle: ShuffleHandle,
        mapId: Long,
        context: TaskContext,
        metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] =
      throw new AssertionError("Native Comet data must never reach a local fallback writer")

    override def getReader[K, C](
        handle: ShuffleHandle,
        startMapIndex: Int,
        endMapIndex: Int,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext,
        metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] =
      throw new AssertionError("Native Comet data must never reach a local fallback reader")

    override def shuffleBlockResolver: ShuffleBlockResolver = null

    override def unregisterShuffle(shuffleId: Int): Boolean = {
      unregisteredShuffle = Some(shuffleId)
      true
    }

    override def stop(): Unit = ()
  }

  private def withNativeShuffleDependency(
      run: CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch] => Unit): Unit =
    withNativeShuffleDependency(false, run)

  private def withNativeShuffleDependency(
      useRangePartitioning: Boolean,
      run: CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch] => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      "spark.sql.adaptive.enabled" -> "false") {
      val rows = (0 until 24).map { value =>
        val key = if (useRangePartitioning) value % 2 else value
        (key, s"row-$value")
      }
      withParquetTable(rows, "celeborn_rows") {
        val input = sql("SELECT * FROM celeborn_rows")
        val shuffled =
          if (useRangePartitioning) input.repartitionByRange(10, $"_1")
          else input.repartition(3, $"_1")
        val exchange = shuffled.queryExecution.executedPlan
          .collectFirst { case value: CometShuffleExchangeExec =>
            value
          }
          .getOrElse {
            fail("Expected a native Comet shuffle exchange")
          }
        val dependency = exchange.shuffleDependency
          .asInstanceOf[CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch]]
        run(dependency)
      }
    }
  }

  test("preauthorized native Spark maps reject speculation and commit remotely without files") {
    withNativeShuffleDependency { dependency =>
      val numMappers = dependency.rdd.getNumPartitions
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val client = new RecordingCelebornShuffleClient
          val taskConf = SparkEnv.get.conf
            .clone()
            .set("spark.celeborn.master.endpoints", "recording-celeborn:9097")
          val pusher = CelebornShufflePusherFactory
            .create(
              taskConf,
              client,
              celebornShuffleId = 91,
              numMappers = numMappers,
              numPartitions = dependency.partitioner.numPartitions,
              taskContext = context)
            .get
          val coordinator = SparkEnv.get.outputCommitCoordinator
          assert(
            coordinator.canCommit(
              context.stageId(),
              context.stageAttemptNumber(),
              context.partitionId(),
              context.attemptNumber()))
          assert(
            !coordinator.canCommit(
              context.stageId(),
              context.stageAttemptNumber(),
              context.partitionId(),
              context.attemptNumber() + 1))
          assert(client.lastInvalidatedShuffle == null)
          var commitValidations = 0
          val writer = new CometNativeShuffleWriter[Int, ColumnarBatch](
            dependency.nativeShuffleSpec.get,
            dependency.outputPartitioning.get,
            dependency.outputAttributes,
            dependency.shuffleWriteMetrics,
            dependency.numParts,
            dependency.shuffleId,
            context.taskAttemptId(),
            context,
            context.taskMetrics().shuffleWriteMetrics,
            dependency.rangePartitionBounds,
            Some(
              CelebornNativeShuffleDestination(
                pusher,
                1024 * 1024,
                dependency.partitioner.numPartitions,
                commitAuthorized = true,
                commitValidator = () => {
                  commitValidations += 1
                  client.lastInvalidatedShuffle == null
                })))

          writer.write(inputs)
          val status = writer.stop(success = true).get
          val localFile = SparkEnv.get.shuffleManager.shuffleBlockResolver
            .asInstanceOf[IndexShuffleBlockResolver]
            .getDataFile(dependency.shuffleId, context.taskAttemptId())
          assert(commitValidations == 2)
          assert(client.lastInvalidatedShuffle == null)

          (
            writer.getPartitionLengths(),
            status.mapId,
            context.taskAttemptId(),
            client.mapperEndCalls.get(),
            client.cleanupCalls.get(),
            Option(client.lastPush).map(_.length).getOrElse(0),
            localFile.exists(),
            writer.stop(success = false).isEmpty)
        })

      assert(results.nonEmpty)
      assert(results.forall(_._1.length == 3))
      assert(results.exists(_._1.sum > 0))
      assert(results.forall(result => result._2 == result._3))
      assert(results.forall(_._4 == 1))
      assert(results.forall(_._5 == 1))
      assert(results.exists(_._6 >= 20))
      assert(results.forall(!_._7))
      assert(results.forall(_._8))
    }
  }

  test("asynchronous remote push failures abort the Spark map attempt without a MapStatus") {
    withNativeShuffleDependency { dependency =>
      val numMappers = dependency.rdd.getNumPartitions
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val client = new RecordingCelebornShuffleClient
          val expected = new IOException("a pending Celeborn worker push failed")
          client.mapperEndFailure = expected
          val taskConf = SparkEnv.get.conf
            .clone()
            .set("spark.celeborn.master.endpoints", "recording-celeborn:9097")
          val pusher = CelebornShufflePusherFactory
            .create(
              taskConf,
              client,
              celebornShuffleId = 93,
              numMappers = numMappers,
              numPartitions = dependency.partitioner.numPartitions,
              taskContext = context)
            .get
          val writer = new CometNativeShuffleWriter[Int, ColumnarBatch](
            dependency.nativeShuffleSpec.get,
            dependency.outputPartitioning.get,
            dependency.outputAttributes,
            dependency.shuffleWriteMetrics,
            dependency.numParts,
            dependency.shuffleId,
            context.taskAttemptId(),
            context,
            context.taskMetrics().shuffleWriteMetrics,
            dependency.rangePartitionBounds,
            Some(
              CelebornNativeShuffleDestination(
                pusher,
                1024 * 1024,
                dependency.partitioner.numPartitions)))

          val failure =
            try {
              writer.write(inputs)
              null
            } catch {
              case error: IOException => error
            }

          (
            failure eq expected,
            client.mapperEndCalls.get(),
            client.cleanupCalls.get(),
            writer.stop(success = false).isEmpty,
            writer.mapStatus == null)
        })

      assert(results.nonEmpty)
      assert(results.forall(_._1))
      assert(results.forall(_._2 == 1))
      assert(results.forall(_._3 == 1))
      assert(results.forall(_._4))
      assert(results.forall(_._5))
    }
  }

  test("low-cardinality range shuffles use Spark's actual reducer partition count") {
    withNativeShuffleDependency(
      useRangePartitioning = true,
      dependency => {
        val actualPartitions = dependency.partitioner.numPartitions
        val requestedPartitions = dependency.outputPartitioning.get.numPartitions
        assert(actualPartitions < requestedPartitions)

        val numMappers = dependency.rdd.getNumPartitions
        val results = spark.sparkContext.runJob(
          dependency.rdd,
          (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
            val client = new RecordingCelebornShuffleClient
            val taskConf = SparkEnv.get.conf
              .clone()
              .set("spark.celeborn.master.endpoints", "recording-celeborn:9097")
            val pusher = CelebornShufflePusherFactory
              .create(taskConf, client, 97, numMappers, actualPartitions, context)
              .get
            val writer = new CometNativeShuffleWriter[Int, ColumnarBatch](
              dependency.nativeShuffleSpec.get,
              dependency.outputPartitioning.get,
              dependency.outputAttributes,
              dependency.shuffleWriteMetrics,
              dependency.numParts,
              dependency.shuffleId,
              context.taskAttemptId(),
              context,
              context.taskMetrics().shuffleWriteMetrics,
              dependency.rangePartitionBounds,
              Some(CelebornNativeShuffleDestination(pusher, 1024 * 1024, actualPartitions)))

            writer.write(inputs)
            writer.stop(success = true).get
            (
              writer.getPartitionLengths().length,
              Option(client.lastPush).map(_.numPartitions).getOrElse(actualPartitions))
          })

        assert(results.nonEmpty)
        assert(results.forall(_._1 == actualPartitions))
        assert(results.forall(_._2 == actualPartitions))
      })
  }

  test("a losing map attempt cannot commit Celeborn data or publish Spark MapStatus") {
    withNativeShuffleDependency { dependency =>
      val numMappers = dependency.rdd.getNumPartitions
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val client = new RecordingCelebornShuffleClient
          val taskConf = SparkEnv.get.conf
            .clone()
            .set("spark.celeborn.master.endpoints", "recording-celeborn:9097")
          val pusher = CelebornShufflePusherFactory
            .create(
              taskConf,
              client,
              98,
              numMappers,
              dependency.partitioner.numPartitions,
              context)
            .get
          val writer = new CometNativeShuffleWriter[Int, ColumnarBatch](
            dependency.nativeShuffleSpec.get,
            dependency.outputPartitioning.get,
            dependency.outputAttributes,
            dependency.shuffleWriteMetrics,
            dependency.numParts,
            dependency.shuffleId,
            context.taskAttemptId(),
            context,
            context.taskMetrics().shuffleWriteMetrics,
            dependency.rangePartitionBounds,
            Some(
              CelebornNativeShuffleDestination(
                pusher,
                1024 * 1024,
                dependency.partitioner.numPartitions)))

          assert(
            SparkEnv.get.outputCommitCoordinator.canCommit(
              context.stageId(),
              context.stageAttemptNumber(),
              context.partitionId(),
              context.attemptNumber() + 1))
          val failure =
            try {
              writer.write(inputs)
              null
            } catch {
              case error: CommitDeniedException => error
            }

          (
            failure != null && failure.getMessage.contains("already owns") &&
              !failure.toTaskCommitDeniedReason.countTowardsTaskFailures,
            client.mapperEndCalls.get(),
            client.cleanupCalls.get(),
            writer.mapStatus == null,
            writer.stop(success = false).isEmpty)
        })

      assert(results.nonEmpty)
      assert(results.forall(_._1))
      assert(results.forall(_._2 == 0))
      assert(results.forall(_._3 == 1))
      assert(results.forall(_._4))
      assert(results.forall(_._5))
    }
  }

  test("a generation invalidated during mapperEnd cannot publish a stale Spark MapStatus") {
    withNativeShuffleDependency { dependency =>
      val numMappers = dependency.rdd.getNumPartitions
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val client = new RecordingCelebornShuffleClient
          val taskConf = SparkEnv.get.conf
            .clone()
            .set("spark.celeborn.master.endpoints", "recording-celeborn:9097")
          val pusher = CelebornShufflePusherFactory
            .create(
              taskConf,
              client,
              99,
              numMappers,
              dependency.partitioner.numPartitions,
              context)
            .get
          assert(
            SparkEnv.get.outputCommitCoordinator.canCommit(
              context.stageId(),
              context.stageAttemptNumber(),
              context.partitionId(),
              context.attemptNumber()))
          var validations = 0
          val writer = new CometNativeShuffleWriter[Int, ColumnarBatch](
            dependency.nativeShuffleSpec.get,
            dependency.outputPartitioning.get,
            dependency.outputAttributes,
            dependency.shuffleWriteMetrics,
            dependency.numParts,
            dependency.shuffleId,
            context.taskAttemptId(),
            context,
            context.taskMetrics().shuffleWriteMetrics,
            dependency.rangePartitionBounds,
            Some(
              CelebornNativeShuffleDestination(
                pusher,
                1024 * 1024,
                dependency.partitioner.numPartitions,
                commitAuthorized = true,
                commitValidator = () => {
                  validations += 1
                  validations == 1
                })))

          val failure =
            try {
              writer.write(inputs)
              null
            } catch {
              case error: CommitDeniedException => error
            }

          (
            failure != null && !failure.toTaskCommitDeniedReason.countTowardsTaskFailures,
            validations,
            client.mapperEndCalls.get(),
            client.cleanupCalls.get(),
            writer.mapStatus == null,
            writer.stop(success = false).isEmpty)
        })

      assert(results.nonEmpty)
      assert(results.forall(_._1))
      assert(results.forall(_._2 == 2))
      assert(results.forall(_._3 == 1))
      assert(results.forall(_._4 == 1))
      assert(results.forall(_._5))
      assert(results.forall(_._6))
    }
  }

  test("executor RPC coordinates Celeborn generations with the Spark driver's commit owners") {
    val sparkEnvironment = SparkEnv.get
    val sparkCoordinator = new OutputCommitCoordinator(new SparkConf(false), true)
    val coordinator = new CelebornShuffleGenerationCoordinator(sparkCoordinator)
    val stageId = 812345
    val endpointName = "CometCelebornShuffleGenerationCoordinatorSuite"
    val endpoint = sparkEnvironment.rpcEnv.setupEndpoint(
      endpointName,
      new CelebornShuffleGenerationEndpoint(sparkEnvironment.rpcEnv, coordinator))

    try {
      sparkCoordinator.getClass
        .getMethod("stageStart", java.lang.Integer.TYPE, java.lang.Integer.TYPE)
        .invoke(sparkCoordinator, Int.box(stageId), Int.box(0))
      val canCommit = sparkCoordinator.getClass
        .getMethod(
          "handleAskPermissionToCommit",
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE,
          java.lang.Integer.TYPE)
      val driver =
        RpcUtils.makeDriverRef(endpointName, sparkEnvironment.conf, sparkEnvironment.rpcEnv)
      val original = PrepareCelebornShuffleGeneration(7, 91, stageId, 0, 1)
      val replacement = original.copy(celebornShuffleId = 92, stageAttempt = 1)

      assert(driver.askSync[Boolean](original))
      assert(
        canCommit
          .invoke(sparkCoordinator, Int.box(stageId), Int.box(0), Int.box(0), Int.box(0))
          .asInstanceOf[Boolean])
      assert(driver.askSync[Boolean](replacement))
      assert(
        canCommit
          .invoke(sparkCoordinator, Int.box(stageId), Int.box(1), Int.box(0), Int.box(0))
          .asInstanceOf[Boolean])
      assert(!driver.askSync[Boolean](original))
    } finally {
      sparkEnvironment.rpcEnv.stop(endpoint)
    }
  }

  test("task interruption proactively cleans up a blocked Celeborn native push") {
    val client = new RecordingCelebornShuffleClient
    client.pushStarted = new CountDownLatch(1)
    client.allowPush = new CountDownLatch(1)
    client.cleanupUnblocksPush = true
    val taskContext = TaskContext.empty()
    val pusher =
      new org.apache.comet.shuffle.CelebornShufflePartitionPusher(client, 91, 0, 0, 1, 1)
    val callbackFailure = new AtomicReference[Throwable]()
    val cleanupFailure = new AtomicReference[Throwable]()
    val watcher = CelebornNativeShuffleDestination.watchForCancellation(
      taskContext,
      pusher,
      failure => cleanupFailure.set(failure))

    try {
      val worker = new Thread(() => {
        try pusher.pushPartitionData(0, Array[Byte](1), 1)
        catch { case failure: Throwable => callbackFailure.set(failure) }
      })
      worker.start()
      assert(client.pushStarted.await(5, TimeUnit.SECONDS))

      taskContext.markInterrupted("the Spark task was cancelled")
      worker.join(5000)

      assert(!worker.isAlive)
      assert(callbackFailure.get().isInstanceOf[IOException])
      assert(cleanupFailure.get() == null)
      assert(client.cleanupCalls.get() == 2)
    } finally {
      watcher.cancel(false)
      pusher.abort()
    }
  }

  test("task interruption proactively wakes blocked Celeborn map completion") {
    val client = new RecordingCelebornShuffleClient
    client.mapperEndStarted = new CountDownLatch(1)
    client.allowMapperEnd = new CountDownLatch(1)
    client.cleanupUnblocksMapperEnd = true
    val taskContext = TaskContext.empty()
    val pusher =
      new org.apache.comet.shuffle.CelebornShufflePartitionPusher(client, 92, 0, 0, 1, 1)
    val callbackFailure = new AtomicReference[Throwable]()
    val cleanupFailure = new AtomicReference[Throwable]()
    val watcher = CelebornNativeShuffleDestination.watchForCancellation(
      taskContext,
      pusher,
      failure => cleanupFailure.set(failure))

    try {
      val worker = new Thread(() => {
        try pusher.finish()
        catch { case failure: Throwable => callbackFailure.set(failure) }
      })
      worker.start()
      assert(client.mapperEndStarted.await(5, TimeUnit.SECONDS))

      taskContext.markInterrupted("the Spark task was cancelled during map completion")
      worker.join(5000)

      assert(!worker.isAlive)
      assert(callbackFailure.get().isInstanceOf[IOException])
      assert(cleanupFailure.get() == null)
      assert(client.mapperEndCalls.get() == 1)
      assert(client.cleanupCalls.get() == 1)
    } finally {
      watcher.cancel(false)
      pusher.abort()
    }
  }

  test("the existing local native map writer still commits local files and MapStatus") {
    withNativeShuffleDependency { dependency =>
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val writer = new CometNativeShuffleWriter[Int, ColumnarBatch](
            dependency.nativeShuffleSpec.get,
            dependency.outputPartitioning.get,
            dependency.outputAttributes,
            dependency.shuffleWriteMetrics,
            dependency.numParts,
            dependency.shuffleId,
            context.taskAttemptId(),
            context,
            context.taskMetrics().shuffleWriteMetrics,
            dependency.rangePartitionBounds)

          writer.write(inputs)
          val status = writer.stop(success = true).get
          val resolver = SparkEnv.get.shuffleManager.shuffleBlockResolver
            .asInstanceOf[IndexShuffleBlockResolver]

          (
            writer.getPartitionLengths(),
            status.mapId,
            context.taskAttemptId(),
            resolver.getDataFile(dependency.shuffleId, context.taskAttemptId()).exists(),
            resolver.getIndexFile(dependency.shuffleId, context.taskAttemptId()).exists())
        })

      assert(results.nonEmpty)
      assert(results.forall(_._1.length == 3))
      assert(results.exists(_._1.sum > 0))
      assert(results.forall(result => result._2 == result._3))
      assert(results.forall(_._4))
      assert(results.forall(_._5))
    }
  }

  test("Celeborn native registration rejects and cleans up a delegated local fallback") {
    withNativeShuffleDependency { dependency =>
      val backend = new LocalFallbackShuffleManager
      val manager =
        new CometCelebornShuffleManager(spark.sparkContext.getConf, true, (_, _) => backend)

      val failure = intercept[UnsupportedOperationException] {
        manager.registerShuffle(dependency.shuffleId, dependency)
      }

      assert(failure.getMessage.contains("local fallback"))
      assert(backend.unregisteredShuffle.contains(dependency.shuffleId))
    }
  }
}
