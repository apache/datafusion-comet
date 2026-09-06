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

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.shuffle.{BaseShuffleHandle, FetchFailedException, IndexShuffleBlockResolver, ShuffleBlockResolver, ShuffleHandle, ShuffleManager, ShuffleReader, ShuffleReadMetricsReporter, ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.{CometConf, CometShuffleSizeLimitException}
import org.apache.comet.shuffle.{CelebornShufflePartitionPusher, CelebornShufflePusherFactory, RecordingCelebornPushClient}

/** Exercises real Spark map tasks, native RSS planning, and the Celeborn map lifecycle. */
class CometCelebornNativeShuffleWriterSuite extends CometTestBase {

  import testImplicits._

  test("native iteration preserves fetch failures when closing also fails") {
    Seq(false, true).foreach { failOnHasNext =>
      val expected = new FetchFailedException(null, 19, 0L, 0, 0, "fetch failure sentinel", null)
      val closeFailure = new IOException("native iterator close sentinel")
      var closed = false
      val iterator = new Iterator[Int] {
        override def hasNext: Boolean = if (failOnHasNext) throw expected else true

        override def next(): Int = throw expected
      }
      val actual = intercept[FetchFailedException] {
        CometNativeShuffleWriter.drainAndClose(
          iterator,
          () => {
            closed = true
            throw closeFailure
          })
      }
      assert(actual eq expected)
      assert(actual.getSuppressed.toSeq == Seq(closeFailure))
      assert(closed)
    }
  }

  test("successful native iteration closes once and propagates standalone close failures") {
    var rows = 0
    var closes = 0
    CometNativeShuffleWriter.drainAndClose(
      Iterator(1, 2, 3).map { value =>
        rows += 1
        value
      },
      () => closes += 1)
    assert(rows == 3)
    assert(closes == 1)

    val expected = new IOException("standalone native iterator close failure")
    val actual = intercept[IOException] {
      CometNativeShuffleWriter.drainAndClose(Iterator.empty, () => throw expected)
    }
    assert(actual eq expected)
  }

  private def withNativeShuffleDependency(rangePartitioning: Boolean = false)(
      run: CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch] => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      "spark.sql.adaptive.enabled" -> "false") {
      val rows = (0 until 24).map { value =>
        val key = if (rangePartitioning) value % 2 else value
        (key, s"row-$value")
      }
      withParquetTable(rows, "celeborn_native_rows") {
        val input = sql("SELECT * FROM celeborn_native_rows")
        val shuffled =
          if (rangePartitioning) input.repartitionByRange(10, $"_1")
          else input.repartition(3, $"_1")
        val exchange = shuffled.queryExecution.executedPlan
          .collectFirst { case value: CometShuffleExchangeExec => value }
          .getOrElse(fail("Expected a native Comet shuffle exchange"))
        run(
          exchange.shuffleDependency
            .asInstanceOf[CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch]])
      }
    }
  }

  test("default pushers and the configured factory can share executor byte admission") {
    val client = new RecordingCelebornPushClient
    val defaultPusher = new CelebornShufflePartitionPusher(client, 19, 3, 0, 12, 9)
    val configuredPusher = CelebornShufflePusherFactory.create(
      new SparkConf(false),
      client,
      19,
      12,
      9,
      TaskContext.empty())

    assert(defaultPusher.maxReservationBytes() == configuredPusher.maxReservationBytes())
    defaultPusher.abort()
    configuredPusher.abort()
  }

  test("configured frame limits and executor admission both constrain native RSS callbacks") {
    val context = TaskContext.empty()
    val configured = new SparkConf(false)
      .set(CometConf.COMET_SHUFFLE_RSS_MAX_FRAME_BYTES.key, "128")
      .set(CometConf.COMET_SHUFFLE_RSS_MAX_IN_FLIGHT_BYTES.key, "4096")
    val limitedFrame = CelebornShufflePusherFactory.create(
      configured,
      new RecordingCelebornPushClient,
      89,
      1,
      1,
      context)
    assert(limitedFrame.maxFrameBytes() == 128)
    val tighterDestination = CelebornNativeShuffleDestination(limitedFrame, 64, 1)
    assert(tighterDestination.callback.maxFrameBytes() == 64)
    assert(tighterDestination.callback.maxReservationBytes() == 4096 - 16)
    tighterDestination.callback.reservePartitionData(64)
    tighterDestination.callback.releasePartitionDataReservation()

    val constrained =
      configured.clone().set(CometConf.COMET_SHUFFLE_RSS_MAX_IN_FLIGHT_BYTES.key, "112")
    val limitedAdmission = CelebornShufflePusherFactory.create(
      constrained,
      new RecordingCelebornPushClient,
      90,
      1,
      1,
      context)
    assert(limitedAdmission.maxFrameBytes() == 32)
    limitedFrame.abort()
    limitedAdmission.abort()
  }

  test("task interruption proactively aborts its task-owned Celeborn callback") {
    val interrupted = new CountDownLatch(1)
    val client = new RecordingCelebornPushClient {
      override def cleanup(shuffleId: Int, mapId: Int, attemptId: Int): Unit = {
        super.cleanup(shuffleId, mapId, attemptId)
        interrupted.countDown()
      }
    }
    val context = TaskContext.empty()
    val pusher = new CelebornShufflePartitionPusher(client, 90, 0, 0, 1, 1)
    val watcher = CelebornNativeShuffleDestination.watchForCancellation(
      context,
      pusher,
      failure => throw failure)

    try {
      context.markInterrupted("Spark cancelled its native shuffle map attempt")
      assert(interrupted.await(5, TimeUnit.SECONDS))
      assert(client.cleanupCalls.get() == 1)
    } finally {
      watcher.cancel(false)
      pusher.abort()
    }
  }

  test("native map attempts push complete frames and commit remotely without local files") {
    withNativeShuffleDependency() { dependency =>
      val numMappers = dependency.rdd.getNumPartitions
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val client = new RecordingCelebornPushClient
          val pusher = CelebornShufflePusherFactory.create(
            SparkEnv.get.conf,
            client,
            91,
            numMappers,
            dependency.partitioner.numPartitions,
            context)
          assert(
            SparkEnv.get.outputCommitCoordinator.canCommit(
              context.stageId(),
              context.stageAttemptNumber(),
              context.partitionId(),
              context.attemptNumber()))
          var validations = 0
          val writer = CometCelebornNativeShuffleWriterSuite.newWriter(
            dependency,
            context,
            pusher,
            commitAuthorized = true,
            commitValidator = () => {
              validations += 1
              true
            })
          val plan = writer.buildUnifiedPlan("", "").getShuffleWriter
          assert(plan.getPartitionWriter.hasRss)
          assert(plan.getOutputDataFile.isEmpty)
          assert(plan.getOutputIndexFile.isEmpty)

          writer.write(inputs)
          val status = writer.stop(success = true).get
          val localDataFile = SparkEnv.get.shuffleManager.shuffleBlockResolver
            .asInstanceOf[IndexShuffleBlockResolver]
            .getDataFile(dependency.shuffleId, context.taskAttemptId())

          (
            writer.getPartitionLengths(),
            status.mapId,
            context.taskAttemptId(),
            client.mapperEndCalls.get(),
            client.cleanupCalls.get(),
            Option(client.lastPush).map(_.length).getOrElse(0),
            localDataFile.exists(),
            validations,
            writer.stop(success = false).isEmpty)
        })

      assert(results.nonEmpty)
      assert(results.forall(_._1.length == 3))
      assert(results.exists(_._1.sum > 0))
      assert(results.forall(result => result._2 == result._3))
      assert(results.forall(_._4 == 1))
      assert(results.forall(_._5 == 0))
      assert(results.exists(_._6 >= 20))
      assert(results.forall(!_._7))
      assert(results.forall(_._8 == 2))
      assert(results.forall(_._9))
    }
  }

  test("mapper completion failures abort remote attempts without publishing MapStatus") {
    withNativeShuffleDependency() { dependency =>
      val numMappers = dependency.rdd.getNumPartitions
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val client = new RecordingCelebornPushClient
          val expected = new IOException("Celeborn rejected completed map output")
          client.mapperEndFailure = expected
          val pusher = CelebornShufflePusherFactory.create(
            SparkEnv.get.conf,
            client,
            92,
            numMappers,
            dependency.partitioner.numPartitions,
            context)
          val writer =
            CometCelebornNativeShuffleWriterSuite.newWriter(dependency, context, pusher)
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
            writer.mapStatus == null,
            writer.stop(success = false).isEmpty)
        })

      assert(results.nonEmpty)
      assert(results.forall(_._1))
      assert(results.forall(_._2 == 1))
      assert(results.forall(_._3 == 1))
      assert(results.forall(_._4))
      assert(results.forall(_._5))
    }
  }

  test("native frame limit failures abort before requesting a shuffle fallback") {
    withNativeShuffleDependency() { dependency =>
      val numMappers = dependency.rdd.getNumPartitions
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val client = new RecordingCelebornPushClient
          val pusher = CelebornShufflePusherFactory.create(
            SparkEnv.get.conf.clone().set(CometConf.COMET_SHUFFLE_RSS_MAX_FRAME_BYTES.key, "20"),
            client,
            96,
            numMappers,
            dependency.partitioner.numPartitions,
            context)
          val restart =
            new FetchFailedException(
              null,
              dependency.shuffleId,
              -1L,
              -1,
              0,
              "restart locally",
              null)
          var fallbackCalls = 0
          var reportedFailure: Throwable = null
          var cleanupBeforeFallback = false
          val writer = CometCelebornNativeShuffleWriterSuite.newWriter(
            dependency,
            context,
            pusher,
            onSizeLimitExceeded = failure => {
              fallbackCalls += 1
              reportedFailure = failure
              cleanupBeforeFallback = client.cleanupCalls.get() == 1
              throw restart
            })
          val actual =
            try {
              writer.write(inputs)
              null
            } catch {
              case failure: FetchFailedException => failure
            }

          (
            actual eq restart,
            fallbackCalls,
            reportedFailure.isInstanceOf[CometShuffleSizeLimitException],
            cleanupBeforeFallback,
            client.pushCount,
            client.mapperEndCalls.get(),
            writer.mapStatus == null,
            writer.stop(success = false).isEmpty)
        })

      assert(results.nonEmpty)
      assert(results.forall(_._1))
      assert(results.forall(_._2 == 1))
      assert(results.forall(_._3))
      assert(results.forall(_._4))
      assert(results.forall(_._5 == 0))
      assert(results.forall(_._6 == 0))
      assert(results.forall(_._7))
      assert(results.forall(_._8))
    }
  }

  test("a size failure after a successful push discards the partial remote map") {
    withNativeShuffleDependency() { dependency =>
      val numMappers = dependency.rdd.getNumPartitions
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val expected = new CometShuffleSizeLimitException("single row exceeds RSS frame limit")
          val client = new RecordingCelebornPushClient {
            override def pushOrMergeData(
                shuffleId: Int,
                mapId: Int,
                attemptId: Int,
                partitionId: Int,
                bytes: Array[Byte],
                offset: Int,
                length: Int,
                numMappers: Int,
                numPartitions: Int,
                doPush: Boolean,
                skipCompress: Boolean): Int = {
              if (pushCount == 1) failure = expected
              super.pushOrMergeData(
                shuffleId,
                mapId,
                attemptId,
                partitionId,
                bytes,
                offset,
                length,
                numMappers,
                numPartitions,
                doPush,
                skipCompress)
            }
          }
          val pusher = CelebornShufflePusherFactory.create(
            SparkEnv.get.conf,
            client,
            97,
            numMappers,
            dependency.partitioner.numPartitions,
            context)
          var fallbackCalls = 0
          var cleanupBeforeFallback = false
          val writer = CometCelebornNativeShuffleWriterSuite.newWriter(
            dependency,
            context,
            pusher,
            onSizeLimitExceeded = failure => {
              assert(failure eq expected)
              fallbackCalls += 1
              cleanupBeforeFallback = client.cleanupCalls.get() == 1
            })
          val actual =
            try {
              writer.write(inputs)
              null
            } catch {
              case failure: CometShuffleSizeLimitException => failure
            }

          (
            actual eq expected,
            fallbackCalls,
            cleanupBeforeFallback,
            client.pushCount,
            client.mapperEndCalls.get(),
            writer.mapStatus == null,
            writer.stop(success = false).isEmpty)
        })

      assert(results.nonEmpty)
      assert(results.forall(_._1))
      assert(results.forall(_._2 == 1))
      assert(results.forall(_._3))
      assert(results.forall(_._4 == 2))
      assert(results.forall(_._5 == 0))
      assert(results.forall(_._6))
      assert(results.forall(_._7))
    }
  }

  test("transport failures do not request fallback based on their error message") {
    withNativeShuffleDependency() { dependency =>
      val numMappers = dependency.rdd.getNumPartitions
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val expected = new IOException("single row exceeds RSS frame limit")
          val client = new RecordingCelebornPushClient
          client.failure = expected
          val pusher = CelebornShufflePusherFactory.create(
            SparkEnv.get.conf,
            client,
            98,
            numMappers,
            dependency.partitioner.numPartitions,
            context)
          var fallbackCalls = 0
          val writer = CometCelebornNativeShuffleWriterSuite.newWriter(
            dependency,
            context,
            pusher,
            onSizeLimitExceeded = _ => fallbackCalls += 1)
          val actual =
            try {
              writer.write(inputs)
              null
            } catch {
              case failure: IOException => failure
            }

          (
            actual eq expected,
            fallbackCalls,
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

  for (adaptive <- Seq(false, true)) {
    test(
      s"a row exceeding the RSS frame limit roundtrips through local Comet shuffle: AQE=$adaptive") {
      // Exercise the reported 70 MiB row once; the AQE variant uses the same limit crossing at a
      // smaller scale. Disabling compression makes the encoded size deterministic.
      val payloadBytes = if (adaptive) 1024 * 1024 else 70 * 1024 * 1024
      val payload = "x" * payloadBytes
      val rows = Seq((0, "small"), (1, payload))
      val runtimeConf = SparkEnv.get.conf
      val previousCompression = runtimeConf.getOption("spark.shuffle.compress")
      runtimeConf.set("spark.shuffle.compress", "false")
      try {
        withSQLConf(
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_MODE.key -> "native",
          CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
          "spark.sql.adaptive.enabled" -> adaptive.toString) {
          withTempPath { path =>
            rows
              .toDF("key", "payload")
              .coalesce(1)
              .write
              .option("parquet.enable.dictionary", "false")
              .parquet(path.getCanonicalPath)
            val shuffled = spark.read.parquet(path.getCanonicalPath).repartition(3, $"key")
            val dependency = collect(shuffled.queryExecution.executedPlan) {
              case exchange: CometShuffleExchangeExec =>
                exchange.shuffleDependency
                  .asInstanceOf[CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch]]
            }.headOption.getOrElse(fail("Expected a native Comet shuffle exchange"))
            val numMappers = dependency.rdd.getNumPartitions
            val frameBytes = if (adaptive) "32k" else "64m"
            val remoteAttempts = spark.sparkContext.runJob(
              dependency.rdd,
              (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
                val client = new RecordingCelebornPushClient
                val pusher = CelebornShufflePusherFactory.create(
                  SparkEnv.get.conf
                    .clone()
                    .set(CometConf.COMET_SHUFFLE_RSS_MAX_FRAME_BYTES.key, frameBytes),
                  client,
                  99,
                  numMappers,
                  dependency.partitioner.numPartitions,
                  context)
                var fallbackRequested = false
                val writer = CometCelebornNativeShuffleWriterSuite.newWriter(
                  dependency,
                  context,
                  pusher,
                  onSizeLimitExceeded = _ => fallbackRequested = true)
                try {
                  writer.write(inputs)
                } catch {
                  case _: CometShuffleSizeLimitException =>
                }
                val failedWithoutCommit =
                  fallbackRequested && client.cleanupCalls.get() == 1 &&
                    client.mapperEndCalls.get() == 0 && writer.mapStatus == null
                writer.stop(success = false)
                failedWithoutCommit
              })
            assert(remoteAttempts.exists(identity))

            // The dependency is registered with the suite's real local Comet manager. Running
            // it again exercises native file output, Spark map-status publication, and the Comet
            // reader, including a frame larger than the remote limit and normal AQE stage reads.
            val actual = shuffled.collect().map(row => (row.getInt(0), row.getString(1))).toSeq
            assert(actual.sortBy(_._1) == rows)
          }
        }
      } finally {
        previousCompression match {
          case Some(value) => runtimeConf.set("spark.shuffle.compress", value)
          case None => runtimeConf.remove("spark.shuffle.compress")
        }
      }
    }
  }

  test("low-cardinality range shuffle plans use the actual reducer partition count") {
    withNativeShuffleDependency(rangePartitioning = true) { dependency =>
      val actualPartitions = dependency.partitioner.numPartitions
      assert(actualPartitions < dependency.outputPartitioning.get.numPartitions)
      val numMappers = dependency.rdd.getNumPartitions
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val client = new RecordingCelebornPushClient
          val pusher = CelebornShufflePusherFactory.create(
            SparkEnv.get.conf,
            client,
            93,
            numMappers,
            actualPartitions,
            context)
          val writer =
            CometCelebornNativeShuffleWriterSuite.newWriter(dependency, context, pusher)
          writer.write(inputs)
          writer.stop(success = true).get

          (
            writer.getPartitionLengths().length,
            Option(client.lastPush).map(_.numPartitions).getOrElse(actualPartitions))
        })

      assert(results.nonEmpty)
      assert(results.forall(_._1 == actualPartitions))
      assert(results.forall(_._2 == actualPartitions))
    }
  }

  test("speculative losing attempts cannot finalize or publish remote shuffle output") {
    withNativeShuffleDependency() { dependency =>
      val numMappers = dependency.rdd.getNumPartitions
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val client = new RecordingCelebornPushClient
          val pusher = CelebornShufflePusherFactory.create(
            SparkEnv.get.conf,
            client,
            94,
            numMappers,
            dependency.partitioner.numPartitions,
            context)
          val writer =
            CometCelebornNativeShuffleWriterSuite.newWriter(dependency, context, pusher)
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
              case denied: CommitDeniedException => denied
            }

          (
            failure != null && !failure.toTaskCommitDeniedReason.countTowardsTaskFailures,
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

  test("generation invalidation during completion cannot publish a stale MapStatus") {
    withNativeShuffleDependency() { dependency =>
      val numMappers = dependency.rdd.getNumPartitions
      val results = spark.sparkContext.runJob(
        dependency.rdd,
        (context: TaskContext, inputs: Iterator[Product2[Int, ColumnarBatch]]) => {
          val client = new RecordingCelebornPushClient
          val pusher = CelebornShufflePusherFactory.create(
            SparkEnv.get.conf,
            client,
            95,
            numMappers,
            dependency.partitioner.numPartitions,
            context)
          var validations = 0
          val writer = CometCelebornNativeShuffleWriterSuite.newWriter(
            dependency,
            context,
            pusher,
            commitAuthorized = true,
            commitValidator = () => {
              validations += 1
              validations == 1
            })
          val failure =
            try {
              writer.write(inputs)
              null
            } catch {
              case denied: CommitDeniedException => denied
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
      assert(results.forall(_._4 == 0))
      assert(results.forall(_._5))
      assert(results.forall(_._6))
    }
  }

  test("native registration rejects and cleans up Celeborn's delegated local fallback") {
    withNativeShuffleDependency() { dependency =>
      val backend = new CometCelebornNativeShuffleWriterSuite.LocalFallbackShuffleManager
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

private[shuffle] object CometCelebornNativeShuffleWriterSuite {

  def newWriter(
      dependency: CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch],
      context: TaskContext,
      pusher: CelebornShufflePartitionPusher,
      commitAuthorized: Boolean = false,
      commitValidator: () => Boolean = () => true,
      onSizeLimitExceeded: Throwable => Unit = _ => ())
      : CometNativeShuffleWriter[Int, ColumnarBatch] =
    new CometNativeShuffleWriter[Int, ColumnarBatch](
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
          pusher.maxFrameBytes(),
          dependency.partitioner.numPartitions,
          commitAuthorized,
          commitValidator,
          onSizeLimitExceeded)))

  final class LocalFallbackShuffleManager extends ShuffleManager {
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
      throw new AssertionError("Native Comet data must not reach a local fallback writer")

    override def getReader[K, C](
        handle: ShuffleHandle,
        startMapIndex: Int,
        endMapIndex: Int,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext,
        metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] =
      throw new AssertionError("Native Comet data must not reach a local fallback reader")

    override def shuffleBlockResolver: ShuffleBlockResolver = null

    override def unregisterShuffle(shuffleId: Int): Boolean = {
      unregisteredShuffle = Some(shuffleId)
      true
    }

    override def stop(): Unit = ()
  }
}
