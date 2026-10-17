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

package org.apache.comet.parquet

import java.io.File

import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.commit.files.SuccessData
import org.apache.parquet.hadoop.ParquetOutputCommitter
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.comet.CometNativeWriteExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometS3TestBase
import org.apache.comet.serde.operator.NativeWriteUtils

import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest

/**
 * Native Parquet writes to S3 that S3A's magic committer commits, against MinIO.
 *
 * A manual suite like the other MinIO suites, since it needs Docker. Every session here gets the
 * magic committer the way Spark configures it when spark-hadoop-cloud is on the classpath: by
 * default on Spark 4.1+ (SPARK-47618), and for a bucket that asks for it on earlier versions. The
 * shared test configuration turns that off for every other suite.
 */
class ParquetWriteWithMagicCommitterSuite
    extends CometParquetWriterTestBase
    with CometS3TestBase {

  import testImplicits._

  override protected val testBucketName = "magic-committer"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
    conf.set(s"spark.hadoop.fs.s3a.bucket.$testBucketName.committer.magic.enabled", "true")
    conf
  }

  /** The multipart uploads still pending in `bucket`, as their object keys. */
  private def pendingUploads(bucket: String): Seq[String] = withS3Client { client =>
    client
      .listMultipartUploads(ListMultipartUploadsRequest.builder().bucket(bucket).build())
      .uploads()
      .asScala
      .map(_.key())
      .toSeq
  }

  /** `rows` rows read natively from a local copy, so the write's input is a Comet scan. */
  private def source(rows: Int, dir: File): DataFrame =
    materializeAsCometSource(
      (1 to rows).map(i => (i, s"str_$i")).toDF("id", "name"),
      new File(dir, "source.parquet").getAbsolutePath)

  /**
   * The files and bytes the write reports: from Spark's stats tracker on Spark 4.0+, which leaves
   * the write command in the plan, and from the native writer on 3.x, which replaces it.
   */
  private def writtenFilesAndBytes(plan: SparkPlan): (Long, Long) =
    plan
      .collectFirst {
        case write: DataWritingCommandExec =>
          (write.cmd.metrics("numFiles").value, write.cmd.metrics("numOutputBytes").value)
        case write: CometNativeWriteExec =>
          (write.metrics("files_written").value, write.metrics("bytes_written").value)
      }
      .getOrElse(fail(s"No write in the plan:\n${plan.treeString}"))

  /**
   * Writes `rows` rows in `files` tasks natively to `table` under `confs`, and checks that the
   * magic committer committed them: its `_SUCCESS` manifest names it and the files, no `__magic`
   * directory or pending upload is left behind, the write reports each file's real size, and the
   * data reads back. On Spark 4.0+ that size comes from the marker S3A writes when it closes a
   * magic file, and on 3.x from what the native writer counted.
   */
  private def checkMagicCommittedWrite(table: String, rows: Int, files: Int)(
      confs: (String, String)*): Unit =
    withTempPath { dir =>
      val df = source(rows, dir).repartition(files)
      withNativeWriter {
        withSQLConf(confs: _*) {
          val plan = captureWritePlan(p => df.write.parquet(p), table)
          assertHasCometNativeWriteExec(plan)

          val tablePath = new Path(table)
          val fs = tablePath.getFileSystem(spark.sessionState.newHadoopConf())
          val manifest = SuccessData.load(fs, new Path(tablePath, "_SUCCESS"))
          assert(manifest.getCommitter == "magic", manifest)
          assert(manifest.getFilenames.size == files, manifest)

          val entries = fs.listStatus(tablePath)
          assert(
            entries.forall(!_.getPath.getName.startsWith("__magic")),
            entries.map(_.getPath).mkString(", "))
          val parts = entries.filter(_.getPath.getName.startsWith("part-"))
          assert(parts.length == files, parts.map(_.getPath).mkString(", "))
          assert(pendingUploads(tablePath.toUri.getHost).isEmpty)
          assert(writtenFilesAndBytes(plan) == ((files.toLong, parts.map(_.getLen).sum)))
        }
      }
      checkAnswer(spark.read.parquet(table), df)
    }

  test("a native write is committed by S3A's magic committer") {
    // Spark's own magic committer configuration, rather than one this suite spells out.
    assert(
      spark.conf.get(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key) ==
        NativeWriteUtils.PathOutputCommitProtocolClass)
    assert(
      spark.conf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key) ==
        NativeWriteUtils.BindingParquetOutputCommitterClass)
    checkMagicCommittedWrite(s"s3a://$testBucketName/committed", rows = 10000, files = 4)()
  }

  test("a native write is committed with the magic committer tracking commits in memory") {
    assume(
      Try(
        Class.forName(
          "org.apache.hadoop.fs.s3a.commit.magic.InMemoryMagicCommitTracker")).isSuccess,
      "Requires S3A's in-memory commit tracking, which Hadoop 3.3.4 does not have")
    // Task commit then reads the pending uploads from S3A's in-process tracker, not from the
    // `.pending` files: a native writer that only reproduced those files would lose its data
    // here. Its own bucket, since S3A reads the setting from the file system instance it caches
    // per bucket.
    val bucket = "magic-in-memory"
    createBucketIfNotExists(bucket)
    checkMagicCommittedWrite(s"s3a://$bucket/committed", rows = 1000, files = 2)(
      "fs.s3a.committer.magic.track.commits.in.memory.enabled" -> "true")
  }

  // Spark 4.1 configures both bindings at once. Either one alone asks the committer factory for
  // the magic committer: PathOutputCommitProtocol ignores the Parquet committer class, and Spark's
  // default protocol instantiates BindingParquetOutputCommitter, which hands out the magic
  // committer's paths.
  Seq(
    "PathOutputCommitProtocol" -> (SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key ->
      classOf[ParquetOutputCommitter].getName),
    "BindingParquetOutputCommitter" -> (SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
      SQLConf.FILE_COMMIT_PROTOCOL_CLASS.defaultValueString)).foreach {
    case (binding, otherBindingUnset) =>
      test(s"a native write is committed by the magic committer through $binding alone") {
        checkMagicCommittedWrite(s"s3a://$testBucketName/$binding", rows = 1000, files = 2)(
          otherBindingUnset)
      }
  }

  test("a failed native write leaves no pending upload behind") {
    // S3A starts the multipart upload of a magic file when the file is created, so a failed write
    // has to abort the file for the upload not to stay pending, and billed. Job abort otherwise
    // cleans up only with fs.s3a.committer.abort.pending.uploads, which deployments writing one
    // destination from concurrent jobs turn off, so it is off here: only the writer's own abort
    // can leave the bucket clean. One task, because job abort does not wait for tasks still
    // running, so with several what is left would depend on how far the others got.
    withTempPath { dir =>
      val divisors = materializeAsCometSource(
        (1 to 1000).map(i => (i, i - 500)).toDF("id", "divisor"),
        new File(dir, "source.parquet").getAbsolutePath)
      val table = s"s3a://$testBucketName/failed"
      withNativeWriter {
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          "fs.s3a.committer.abort.pending.uploads" -> "false") {
          // Built under ANSI mode, which Spark 3.x division captures when it is created.
          val df = divisors.select((col("id") / col("divisor")).as("quotient")).coalesce(1)
          val (plan, error) = captureFailedWritePlan(df.write.parquet(table))
          assertHasCometNativeWriteExec(plan)
          assert(
            causeChain(error).exists(_.getMessage.contains("DIVIDE_BY_ZERO")),
            s"Expected the division by zero to fail the write, got: $error")
        }
      }
      val tablePath = new Path(table)
      val fs = tablePath.getFileSystem(spark.sessionState.newHadoopConf())
      if (fs.exists(tablePath)) {
        val iterator = fs.listFiles(tablePath, true)
        val files =
          Iterator.continually(iterator).takeWhile(_.hasNext).map(_.next().getPath).toList
        assert(files.isEmpty, s"A failed write left ${files.mkString(", ")}")
      }
      assert(pendingUploads(testBucketName).isEmpty, pendingUploads(testBucketName))
    }
  }

  test("an S3 write that S3A's magic committer does not commit stays on Spark") {
    // Only the magic committer's writes have S3A create each file. The others need the native
    // writer to do what S3A does at create time, which it does not yet.
    Seq("file", "directory").foreach { committer =>
      withTempPath { dir =>
        val df = source(100, dir)
        val table = s"s3a://$testBucketName/committer-$committer"
        withNativeWriter {
          withSQLConf("fs.s3a.committer.name" -> committer) {
            val plan = captureWritePlan(p => df.write.parquet(p), table)
            assertNoCometNativeWriteExec(plan)
          }
        }
        checkAnswer(spark.read.parquet(table), df)
      }
    }
  }
}
