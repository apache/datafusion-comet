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
import java.net.URI
import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetOutputCommitter
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.comet.CometConf
import org.apache.comet.CometS3TestBase
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.serde.operator.NativeWriteUtils

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest

/**
 * Native Parquet writes to S3 that S3A's magic committer commits, against MinIO.
 *
 * A manual suite like the other MinIO suites, since it needs Docker. Every session here gets the
 * magic committer the way Spark configures it when spark-hadoop-cloud is on the classpath: by
 * default on Spark 4.1+ (SPARK-47618), and for a bucket that asks for it on Spark 4.0. The shared
 * test configuration turns that off for every other suite.
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

  private def s3Client(): S3Client =
    S3Client
      .builder()
      .endpointOverride(URI.create(minioContainer.getS3URL))
      .region(Region.US_EAST_1)
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(userName, password)))
      .forcePathStyle(true)
      .build()

  /** The multipart uploads still pending in `bucket`, as their object keys. */
  private def pendingUploads(bucket: String): Seq[String] = {
    val client = s3Client()
    try {
      client
        .listMultipartUploads(ListMultipartUploadsRequest.builder().bucket(bucket).build())
        .uploads()
        .asScala
        .map(_.key())
        .toSeq
    } finally {
      client.close()
    }
  }

  /** `df` persisted locally with Comet off and read back, so its plan is a native scan. */
  private def nativeSource(df: DataFrame, dir: File): DataFrame = {
    val sourcePath = new File(dir, "source.parquet").getAbsolutePath
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      df.write.parquet(sourcePath)
    }
    spark.read.parquet(sourcePath)
  }

  private def writeStat(plan: SparkPlan, name: String): Long =
    plan
      .collectFirst { case write: DataWritingCommandExec => write.cmd.metrics(name).value }
      .getOrElse(fail(s"No write command in the plan:\n${plan.treeString}"))

  /**
   * Checks that the magic committer committed `files` part files to `table`: its `_SUCCESS`
   * manifest names it and them, no `__magic` directory or pending upload is left behind, and the
   * write's statistics saw each file's real size, which S3A reports for a magic file only through
   * the marker it writes when the file is closed.
   */
  private def assertCommittedByMagic(table: String, plan: SparkPlan, files: Int): Unit = {
    val tablePath = new Path(table)
    val fs = tablePath.getFileSystem(spark.sessionState.newHadoopConf())
    val manifest = Using.resource(fs.open(new Path(tablePath, "_SUCCESS"))) { in =>
      new ObjectMapper().readTree(IOUtils.toString(in, StandardCharsets.UTF_8))
    }
    assert(manifest.get("committer").asText() == "magic", manifest.toPrettyString)

    val parts = fs.listStatus(tablePath).filter(_.getPath.getName.startsWith("part-"))
    assert(parts.length == files, parts.map(_.getPath).mkString(", "))
    assert(manifest.get("filenames").size() == files, manifest.toPrettyString)
    assert(
      fs.listStatus(tablePath).forall(!_.getPath.getName.startsWith("__magic")),
      fs.listStatus(tablePath).map(_.getPath).mkString(", "))
    assert(pendingUploads(tablePath.toUri.getHost).isEmpty)

    assert(writeStat(plan, "numFiles") == files)
    assert(writeStat(plan, "numOutputBytes") == parts.map(_.getLen).sum)
  }

  test("a native write is committed by S3A's magic committer") {
    assume(isSpark40Plus, "Requires the WriteFilesExec seam")
    // Spark's own magic committer configuration, rather than one this suite spells out.
    assert(
      spark.conf.get(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key) ==
        NativeWriteUtils.PathOutputCommitProtocolClass)
    assert(
      spark.conf.get(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key) ==
        NativeWriteUtils.BindingParquetOutputCommitterClass)
    withTempPath { dir =>
      val df = nativeSource((1 to 10000).map(i => (i, s"str_$i")).toDF("id", "name"), dir)
        .repartition(4)
      val table = s"s3a://$testBucketName/committed"
      withNativeWriter {
        val plan = captureWritePlan(p => df.write.parquet(p), table)
        assertHasCometNativeWriteExec(plan)
        assertCommittedByMagic(table, plan, files = 4)
      }
      checkAnswer(spark.read.parquet(table), df)
    }
  }

  test("a native write is committed with the magic committer tracking commits in memory") {
    assume(isSpark40Plus, "Requires the WriteFilesExec seam")
    // Task commit then reads the pending uploads from S3A's in-process tracker, not from the
    // `.pending` files: a native writer that only reproduced those files would lose its data
    // here. Its own bucket, since S3A reads the setting from the file system instance it caches
    // per bucket.
    val bucket = "magic-in-memory"
    createBucketIfNotExists(bucket)
    withTempPath { dir =>
      val df = nativeSource((1 to 1000).map(i => (i, s"str_$i")).toDF("id", "name"), dir)
        .repartition(2)
      val table = s"s3a://$bucket/committed"
      withNativeWriter {
        withSQLConf("fs.s3a.committer.magic.track.commits.in.memory.enabled" -> "true") {
          val plan = captureWritePlan(p => df.write.parquet(p), table)
          assertHasCometNativeWriteExec(plan)
          assertCommittedByMagic(table, plan, files = 2)
        }
      }
      checkAnswer(spark.read.parquet(table), df)
    }
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
        assume(isSpark40Plus, "Requires the WriteFilesExec seam")
        withTempPath { dir =>
          val df = nativeSource((1 to 1000).map(i => (i, s"str_$i")).toDF("id", "name"), dir)
            .repartition(2)
          val table = s"s3a://$testBucketName/$binding"
          withNativeWriter {
            withSQLConf(otherBindingUnset) {
              val plan = captureWritePlan(p => df.write.parquet(p), table)
              assertHasCometNativeWriteExec(plan)
              assertCommittedByMagic(table, plan, files = 2)
            }
          }
          checkAnswer(spark.read.parquet(table), df)
        }
      }
  }

  test("a failed native write leaves no pending upload behind") {
    assume(isSpark40Plus, "Requires the WriteFilesExec seam")
    // S3A starts the multipart upload of a magic file when the file is created, so a failed write
    // has to abort the file for the upload not to stay pending, and billed. Job abort otherwise
    // cleans up only with fs.s3a.committer.abort.pending.uploads, which deployments writing one
    // destination from concurrent jobs turn off, so it is off here: only the writer's own abort
    // can leave the bucket clean. One task, because job abort does not wait for tasks still
    // running, so with several what is left would depend on how far the others got.
    withTempPath { dir =>
      val df = nativeSource((1 to 1000).map(i => (i, i - 500)).toDF("id", "divisor"), dir)
        .select((col("id") / col("divisor")).as("quotient"))
        .coalesce(1)
      val table = s"s3a://$testBucketName/failed"
      withNativeWriter {
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          "fs.s3a.committer.abort.pending.uploads" -> "false") {
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
    assume(isSpark40Plus, "Requires the WriteFilesExec seam")
    // Only the magic committer's writes have S3A create each file. The others need the native
    // writer to do what S3A does at create time, which it does not yet.
    Seq("file", "directory").foreach { committer =>
      withTempPath { dir =>
        val df = nativeSource((1 to 100).map(i => (i, s"str_$i")).toDF("id", "name"), dir)
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
