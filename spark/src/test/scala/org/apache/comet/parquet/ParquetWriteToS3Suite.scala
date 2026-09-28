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

import java.net.URI

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import org.apache.comet.{CometConf, CometS3TestBase}
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.cloud.s3.{CometS3AccessMode, MinioCometS3CredentialProvider}

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.HeadObjectRequest

/**
 * Native Parquet writes to S3, against MinIO. Which S3 destinations the native writer accepts is
 * covered without an S3 endpoint in `CometParquetWriterSuite`; this suite checks that the ones it
 * accepts are written where Spark's commit protocol expects them.
 *
 * A manual suite, like the other MinIO suites: it needs Docker, so CI does not run it (see
 * `dev/ci/check-suites.py`).
 */
class ParquetWriteToS3Suite extends CometParquetWriterTestBase with CometS3TestBase {

  import testImplicits._

  override protected val testBucketName = "native-write-bucket"

  /** A bucket whose native access goes through [[MinioCometS3CredentialProvider]]. */
  private val credentialBucket = "native-write-credential-bucket"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(
      s"spark.hadoop.fs.s3a.bucket.$credentialBucket.comet.credential.provider.class",
      classOf[MinioCometS3CredentialProvider].getName)
    conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    MinioCometS3CredentialProvider.installCredentials(userName, password)
    createBucketIfNotExists(credentialBucket)
  }

  private def s3Path(key: String, bucket: String = testBucketName): String =
    s"s3a://$bucket/$key"

  /**
   * Persist `df` to S3 with Spark's writer and return a DataFrame that reads it back, so that the
   * write under test has the Comet scan below it that native writes require.
   */
  private def cometSource(df: DataFrame, name: String): DataFrame = {
    val path = s3Path(s"sources/$name")
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      df.write.mode(SaveMode.Overwrite).parquet(path)
    }
    spark.read.parquet(path)
  }

  private def someRows(name: String): DataFrame =
    cometSource((1 to 1000).map(i => (i, s"name_$i")).toDF("id", "name").repartition(4), name)

  private def dataFiles(dir: String): Seq[FileStatus] = {
    val path = new Path(dir)
    path
      .getFileSystem(spark.sessionState.newHadoopConf())
      .listStatus(path)
      .filter(_.getPath.getName.startsWith("part-"))
      .toSeq
  }

  /**
   * Check `path` holds exactly `expected`, read with Spark's own reader so that a Comet reader
   * bug cannot hide a writer bug. Spark only finds files its committer moved into place, so this
   * also checks that the native writer created them where the commit protocol expected.
   */
  private def checkWrittenData(path: String, expected: DataFrame): Unit = {
    val rows = expected.collect().toSeq
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      checkAnswer(spark.read.parquet(path), rows)
    }
  }

  private def objectETag(bucket: String, key: String): String = {
    val s3 = S3Client
      .builder()
      .endpointOverride(URI.create(minioContainer.getS3URL))
      .credentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(userName, password)))
      .forcePathStyle(true)
      .region(Region.US_EAST_1)
      .build()
    try {
      s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()).eTag()
    } finally {
      s3.close()
    }
  }

  test("native write to s3a:// is committed where Spark reads it") {
    val df = someRows("round-trip")
    val out = s3Path("out/round-trip")
    withNativeWriter {
      assertHasCometNativeWriteExec(
        captureWritePlan(p => df.write.mode(SaveMode.Overwrite).parquet(p), out))
    }

    assert(dataFiles(out).nonEmpty, s"No data files under $out")
    checkWrittenData(out, df)
    if (isSpark40Plus) {
      // On Spark 4.0+ Spark's own commit protocol commits the job.
      val success = new Path(out, "_SUCCESS")
      assert(success.getFileSystem(spark.sessionState.newHadoopConf()).exists(success))
    }
  }

  test("a file larger than the upload buffer is written as a multipart upload") {
    // Uncompressed and in one file, three long columns of a million rows come to over 20 MiB,
    // twice the 10 MiB the native writer buffers before it switches from one PUT to a multipart
    // upload.
    val df = cometSource(
      spark.range(0, 1000000).selectExpr("id", "id * 3 AS a", "id * 7 AS b").coalesce(1),
      "multipart")
    val out = s3Path("out/multipart")
    withNativeWriter {
      assertHasCometNativeWriteExec(
        captureWritePlan(
          p => df.write.mode(SaveMode.Overwrite).option("compression", "none").parquet(p),
          out))
    }

    val files = dataFiles(out)
    assert(files.size == 1, s"Expected one data file, found ${files.map(_.getPath)}")
    assert(files.head.getLen > 10L * 1024 * 1024, s"File too small: ${files.head.getLen} bytes")
    // S3 gives an object uploaded in parts an ETag that ends in "-<number of parts>".
    val key = files.head.getPath.toUri.getPath.stripPrefix("/")
    val eTag = objectETag(testBucketName, key)
    assert(eTag.contains("-"), s"Expected a multipart ETag for $key, got $eTag")
    // Sums rather than a million rows: every value still has to come back.
    val idSum = 999999L * 1000000L / 2
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      checkAnswer(
        spark.read.parquet(out).selectExpr("count(*)", "sum(id)", "sum(a)", "sum(b)"),
        Row(1000000L, idSum, 3 * idSum, 7 * idSum))
    }
  }

  test("SaveMode.Append and SaveMode.Overwrite on S3") {
    val first = cometSource((1 to 100).map(i => (i, s"first_$i")).toDF("id", "name"), "first")
    val second =
      cometSource((101 to 150).map(i => (i, s"second_$i")).toDF("id", "name"), "second")
    val out = s3Path("out/save-modes")
    withNativeWriter {
      assertHasCometNativeWriteExec(
        captureWritePlan(p => first.write.mode(SaveMode.Overwrite).parquet(p), out))
      assertHasCometNativeWriteExec(
        captureWritePlan(p => second.write.mode(SaveMode.Append).parquet(p), out))
    }
    checkWrittenData(out, first.union(second))

    withNativeWriter {
      assertHasCometNativeWriteExec(
        captureWritePlan(p => second.write.mode(SaveMode.Overwrite).parquet(p), out))
    }
    checkWrittenData(out, second)
  }

  test("a directory name with a space and non-ASCII characters stays native") {
    // Both survive the native writer's URL round trip on S3, unlike on HDFS. Were the key it
    // derives different, Spark's committer would not find the files and the read would be empty.
    // Built from code points because scalastyle forbids non-ASCII source characters.
    val eAcute = new String(Character.toChars(0x00e9))
    val df = someRows("escaped-names")
    val out = s3Path(s"out/dir with space/caf$eAcute")
    withNativeWriter {
      assertHasCometNativeWriteExec(
        captureWritePlan(p => df.write.mode(SaveMode.Overwrite).parquet(p), out))
    }
    checkWrittenData(out, df)
  }

  test("a path the native writer cannot reproduce falls back to Spark") {
    // The native writer would decode `%25` into `%` and write under a different key.
    val df = someRows("percent")
    val out = s3Path("out/50%25off")
    withNativeWriter {
      assertNoCometNativeWriteExec(
        captureWritePlan(p => df.write.mode(SaveMode.Overwrite).parquet(p), out))
    }
    checkWrittenData(out, df)
  }

  test("a write that names the S3A magic committer falls back to Spark") {
    // Spark's default commit protocol still commits this write with a FileOutputCommitter, so
    // Spark's writer succeeds here. Comet declines as soon as the magic committer is named.
    val df = someRows("magic")
    val out = s3Path("out/magic")
    withNativeWriter {
      assertNoCometNativeWriteExec(
        captureWritePlan(
          p =>
            df.write
              .mode(SaveMode.Overwrite)
              .option("fs.s3a.committer.name", "magic")
              .parquet(p),
          out))
    }
    checkWrittenData(out, df)
  }

  test("a native write asks the bucket's CometS3CredentialProvider for write access") {
    // The source lives in the default bucket, which the native scan reads with the static keys, so
    // every request that reaches the provider comes from the write.
    val df = someRows("credentials")
    val out = s3Path("out/credentials", credentialBucket)
    MinioCometS3CredentialProvider.resetCounters()
    withNativeWriter {
      assertHasCometNativeWriteExec(
        captureWritePlan(p => df.write.mode(SaveMode.Overwrite).parquet(p), out))
    }

    // Collections.singleton, not Set.of: Scala 2.12 cannot choose between Set.of(E) and
    // Set.of(E...) for a single argument.
    assert(
      MinioCometS3CredentialProvider.accessModes() ==
        java.util.Collections.singleton(CometS3AccessMode.WRITE),
      s"Unexpected access modes: ${MinioCometS3CredentialProvider.accessModes()}")
    assert(MinioCometS3CredentialProvider.lastBucket() == credentialBucket)
    checkWrittenData(out, df)
  }
}
