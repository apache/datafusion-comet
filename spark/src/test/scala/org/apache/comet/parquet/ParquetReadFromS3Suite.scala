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

import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.parquet.crypto.DecryptionPropertiesFactory
import org.apache.parquet.crypto.keytools.{KeyToolkit, PropertiesDrivenCryptoFactory}
import org.apache.parquet.crypto.keytools.mocks.InMemoryKMS
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.comet.{CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, expr, max, sum}

import org.apache.comet.CometS3TestBase

class ParquetReadFromS3Suite extends CometS3TestBase with AdaptiveSparkPlanHelper {

  override protected val testBucketName = "test-bucket"
  // Second bucket for the mixed-bucket regression below. BlobSchemeFileSystem is an S3AFileSystem
  // reading the global fs.s3a.* surface, so this bucket needs no extra per-bucket config.
  private val secondBucketName = "test-bucket-2"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    // Opt into the `blob` alias (shared setup in CometS3TestBase). The blob:// tests below exercise
    // the alias->s3 rewrite, path-style defaulting, and native claiming.
    applyBlobSchemeProps(conf, testBucketName)
    conf
  }

  // Encryption keys for testing parquet encryption
  private val encoder = Base64.getEncoder
  private val footerKey =
    encoder.encodeToString("0123456789012345".getBytes(StandardCharsets.UTF_8))
  private val key1 = encoder.encodeToString("1234567890123450".getBytes(StandardCharsets.UTF_8))
  private val key2 = encoder.encodeToString("1234567890123451".getBytes(StandardCharsets.UTF_8))
  private val cryptoFactoryClass =
    "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"

  private def writeTestParquetFile(filePath: String): Unit = {
    val df = spark.range(0, 1000)
    df.write.format("parquet").mode(SaveMode.Overwrite).save(filePath)
  }

  private def writePartitionedParquetFile(filePath: String): Unit = {
    val df = spark.range(0, 1000).withColumn("val", expr("concat('val#', id % 10)"))
    df.write.format("parquet").partitionBy("val").mode(SaveMode.Overwrite).save(filePath)
  }

  private def assertCometScan(df: DataFrame): Unit = {
    val scans = collect(df.queryExecution.executedPlan) {
      case p: CometScanExec => p
      case p: CometNativeScanExec => p
    }
    assert(scans.size == 1)
  }

  test("read parquet file from MinIO") {
    val testFilePath = s"s3a://$testBucketName/data/test-file.parquet"
    writeTestParquetFile(testFilePath)

    val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")))
    assertCometScan(df)
    assert(df.first().getLong(0) == 499500)
  }

  test("read partitioned parquet file from MinIO") {
    val testFilePath = s"s3a://$testBucketName/data/test-partitioned-file.parquet"
    writePartitionedParquetFile(testFilePath)

    val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")), max(col("val")))
    val firstRow = df.first()
    assert(firstRow.getLong(0) == 499500)
    assert(firstRow.getString(1) == "val#9")
  }

  test("read parquet file from MinIO with URL escape sequences in path") {
    // Path with '%23' and '%20' which are URL escape sequences for '#' and ' '
    val testFilePath = s"s3a://$testBucketName/data/Brand%2321/test%20file.parquet"
    writeTestParquetFile(testFilePath)

    val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")))
    assertCometScan(df)
    assert(df.first().getLong(0) == 499500)
  }

  test("write and read encrypted parquet from S3") {
    import testImplicits._

    withSQLConf(
      DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME -> cryptoFactoryClass,
      KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME ->
        "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
      InMemoryKMS.KEY_LIST_PROPERTY_NAME ->
        s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

      val inputDF = spark
        .range(0, 1000)
        .map(i => (i, i.toString, i.toFloat))
        .repartition(5)
        .toDF("a", "b", "c")

      val testFilePath = s"s3a://$testBucketName/data/encrypted-test.parquet"
      inputDF.write
        .option(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, "key1: a, b; key2: c")
        .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
        .parquet(testFilePath)

      val df = spark.read.parquet(testFilePath).agg(sum(col("a")))
      assertCometScan(df)
      assert(df.first().getLong(0) == 499500)
    }
  }

  test("read parquet file from MinIO over blob://") {
    // Plain read over the opt-in blob:// alias: the native scan reads through object_store after
    // rewriting blob:// -> s3://. path-style (defaulted on by the blob endpoint) is required for
    // MinIO. assertCometScan confirms the alias was claimed natively.
    val testFilePath = s"blob://$testBucketName/data/blob-test-file.parquet"
    writeTestParquetFile(testFilePath)

    val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")))
    assertCometScan(df)
    assert(df.first().getLong(0) == 499500)
  }

  test("write and read encrypted parquet from S3 over blob://") {
    import testImplicits._

    // Encryption cache-key agreement over blob paths, end to end. The put side caches the key
    // retriever under the blob:// URI. The native side calls back with the rewritten s3:// URI.
    // Both must canonicalize to the same key (CometFileKeyUnwrapper.normalizeS3Scheme) or it fails.
    withSQLConf(
      DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME -> cryptoFactoryClass,
      KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME ->
        "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
      InMemoryKMS.KEY_LIST_PROPERTY_NAME ->
        s"footerKey: ${footerKey}, key1: ${key1}, key2: ${key2}") {

      val inputDF = spark
        .range(0, 1000)
        .map(i => (i, i.toString, i.toFloat))
        .repartition(5)
        .toDF("a", "b", "c")

      val testFilePath = s"blob://$testBucketName/data/encrypted-blob-test.parquet"
      inputDF.write
        .option(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, "key1: a, b; key2: c")
        .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
        .parquet(testFilePath)

      val df = spark.read.parquet(testFilePath).agg(sum(col("a")))
      assertCometScan(df)
      assert(df.first().getLong(0) == 499500)
    }
  }

  test("mixed-bucket blob:// scan falls back and returns correct results") {
    // Native planning registers ONE object store per FilePartition (keyed on the first file's
    // bucket) and strips the authority from every file's object key, so a scan spanning two blob
    // buckets would read every file from the first bucket -- returning [111, 111] instead of
    // [111, 222]. CometScanRule must decline it so Spark reads both buckets correctly. Regression
    // for the P1 in sunchao's 2026-09-02 review. Same key in each bucket makes the misread visible.
    createBucketIfNotExists(secondBucketName)
    val key = "multibucket/same-key.parquet"
    val firstPath = s"blob://$testBucketName/$key"
    val secondPath = s"blob://$secondBucketName/$key"
    spark.range(111, 112).toDF("id").write.mode(SaveMode.Overwrite).parquet(firstPath)
    spark.range(222, 223).toDF("id").write.mode(SaveMode.Overwrite).parquet(secondPath)

    val df = spark.read.parquet(firstPath, secondPath)
    val nativeScans = collect(df.queryExecution.executedPlan) {
      case p: CometNativeScanExec => p
      case p: CometScanExec => p
    }
    assert(
      nativeScans.isEmpty,
      "mixed-bucket alias scan must fall back to Spark, but Comet claimed it:\n" +
        df.queryExecution.executedPlan)
    assert(
      df.collect().map(_.getLong(0)).toSet == Set(111L, 222L),
      "both buckets must be read; a single registered object store would read one bucket twice")
  }
}
