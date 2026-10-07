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
import java.nio.file.Files
import java.util.Base64

import org.apache.parquet.crypto.DecryptionPropertiesFactory
import org.apache.parquet.crypto.keytools.{KeyToolkit, PropertiesDrivenCryptoFactory}
import org.apache.parquet.crypto.keytools.mocks.InMemoryKMS
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.functions.{col, expr, max, sum}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometS3TestBase}
import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus

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

  private def assertCometScan(df: DataFrame): Unit =
    assert(cometScans(df.queryExecution.executedPlan).size == 1)

  // Both schemes address the same MinIO. `blob` is the opt-in S3-compliant alias: the native scan
  // reads it through object_store after rewriting blob:// -> s3://, with path-style defaulted on by
  // the blob endpoint (MinIO requires it). assertCometScan confirms the alias is claimed natively.
  private val readSchemes = Seq("s3a", "blob")

  readSchemes.foreach { scheme =>
    test(s"read parquet file from MinIO over $scheme://") {
      val testFilePath = s"$scheme://$testBucketName/data/$scheme-test-file.parquet"
      writeTestParquetFile(testFilePath)

      val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")))
      assertCometScan(df)
      assert(df.first().getLong(0) == 499500)
    }

    test(s"write and read encrypted parquet from S3 over $scheme://") {
      import testImplicits._

      // Encryption cache-key agreement end to end. The put side caches the key retriever under the
      // user-facing URI; the native side calls back with the rewritten s3:// URI. Both must
      // canonicalize to the same key (CometFileKeyUnwrapper.normalizeS3Scheme) or the read fails.
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

        val testFilePath = s"$scheme://$testBucketName/data/encrypted-$scheme-test.parquet"
        inputDF.write
          .option(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, "key1: a, b; key2: c")
          .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
          .parquet(testFilePath)

        val df = spark.read.parquet(testFilePath).agg(sum(col("a")))
        assertCometScan(df)
        assert(df.first().getLong(0) == 499500)
      }
    }
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

  test("mixed-bucket blob:// scan falls back and returns correct results") {
    // Alias settings under the hostless `default` authority resolve for one bucket only, so
    // CometScanRule declines a blob scan spanning two buckets and Spark reads both. Same key in
    // each bucket makes a misread visible.
    createBucketIfNotExists(secondBucketName)
    val key = "multibucket/same-key.parquet"
    val firstPath = s"blob://$testBucketName/$key"
    val secondPath = s"blob://$secondBucketName/$key"
    spark.range(111, 112).toDF("id").write.mode(SaveMode.Overwrite).parquet(firstPath)
    spark.range(222, 223).toDF("id").write.mode(SaveMode.Overwrite).parquet(secondPath)

    val df = spark.read.parquet(firstPath, secondPath)
    assert(
      cometScans(df.queryExecution.executedPlan).isEmpty,
      "mixed-bucket alias scan must fall back to Spark, but Comet claimed it:\n" +
        df.queryExecution.executedPlan)
    assert(
      df.collect().map(_.getLong(0)).toSet == Set(111L, 222L),
      "both buckets must be read; a single registered object store would read one bucket twice")
  }

  // Spark packs every file of the scan into one partition.
  private val onePartition = Seq(
    SQLConf.FILES_MIN_PARTITION_NUM.key -> "1",
    SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1",
    SQLConf.FILES_MAX_PARTITION_BYTES.key -> (128L * 1024 * 1024).toString)

  // Spark gives every file its own partition.
  private val separatePartitions = Seq(
    SQLConf.FILES_MIN_PARTITION_NUM.key -> "1",
    SQLConf.FILES_OPEN_COST_IN_BYTES.key -> (128L * 1024 * 1024).toString,
    SQLConf.FILES_MAX_PARTITION_BYTES.key -> (128L * 1024 * 1024).toString)

  private def bucketOf(path: String): String = new URI(path).getAuthority

  /** A one-file Parquet table of ids `from` to `to` tagged with `tag`, as bytes. */
  private def parquetBytes(from: Int, to: Int, tag: String): Array[Byte] = {
    var bytes: Array[Byte] = null
    withTempDir { dir =>
      val target = new File(dir, "out").getAbsolutePath
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark
          .range(from.toLong, to.toLong + 1)
          .selectExpr("cast(id as int) as id", s"'$tag' as bucket")
          .coalesce(1)
          .write
          .parquet(target)
      }
      val file = new File(target).listFiles().filter(_.getName.endsWith(".parquet")).head
      bytes = Files.readAllBytes(file.toPath)
    }
    bytes
  }

  /** Two files under `prefix` in each bucket, keyed `prefix/<name>` per entry of `names`. */
  private def putTwoBucketTable(prefixA: String, prefixB: String, names: Seq[String]): Unit = {
    createBucketIfNotExists(secondBucketName)
    names.zipWithIndex.foreach { case (name, i) =>
      putObject(testBucketName, s"$prefixA/$name", parquetBytes(i * 3 + 1, i * 3 + 3, "A"))
      putObject(secondBucketName, s"$prefixB/$name", parquetBytes(i * 3 + 1, i * 3 + 3, "B"))
    }
  }

  /**
   * Reads `read` with Comet off and on, and checks that Comet returns Spark's rows through a
   * native scan whose partitions each read from one bucket. `sparkMixesBuckets` states whether
   * Spark's own layout puts both buckets in one partition, so the case really exercises it.
   */
  private def assertMultiBucketRead(read: () => DataFrame, sparkMixesBuckets: Boolean): Unit = {
    var expected: Seq[Row] = Nil
    var sparkLayout: Seq[Set[String]] = Nil
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val df = read()
      expected = df.collect().toSeq
      val scans = collect(df.queryExecution.executedPlan) { case scan: FileSourceScanExec =>
        scan
      }
      sparkLayout = scans.flatMap(_.inputRDD.partitions.toSeq.collect { case p: FilePartition =>
        p.files.map(file => bucketOf(file.filePath.toString)).toSet
      })
    }
    assert(
      sparkLayout.exists(_.size > 1) == sparkMixesBuckets,
      s"Spark's partitions read buckets $sparkLayout")

    val df = read()
    val rows = df.collect().toSeq
    val plan = df.queryExecution.executedPlan
    val scans = collect(plan) { case scan: CometNativeScanExec => scan }
    val cometLayouts = scans.map(_.perPartitionFilePaths.map(_.map(bucketOf).toSet).toSeq)
    val bucketsPerScan = cometLayouts.map(_.flatten.toSet)
    assert(bucketsPerScan.exists(_.size > 1), s"expected a native scan over both buckets:\n$plan")
    assert(
      cometLayouts.forall(_.forall(_.size == 1)),
      s"Comet's partitions read buckets $cometLayouts")
    assert(rows.map(_.toString).sorted == expected.map(_.toString).sorted)
  }

  test("native scan over two buckets holding the same keys reads each file from its bucket") {
    putTwoBucketTable("mb-same", "mb-same", Seq("f1.parquet", "f2.parquet"))
    withSQLConf(onePartition: _*) {
      assertMultiBucketRead(
        () =>
          spark.read
            .parquet(s"s3a://$testBucketName/mb-same", s"s3a://$secondBucketName/mb-same"),
        sparkMixesBuckets = true)
    }
  }

  test("native scan over two buckets with distinct keys reads each file from its bucket") {
    putTwoBucketTable("mb-a", "mb-b", Seq("f1.parquet", "f2.parquet"))
    withSQLConf(onePartition: _*) {
      assertMultiBucketRead(
        () => spark.read.parquet(s"s3a://$testBucketName/mb-a", s"s3a://$secondBucketName/mb-b"),
        sparkMixesBuckets = true)
    }
  }

  test("native scan over two buckets with distinct keys, without adaptive execution") {
    putTwoBucketTable("mb-a", "mb-b", Seq("f1.parquet", "f2.parquet"))
    withSQLConf(onePartition :+ (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"): _*) {
      assertMultiBucketRead(
        () => spark.read.parquet(s"s3a://$testBucketName/mb-a", s"s3a://$secondBucketName/mb-b"),
        sparkMixesBuckets = true)
    }
  }

  test("native scan over two buckets in separate partitions reads each file from its bucket") {
    putTwoBucketTable("mb-a", "mb-b", Seq("f1.parquet", "f2.parquet"))
    withSQLConf(separatePartitions: _*) {
      assertMultiBucketRead(
        () => spark.read.parquet(s"s3a://$testBucketName/mb-a", s"s3a://$secondBucketName/mb-b"),
        sparkMixesBuckets = false)
    }
  }

  test("dynamic partition pruning over a table whose partitions span two buckets") {
    // AQE dynamic partition pruning falls back to Spark's scan before Spark 3.5.
    assume(isSpark35Plus)
    createBucketIfNotExists(secondBucketName)
    withTable("mb_fact", "mb_dim") {
      sql(s"""CREATE TABLE mb_fact (id INT, p STRING) USING parquet PARTITIONED BY (p)
             |LOCATION 's3a://$testBucketName/mb_fact'""".stripMargin)
      sql("INSERT INTO mb_fact PARTITION (p = 'a') SELECT CAST(id AS INT) FROM range(0, 10)")
      sql("INSERT INTO mb_fact PARTITION (p = 'c') SELECT CAST(id AS INT) FROM range(20, 30)")
      // Partition b lives in the second bucket, attached by location.
      val partitionB = s"s3a://$secondBucketName/mb_fact_b"
      spark
        .range(10, 20)
        .selectExpr("cast(id as int) as id")
        .write
        .mode(SaveMode.Overwrite)
        .parquet(partitionB)
      sql("ALTER TABLE mb_fact ADD PARTITION (p = 'b')")
      sql(s"ALTER TABLE mb_fact PARTITION (p = 'b') SET LOCATION '$partitionB'")
      sql(s"""CREATE TABLE mb_dim (p STRING, kind STRING) USING parquet
             |LOCATION 's3a://$testBucketName/mb_dim'""".stripMargin)
      sql("INSERT INTO mb_dim VALUES ('a', 'keep'), ('b', 'keep'), ('c', 'drop')")

      val query =
        "SELECT f.id, f.p FROM mb_fact f JOIN mb_dim d ON f.p = d.p WHERE d.kind = 'keep'"
      withSQLConf(onePartition: _*) {
        assertMultiBucketRead(() => sql(query), sparkMixesBuckets = true)
        val scans = collect(sql(query).queryExecution.executedPlan) {
          case scan: CometNativeScanExec => scan
        }
        assert(
          scans.exists(_.partitionFilters.exists(_.isInstanceOf[DynamicPruningExpression])),
          s"expected a dynamically pruned native scan: $scans")
      }
    }
  }

  test("bucketed table whose partitions span two buckets falls back to Spark") {
    // A bucketed scan keeps one partition per table bucket, so it cannot be grouped per store.
    // The partition filter makes the scan's root paths the partition locations. Partition b is
    // written by a twin table in the second bucket and attached by location.
    createBucketIfNotExists(secondBucketName)
    withTable("mb_bucketed", "mb_bucketed_b") {
      Seq(("mb_bucketed", testBucketName), ("mb_bucketed_b", secondBucketName)).foreach {
        case (table, bucket) =>
          sql(s"""CREATE TABLE $table (id INT, p STRING) USING parquet PARTITIONED BY (p)
                 |CLUSTERED BY (id) INTO 2 BUCKETS
                 |LOCATION 's3a://$bucket/$table'""".stripMargin)
      }
      sql("INSERT INTO mb_bucketed PARTITION (p = 'a') SELECT CAST(id AS INT) FROM range(10)")
      sql("INSERT INTO mb_bucketed_b PARTITION (p = 'b') SELECT CAST(id AS INT) FROM range(10)")
      sql("ALTER TABLE mb_bucketed ADD PARTITION (p = 'b')")
      sql(s"""ALTER TABLE mb_bucketed PARTITION (p = 'b')
             |SET LOCATION 's3a://$secondBucketName/mb_bucketed_b/p=b'""".stripMargin)

      withSQLConf(SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false") {
        val query = "SELECT * FROM mb_bucketed WHERE p IN ('a', 'b')"
        var expected: Seq[Row] = Nil
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = sql(query)
          expected = df.collect().toSeq
          val sparkScans = collect(df.queryExecution.executedPlan) {
            case scan: FileSourceScanExec => scan
          }
          assert(sparkScans.exists(_.bucketedScan), s"expected a bucketed scan: $sparkScans")
        }
        val df = sql(query)
        val rows = df.collect().toSeq
        assert(
          cometScans(df.queryExecution.executedPlan).isEmpty,
          s"a bucketed scan over two buckets must fall back:\n${df.queryExecution.executedPlan}")
        assert(rows.map(_.toString).sorted == expected.map(_.toString).sorted)
        checkSparkAnswerAndFallbackReason(
          sql(query),
          "Native Parquet scan of a bucketed table reads paths in object stores " +
            s"s3://$testBucketName, s3://$secondBucketName, but reads each table bucket " +
            "through one store")
        // A bucketed scan of one bucket stays native.
        checkSparkAnswerAndOperator(sql("SELECT * FROM mb_bucketed WHERE p = 'a'"))
      }
    }
  }
}
