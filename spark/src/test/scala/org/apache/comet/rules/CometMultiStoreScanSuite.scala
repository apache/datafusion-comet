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

package org.apache.comet.rules

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.comet.{CometCsvNativeScanExec, CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.{ExtendedMode, FileSourceScanExec, FormattedMode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.CometConf.{COMET_LIBHDFS_SCHEMES_KEY, COMET_S3_COMPLIANT_SCHEMES_KEY}
import org.apache.comet.hadoop.fs.FakeHdfsAuthorityFileSystem
import org.apache.comet.objectstore.NativeConfig

/**
 * Native scans over files in more than one object store, without a cloud store: `hdfs://nn1` and
 * `hdfs://nn2` are two native stores backed by the local disk. Native execution cannot read them,
 * so claimed scans are checked on their plans, and declined scans are run by Spark.
 */
class CometMultiStoreScanSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private var rootDir: File = _

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.hadoop.fs.hdfs.impl", classOf[FakeHdfsAuthorityFileSystem].getName)
    conf.set("spark.hadoop.fs.hdfs.impl.disable.cache", "true")
    Seq("s3a", "blob").foreach { scheme =>
      conf.set(s"spark.hadoop.fs.$scheme.impl", classOf[FakeHdfsAuthorityFileSystem].getName)
      conf.set(s"spark.hadoop.fs.$scheme.impl.disable.cache", "true")
    }
    conf
  }

  override def beforeAll(): Unit = {
    rootDir = Files.createTempDirectory(s"comet_multi_store_${UUID.randomUUID()}").toFile
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    if (rootDir != null) FileUtils.deleteDirectory(rootDir)
    super.afterAll()
  }

  private val nativeScan = Seq(
    CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true")

  private val nativeCsv =
    Seq(CometConf.COMET_CSV_V2_NATIVE_ENABLED.key -> "true", SQLConf.USE_V1_SOURCE_LIST.key -> "")

  // Spark packs every file of the scan into one partition.
  private val onePartition = Seq(
    SQLConf.FILES_MIN_PARTITION_NUM.key -> "1",
    SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1",
    SQLConf.FILES_MAX_PARTITION_BYTES.key -> (128L * 1024 * 1024).toString)

  // Over the files `writeMixedLayout` writes (120, 15 and 10 bytes, with a 140 byte split), Spark
  // packs the first two into one partition and the third into a second one.
  private val twoPartitionsOneMixed = Seq(
    SQLConf.FILES_MIN_PARTITION_NUM.key -> "1",
    SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1",
    SQLConf.FILES_MAX_PARTITION_BYTES.key -> "140")

  private val idSchema = new StructType().add("id", IntegerType)

  private def hdfs(nameNode: String, name: String): String =
    s"hdfs://$nameNode${rootDir.getAbsolutePath}/$name"

  private def bucket(scheme: String, name: String): String =
    s"$scheme://bucket${rootDir.getAbsolutePath}/$name"

  private def local(name: String): String = s"file://${rootDir.getAbsolutePath}/$name"

  private def storeOf(path: String): String = new URI(path).getAuthority

  private def storeKey(path: String): String =
    NativeConfig.objectStoreKey(new URI(path), Set.empty, Set("hdfs")).key

  // A forwarded object store option that the plan's text must not show.
  private val forwardedMarker = COMET_S3_COMPLIANT_SCHEMES_KEY -> "zzforwardedmarker"

  private def assertPlanHidesForwardedOptions(df: DataFrame): Unit = {
    val texts = Seq(
      df.queryExecution.executedPlan.toString,
      df.queryExecution.explainString(ExtendedMode),
      df.queryExecution.explainString(FormattedMode))
    texts.foreach(text => assert(!text.contains(forwardedMarker._2), text))
  }

  private def withoutComet(f: => Unit): Unit =
    withSQLConf(CometConf.COMET_ENABLED.key -> "false")(f)

  private def writeIds(
      path: String,
      from: Int,
      format: String = "parquet",
      count: Int = 5): Unit =
    withoutComet {
      spark
        .range(from.toLong, from.toLong + count)
        .selectExpr("cast(id as int) as id")
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .format(format)
        .save(path)
    }

  /** CSV files of 120, 15 and 10 bytes at `first`, `second` and `third`. */
  private def writeMixedLayout(first: String, second: String, third: String): Unit = {
    writeIds(first, 100, "csv", count = 30)
    writeIds(second, 10, "csv")
    writeIds(third, 0, "csv")
  }

  /** The files of each partition of Spark's own scans in `df`, which is planned, not run. */
  private def sparkLayout(df: => DataFrame): Seq[Seq[String]] = {
    var layout: Seq[Seq[String]] = Nil
    withoutComet {
      val plan = df.queryExecution.executedPlan
      val partitions = collect(plan) {
        case scan: FileSourceScanExec => scan.inputRDD.partitions.toSeq
        case scan: BatchScanExec => scan.inputPartitions
      }.flatten
      layout = partitions.collect { case p: FilePartition =>
        p.files.map(_.filePath.toString).toSeq
      }
    }
    layout
  }

  /** The scans CometScanRule claims in the Spark plan of `df`. */
  private def ruleClaims(df: => DataFrame): Seq[CometScanExec] = {
    var sparkPlan: SparkPlan = null
    withoutComet {
      sparkPlan = df.queryExecution.executedPlan
    }
    CometScanRule(spark).apply(stripAQEPlan(sparkPlan)).collect { case s: CometScanExec => s }
  }

  private def nativeParquetScan(df: DataFrame): CometNativeScanExec = {
    val plan = df.queryExecution.executedPlan
    val scans = collect(plan) { case scan: CometNativeScanExec => scan }
    assert(scans.size == 1, s"expected one native Parquet scan:\n$plan")
    scans.head
  }

  test("parquet scan over two name nodes packs each name node's files on its own") {
    val (a, b) = (hdfs("nn1", "two-nn-a"), hdfs("nn2", "two-nn-b"))
    writeIds(a, 0)
    writeIds(b, 10)
    withSQLConf(nativeScan ++ onePartition: _*) {
      val sparkFiles = sparkLayout(spark.read.parquet(a, b))
      assert(sparkFiles.exists(_.map(storeOf).distinct.size > 1), s"Spark's: $sparkFiles")
      val scan = nativeParquetScan(spark.read.parquet(a, b))
      val cometFiles = scan.perPartitionFilePaths.toSeq
      assert(cometFiles.forall(_.map(storeOf).distinct.size == 1), s"Comet's: $cometFiles")
      assert(cometFiles.flatten.sorted == sparkFiles.flatten.sorted)
      assert(cometFiles.size != sparkFiles.size)
    }
  }

  test("parquet scan over one name node keeps Spark's layout") {
    val (a, b) = (hdfs("nn1", "one-nn-a"), hdfs("nn1", "one-nn-b"))
    writeIds(a, 0)
    writeIds(b, 10)
    withSQLConf(nativeScan ++ onePartition: _*) {
      val sparkFiles = sparkLayout(spark.read.parquet(a, b))
      val scan = nativeParquetScan(spark.read.parquet(a, b))
      assert(scan.perPartitionFilePaths.toSeq == sparkFiles)
    }
  }

  test("csv scan over two name nodes splits partitions that mix name nodes") {
    val (a, b, c) =
      (hdfs("nn1", "csv-two-nn-a"), hdfs("nn2", "csv-two-nn-b"), hdfs("nn1", "csv-two-nn-c"))
    writeMixedLayout(a, b, c)
    withSQLConf(nativeCsv ++ twoPartitionsOneMixed: _*) {
      val sparkFiles = sparkLayout(spark.read.schema(idSchema).csv(a, b, c))
      // More than one partition, so every Spark version plans the scan as unknown partitioning.
      assert(sparkFiles.size > 1, s"Spark's: $sparkFiles")
      assert(sparkFiles.exists(_.map(storeOf).distinct.size > 1), s"Spark's: $sparkFiles")
      val df = spark.read.schema(idSchema).csv(a, b, c)
      val plan = df.queryExecution.executedPlan
      val scans = collect(plan) { case scan: CometCsvNativeScanExec => scan }
      assert(scans.size == 1, s"expected one native CSV scan:\n$plan")
      val partitions = scans.head.nativeOp.getCsvScan.getFilePartitionsList.asScala.toSeq
        .map(_.getPartitionedFileList.asScala.map(_.getFilePath).toSeq)
      assert(partitions.forall(_.map(storeOf).distinct.size == 1), s"Comet's: $partitions")
      assert(partitions.size != sparkFiles.size)
      assert(scans.head.outputPartitioning.numPartitions == partitions.size)
    }
  }

  test("global aggregate over a csv scan Spark plans as one partition across name nodes") {
    // Spark 3.4 reports a one-partition V2 scan as SinglePartition and plans no exchange below
    // the final aggregate, so a split scan would run that aggregate on two partitions.
    val (a, b) = (hdfs("nn1", "csv-agg-a"), hdfs("nn2", "csv-agg-b"))
    writeIds(a, 0, "csv")
    writeIds(b, 10, "csv")
    def query: DataFrame = spark.read.schema(idSchema).csv(a, b).groupBy().count()
    withSQLConf(
      nativeCsv ++ onePartition :+ (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"): _*) {
      var sparkScan: BatchScanExec = null
      withoutComet {
        sparkScan = collect(query.queryExecution.executedPlan) { case s: BatchScanExec => s }.head
      }
      val sparkFiles = sparkScan.inputPartitions.collect { case p: FilePartition =>
        p.files.map(file => storeOf(file.filePath.toString)).toSeq
      }
      assert(sparkFiles.size == 1, s"Spark's: $sparkFiles")
      assert(sparkFiles.head.distinct.sorted == Seq("nn1", "nn2"), s"Spark's: $sparkFiles")

      val plan = query.queryExecution.executedPlan
      val scans = collect(plan) { case scan: CometCsvNativeScanExec => scan }
      if (sparkScan.outputPartitioning.isInstanceOf[UnknownPartitioning]) {
        // Spark planned an exchange above the scan, so splitting the scan is safe. The query is
        // not run because native cannot read the fake stores.
        assert(scans.size == 1, s"expected one native CSV scan:\n$plan")
        assert(scans.head.outputPartitioning.numPartitions == 2)
        val exchanges = collect(plan) { case e: ShuffleExchangeLike => e }
        assert(exchanges.nonEmpty, s"no exchange above the scan:\n$plan")
      } else {
        assert(scans.isEmpty, s"a split scan would break Spark's partitioning:\n$plan")
        checkSparkAnswerAndFallbackReason(
          query,
          "Native CSV scan would split a partition that mixes object stores, but Spark " +
            "planned the operators above it for the scan's own partitioning")
      }
    }
  }

  test("csv scan over local and hdfs files reads each store's files in their own partitions") {
    val (a, b, c) = (local("csv-mixed-a"), hdfs("nn2", "csv-mixed-b"), local("csv-mixed-c"))
    writeMixedLayout(a, b, c)
    withSQLConf(nativeCsv ++ twoPartitionsOneMixed :+ forwardedMarker: _*) {
      val sparkFiles = sparkLayout(spark.read.schema(idSchema).csv(a, b, c))
      assert(sparkFiles.exists(_.map(storeKey).distinct.size > 1), s"Spark's: $sparkFiles")
      val df = spark.read.schema(idSchema).csv(a, b, c)
      val plan = df.queryExecution.executedPlan
      val scans = collect(plan) { case scan: CometCsvNativeScanExec => scan }
      assert(scans.size == 1, s"expected one native CSV scan:\n$plan")
      val csvScan = scans.head.nativeOp.getCsvScan
      val partitions = csvScan.getFilePartitionsList.asScala.toSeq
        .map(_.getPartitionedFileList.asScala.map(_.getFilePath).toSeq)
      assert(partitions.forall(_.map(storeKey).distinct.size == 1), s"Comet's: $partitions")
      assert(partitions.flatten.map(storeKey).distinct.sorted == Seq("file://", "hdfs://nn2"))
      // Spark's mixed partition becomes two.
      assert(partitions.size == sparkFiles.size + 1, s"Comet's: $partitions")
      assert(scans.head.outputPartitioning.numPartitions == partitions.size)
      val options = csvScan.getObjectStoreOptionsMap.asScala
      assert(options.get(forwardedMarker._1).contains(forwardedMarker._2))
      assertPlanHidesForwardedOptions(df)
    }
  }

  test("parquet scan reads the scheme lists from the Hadoop conf") {
    // The lists are set only as Hadoop keys, as core-site.xml would set them, so the rule's
    // SQLConf entry does not see them. With them, `blob` is served by libhdfs, so `s3a` and
    // `blob` are two stores that share no settings.
    val s3CompliantSchemes = Set("blob")
    val libhdfsSchemes = Set("hdfs", "blob")
    val (a, b) = (bucket("s3a", "hadoop-conf-a"), bucket("blob", "hadoop-conf-b"))
    writeIds(a, 0)
    writeIds(b, 10)
    def store(path: String): String =
      NativeConfig.objectStoreKey(new URI(path), s3CompliantSchemes, libhdfsSchemes).toString
    val schemeLists = Seq(
      COMET_S3_COMPLIANT_SCHEMES_KEY -> s3CompliantSchemes.mkString(","),
      COMET_LIBHDFS_SCHEMES_KEY -> libhdfsSchemes.mkString(","))
    withSQLConf(nativeScan ++ onePartition ++ schemeLists: _*) {
      def df: DataFrame = spark.read.parquet(a, b)
      val sparkFiles = sparkLayout(df)
      assert(sparkFiles.exists(_.map(store).distinct.size > 1), s"Spark's: $sparkFiles")
      val reasons = new ExtendedExplainInfo().getFallbackReasons(df.queryExecution.executedPlan)
      assert(ruleClaims(df).size == 1, s"fallback reasons: $reasons")
      val files = nativeParquetScan(df).perPartitionFilePaths.toSeq
      assert(files.forall(_.map(store).distinct.size == 1), s"Comet's: $files")
      val stores = files.flatten.map(store).distinct.sorted
      assert(stores == Seq("blob://bucket (libhdfs)", "s3://bucket"), s"Comet's: $files")
    }
  }

  test("catalog table with a partition on another scheme reads each store's files apart") {
    // Without a partition filter the scan's root path is the table location alone, so only
    // the listed files show the second store.
    withTable("mixed_family") {
      withoutComet {
        sql(s"""CREATE TABLE mixed_family (id INT, p STRING) USING parquet PARTITIONED BY (p)
               |LOCATION '${local("mixed-family")}'""".stripMargin)
        sql("INSERT INTO mixed_family PARTITION (p = 'a') SELECT CAST(id AS INT) FROM range(5)")
        val partitionB = hdfs("nn2", "mixed-family-b")
        writeIds(partitionB, 10)
        sql("ALTER TABLE mixed_family ADD PARTITION (p = 'b')")
        sql(s"ALTER TABLE mixed_family PARTITION (p = 'b') SET LOCATION '$partitionB'")
      }
      withSQLConf(nativeScan ++ onePartition :+ forwardedMarker: _*) {
        val df = spark.table("mixed_family")
        val scan = nativeParquetScan(df)
        val files = scan.perPartitionFilePaths.toSeq
        assert(files.forall(_.map(storeKey).distinct.size == 1), s"Comet's: $files")
        assert(files.flatten.map(storeKey).distinct.sorted == Seq("file://", "hdfs://nn2"))
        val options = scan.nativeOp.getNativeScan.getCommon.getObjectStoreOptionsMap.asScala
        assert(options.get(forwardedMarker._1).contains(forwardedMarker._2))
        assertPlanHidesForwardedOptions(df)
      }
    }
  }

  test("bucketed table whose partitions span two name nodes falls back to Spark") {
    withTable("bucketed_nn", "bucketed_nn_b") {
      withoutComet {
        Seq(("bucketed_nn", "nn1"), ("bucketed_nn_b", "nn2")).foreach { case (table, nn) =>
          sql(s"""CREATE TABLE $table (id INT, p STRING) USING parquet PARTITIONED BY (p)
                 |CLUSTERED BY (id) INTO 2 BUCKETS
                 |LOCATION '${hdfs(nn, table)}'""".stripMargin)
        }
        sql("INSERT INTO bucketed_nn PARTITION (p = 'a') SELECT CAST(id AS INT) FROM range(10)")
        sql("INSERT INTO bucketed_nn_b PARTITION (p = 'b') SELECT CAST(id AS INT) FROM range(10)")
        sql("ALTER TABLE bucketed_nn ADD PARTITION (p = 'b')")
        sql(s"""ALTER TABLE bucketed_nn PARTITION (p = 'b')
               |SET LOCATION '${hdfs("nn2", "bucketed_nn_b")}/p=b'""".stripMargin)
      }
      val reason = "Native Parquet scan of a bucketed table reads paths in object stores " +
        "hdfs://nn1 (libhdfs), hdfs://nn2 (libhdfs), but reads each table bucket through " +
        "one store"
      withSQLConf(nativeScan :+ (SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key -> "false"): _*) {
        // The partition filter makes the partition locations the scan's root paths, so the
        // rule declines.
        val filtered = "SELECT * FROM bucketed_nn WHERE p IN ('a', 'b')"
        assert(ruleClaims(sql(filtered)).isEmpty)
        checkSparkAnswerAndFallbackReason(sql(filtered), reason)
        // Without it the root path is the table location: the rule claims the scan, and the
        // conversion declines it over the listed files.
        assert(ruleClaims(spark.table("bucketed_nn")).size == 1)
        val (_, cometPlan) = checkSparkAnswerAndFallbackReason(spark.table("bucketed_nn"), reason)
        assert(collect(cometPlan) { case scan: CometNativeScanExec => scan }.isEmpty)
        // One name node is read natively.
        nativeParquetScan(sql("SELECT * FROM bucketed_nn WHERE p = 'a'"))
      }
    }
  }

  test("dynamic partition pruning over a table whose partitions span two name nodes") {
    // Non-AQE dynamic partition pruning is planned up front, so the claimed scan can be checked
    // without running it. Resolving its pruning runs the local dimension scan only.
    withTable("dpp_fact", "dpp_dim") {
      withoutComet {
        sql(s"""CREATE TABLE dpp_fact (id INT, p STRING) USING parquet PARTITIONED BY (p)
               |LOCATION '${hdfs("nn1", "dpp_fact")}'""".stripMargin)
        sql("INSERT INTO dpp_fact PARTITION (p = 'a') SELECT CAST(id AS INT) FROM range(5)")
        sql("INSERT INTO dpp_fact PARTITION (p = 'c') SELECT CAST(id AS INT) FROM range(5)")
        val partitionB = hdfs("nn2", "dpp_fact_b")
        writeIds(partitionB, 10)
        sql("ALTER TABLE dpp_fact ADD PARTITION (p = 'b')")
        sql(s"ALTER TABLE dpp_fact PARTITION (p = 'b') SET LOCATION '$partitionB'")
        sql(s"""CREATE TABLE dpp_dim (p STRING, kind STRING) USING parquet
               |LOCATION '${local("dpp_dim")}'""".stripMargin)
        sql("INSERT INTO dpp_dim VALUES ('a', 'keep'), ('b', 'keep'), ('c', 'drop')")
      }
      val query =
        "SELECT f.id, f.p FROM dpp_fact f JOIN dpp_dim d ON f.p = d.p WHERE d.kind = 'keep'"
      withSQLConf(
        nativeScan ++ onePartition :+ (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"): _*) {
        val plan = sql(query).queryExecution.executedPlan
        val scans = collect(plan) {
          case scan: CometNativeScanExec if scan.tableIdentifier.exists(_.table == "dpp_fact") =>
            scan
        }
        assert(scans.size == 1, s"expected one native scan of dpp_fact:\n$plan")
        val files = scans.head.perPartitionFilePaths.toSeq
        val allFiles = files.flatten
        assert(allFiles.forall(!_.contains("p=c")), s"p = 'c' was not pruned: $files")
        assert(scans.head.partitionFilters.exists(_.isInstanceOf[DynamicPruningExpression]))
        assert(allFiles.map(storeOf).distinct.sorted == Seq("nn1", "nn2"), s"files: $files")
        assert(files.forall(_.map(storeOf).distinct.size == 1), s"Comet's layout: $files")
      }
    }
  }
}
