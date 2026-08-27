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

package org.apache.comet

import java.io.File

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.IcebergWriteExec

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus
import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.serde.{Compatible, SupportLevel, Unsupported}
import org.apache.comet.serde.operator.CometIcebergNativeWrite

class CometIcebergWriteDetectionSuite extends CometTestBase with CometIcebergTestBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key, "true")
      .set(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key, "true")
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      assume(icebergAvailable, "Iceberg not available in classpath")
      testFun
    }
  }

  test("clean parquet V2 table planned as AppendData yields Compatible") {
    withDetectionCatalog { dir =>
      createTable(dir, "ok", partitionSpec = "")
      assertSupportLevelIs[Compatible]("ok")
    }
  }

  test("registration tags a fall-back reason on the write exec") {
    withDetectionCatalog { dir =>
      createTable(dir, "tagged", partitionSpec = "")
      val writeExec = insertWriteExec("tagged")
      val reasons = writeExec.getTagValue(CometExplainInfo.FALLBACK_REASONS)
      assert(
        reasons.exists(_.nonEmpty),
        s"expected CometExecRule to record a fall-back reason on $writeExec")
    }
  }

  test("SparkWrite reflection helpers all resolve on the current Iceberg runtime") {
    withDetectionCatalog { dir =>
      createTable(dir, "refl_probe", partitionSpec = "")
      val sparkWrite = IcebergReflection
        .getOuterSparkWrite(insertWriteExec("refl_probe").batchWrite)
        .getOrElse(fail("could not unwrap outer SparkWrite from BatchWrite"))
      val table = IcebergReflection
        .getTableFromSparkWrite(sparkWrite)
        .getOrElse(fail("SparkWrite.table reflection returned None"))

      assert(IcebergReflection.getFormatFromSparkWrite(sparkWrite).isDefined, "format")
      assert(
        IcebergReflection.getWritePropertiesFromSparkWrite(sparkWrite).isDefined,
        "writeProperties")
      assert(IcebergReflection.getDataLocation(table).isDefined, "dataLocation")
      assert(IcebergReflection.getTableProperties(table).isDefined, "tableProperties")
    }
  }

  test("Compatible when a session conf overrides the compression codec") {
    withDetectionCatalog { dir =>
      createTable(dir, "session_codec", partitionSpec = "")
      withSQLConf("spark.sql.iceberg.compression-codec" -> "gzip") {
        assertSupportLevelIs[Compatible]("session_codec")
      }
    }
  }

  test("fall-back: write.format.default=orc") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "fmt_orc",
        partitionSpec = "",
        properties = Some("'write.format.default'='orc'"))
      assertUnsupportedContains("fmt_orc", "format=orc", "only parquet")
    }
  }

  test("fall-back: per-write write-format option overrides parquet default") {
    withDetectionCatalog { dir =>
      createTable(dir, "fmt_orc_opt", partitionSpec = "")
      assertUnsupportedContains(
        dfWriteExec("fmt_orc_opt", "write-format" -> "orc"),
        "fmt_orc_opt",
        "format=orc",
        "only parquet")
    }
  }

  test("fall-back: write.object-storage.enabled=true") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "obj_store",
        partitionSpec = "",
        properties = Some("'write.object-storage.enabled'='true'"))
      assertUnsupportedContains("obj_store", "write.object-storage.enabled")
    }
  }

  test("fall-back: write.location-provider.impl set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "loc_provider",
        partitionSpec = "",
        properties = Some("'write.location-provider.impl'='com.example.MyProvider'"))
      assertUnsupportedContainsAllowingWriteFailure(
        "loc_provider",
        "write.location-provider.impl")
    }
  }

  test("fall-back: format-version=3") {
    assume(isSpark35Plus, "V3 tables require Iceberg 1.8.1+ (Spark 3.5 profile)")
    withDetectionCatalog { dir =>
      createTable(dir, "v3", partitionSpec = "", properties = Some("'format-version'='3'"))
      assertUnsupportedContains("v3", "format-version=3")
    }
  }

  test("fall-back: encryption.kms-client-impl set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "enc",
        partitionSpec = "",
        properties = Some("'encryption.kms-client-impl'='com.example.MyKms'"))
      assertUnsupportedContainsAllowingWriteFailure("enc", "encryption")
    }
  }

  test("fall-back: write.metadata.metrics.default=counts") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "metrics_counts",
        partitionSpec = "",
        properties = Some("'write.metadata.metrics.default'='counts'"))
      assertUnsupportedContains("metrics_counts", "write.metadata.metrics.default", "counts")
    }
  }

  test("fall-back: per-column metrics mode=counts") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "metrics_col_counts",
        partitionSpec = "",
        properties = Some("'write.metadata.metrics.column.id'='counts'"))
      assertUnsupportedContains(
        "metrics_col_counts",
        "write.metadata.metrics.column.id",
        "counts")
    }
  }

  test("fall-back: write.parquet.bloom-filter-max-bytes set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "bloom_max",
        partitionSpec = "",
        properties = Some("'write.parquet.bloom-filter-max-bytes'='524288'"))
      assertUnsupportedContains("bloom_max", "write.parquet.bloom-filter-max-bytes")
    }
  }

  test("fall-back: per-column bloom filter enabled") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "bloom_col",
        partitionSpec = "",
        properties = Some("'write.parquet.bloom-filter-enabled.column.id'='true'"))
      assertUnsupportedContains(
        "bloom_col",
        "write.parquet.bloom-filter-enabled.column.id",
        "true")
    }
  }

  test("Compatible when the schema exceeds max-inferred-column-defaults") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "too_many_cols",
        partitionSpec = "",
        properties = Some("'write.metadata.metrics.max-inferred-column-defaults'='2'"))
      assertSupportLevelIs[Compatible]("too_many_cols")
    }
  }

  test("fall-back: row-group-check-min-record-count non-default") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "rg_min",
        partitionSpec = "",
        properties = Some("'write.parquet.row-group-check-min-record-count'='500'"))
      assertUnsupportedContains("rg_min", "write.parquet.row-group-check-min-record-count=500")
    }
  }

  test("Compatible when row-group-check-min-record-count is at default (100)") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "rg_min_default",
        partitionSpec = "",
        properties = Some("'write.parquet.row-group-check-min-record-count'='100'"))
      assertSupportLevelIs[Compatible]("rg_min_default")
    }
  }

  test("fall-back: row-group-check-max-record-count non-default") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "rg_max",
        partitionSpec = "",
        properties = Some("'write.parquet.row-group-check-max-record-count'='50000'"))
      assertUnsupportedContains("rg_max", "write.parquet.row-group-check-max-record-count=50000")
    }
  }

  test("fall-back: write.metadata.metrics.default=none") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "metrics_none",
        partitionSpec = "",
        properties = Some("'write.metadata.metrics.default'='none'"))
      assertUnsupportedContains("metrics_none", "write.metadata.metrics.default", "none")
    }
  }

  test("fall-back: per-column metrics mode=none") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "col_metrics_none",
        partitionSpec = "",
        properties = Some("'write.metadata.metrics.column.region'='none'"))
      assertUnsupportedContains(
        "col_metrics_none",
        "write.metadata.metrics.column.region",
        "none")
    }
  }

  test("fall-back: write.parquet.page-version=v2") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "page_v2",
        partitionSpec = "",
        properties = Some("'write.parquet.page-version'='v2'"))
      assertUnsupportedContains("page_v2", "write.parquet.page-version", "v2")
    }
  }

  test("fall-back: parquet.enable.dictionary set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "enable_dict",
        partitionSpec = "",
        properties = Some("'parquet.enable.dictionary'='false'"))
      assertUnsupportedContains("enable_dict", "parquet.enable.dictionary")
    }
  }

  test("fall-back: per-column write.parquet.stats-enabled.<col> set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "col_stats",
        partitionSpec = "",
        properties = Some("'write.parquet.stats-enabled.column.region'='false'"))
      assertUnsupportedContains("col_stats", "write.parquet.stats-enabled.column.region")
    }
  }

  test("fall-back: unvetted write.parquet.* property") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "unvetted",
        partitionSpec = "",
        properties = Some("'write.parquet.bloom-filter-adaptive-enabled'='true'"))
      assertUnsupportedContains(
        "unvetted",
        "write.parquet.bloom-filter-adaptive-enabled",
        "not a vetted")
    }
  }

  test("fall-back: parquet.* table property other than enable.dictionary") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "pq_mr_prop",
        partitionSpec = "",
        properties = Some("'parquet.columnindex.truncate.length'='32'"))
      assertUnsupportedContains("pq_mr_prop", "parquet.columnindex.truncate.length")
    }
  }

  test("Compatible when a codec level side-channel property is set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "codec_level",
        partitionSpec = "",
        properties = Some("'zlib.compress.level'='9'"))
      assertSupportLevelIs[Compatible]("codec_level")
    }
  }

  test("fall-back: parquet.* key in the session Hadoop configuration") {
    withDetectionCatalog { dir =>
      createTable(dir, "hadoop_conf", partitionSpec = "")
      withSQLConf("parquet.block.size" -> "1048576") {
        assertUnsupportedContains("hadoop_conf", "parquet.block.size", "Hadoop configuration")
      }
    }
  }

  // parquet.hadoop.vectored.io.enabled is a reader-side vectored-IO knob declared by
  // parquet-hadoop (ParquetInputFormat.HADOOP_VECTORED_IO_ENABLED, default true in
  // parquet-hadoop 1.16+). iceberg-java's writer never consumes it, so it must not
  // disable native Iceberg writes when it happens to be present in the session
  // Hadoop configuration.
  test("Compatible when only parquet.hadoop.vectored.io.enabled is set in Hadoop configuration") {
    withDetectionCatalog { dir =>
      createTable(dir, "vectored_io_only", partitionSpec = "")
      withSQLConf("parquet.hadoop.vectored.io.enabled" -> "true") {
        assertSupportLevelIs[Compatible]("vectored_io_only")
      }
    }
  }

  test("fall-back: io-impl set") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "io_impl",
        partitionSpec = "",
        properties = Some("'io-impl'='com.example.MyFileIO'"))
      assertUnsupportedContainsAllowingWriteFailure("io_impl", "io-impl")
    }
  }

  test("fall-back: data location URI scheme not supported by the native writer") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "bad_scheme",
        partitionSpec = "",
        properties = Some("'write.data.path'='hdfs://nonexistent.invalid/iceberg/db/bad_scheme'"))
      assertUnsupportedContainsAllowingWriteFailure("bad_scheme", "storage scheme", "hdfs")
    }
  }

  test("Compatible when the data location scheme is s3") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "s3_scheme",
        partitionSpec = "",
        properties = Some("'write.data.path'='s3://nonexistent-bucket/iceberg/db/s3_scheme'"))
      assertSupportLevelIs[Compatible]("s3_scheme", allowWriteFailure = true)
    }
  }

  test("Compatible for the remaining supported data location schemes") {
    withDetectionCatalog { dir =>
      Seq("gs", "oss", "memory").foreach { scheme =>
        val table = s"${scheme}_scheme"
        createTable(
          dir,
          table,
          partitionSpec = "",
          properties = Some(s"'write.data.path'='$scheme://nonexistent/iceberg/db/$table'"))
        assertSupportLevelIs[Compatible](table, allowWriteFailure = true)
      }
    }
  }

  test("Compatible when the data location is an explicit file:// path") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "file_scheme",
        partitionSpec = "",
        properties = Some(s"'write.data.path'='file://${dir.getAbsolutePath}/file_scheme_data'"))
      assertSupportLevelIs[Compatible]("file_scheme")
    }
  }

  test("fall-back: write.parquet.shred-variants=true") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "shred",
        partitionSpec = "",
        properties = Some("'write.parquet.shred-variants'='true'"))
      assertUnsupportedContains("shred", "write.parquet.shred-variants")
    }
  }

  test("fall-back: unparseable write.metadata.metrics.default") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "metrics_typo",
        partitionSpec = "",
        properties = Some("'write.metadata.metrics.default'='truncat(16)'"))
      assertUnsupportedContains("metrics_typo", "truncat(16)", "supported metrics modes")
    }
  }

  test("Compatible when write.spark.fanout.enabled=true") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "fanout",
        partitionSpec = "PARTITIONED BY (bucket(4, id))",
        properties = Some("'write.spark.fanout.enabled'='true'"))
      assertSupportLevelIs[Compatible]("fanout")
    }
  }

  test("Compatible when write.target-file-size-bytes is non-default") {
    withDetectionCatalog { dir =>
      createTable(
        dir,
        "target_size",
        partitionSpec = "",
        properties = Some("'write.target-file-size-bytes'='1048576'"))
      assertSupportLevelIs[Compatible]("target_size")
    }
  }

  test("no fall-back reason is recorded when the iceberg write feature is disabled") {
    withDetectionCatalog { dir =>
      createTable(dir, "flag_off", partitionSpec = "")
      withSQLConf(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key -> "false") {
        val writeExec = insertWriteExec("flag_off")
        assert(
          writeExec.getTagValue(CometExplainInfo.FALLBACK_REASONS).isEmpty,
          "expected no fall-back reason on the write exec when the feature is disabled")
      }
    }
  }

  test("fall-back: BatchWrite that is not an Iceberg SparkWrite") {
    withDetectionCatalog { dir =>
      createTable(dir, "plain_write", partitionSpec = "")
      val stub = new org.apache.spark.sql.connector.write.BatchWrite {
        override def createBatchWriterFactory(
            info: org.apache.spark.sql.connector.write.PhysicalWriteInfo)
            : org.apache.spark.sql.connector.write.DataWriterFactory =
          throw new UnsupportedOperationException("stub")
        override def commit(
            messages: Array[org.apache.spark.sql.connector.write.WriterCommitMessage]): Unit =
          ()
        override def abort(
            messages: Array[org.apache.spark.sql.connector.write.WriterCommitMessage]): Unit =
          ()
      }
      val fake = insertWriteExec("plain_write").copy(batchWrite = stub)
      assertUnsupportedContains(fake, "plain_write", "not an Iceberg SparkWrite")
    }
  }

  test("Compatible when partitioned by a bucket transform") {
    withDetectionCatalog { dir =>
      createTable(dir, "part_bucket", partitionSpec = "PARTITIONED BY (bucket(4, id))")
      assertSupportLevelIs[Compatible]("part_bucket")
    }
  }

  test("Compatible when partitioned by identity on a string column") {
    withDetectionCatalog { dir =>
      createTable(dir, "part_string", partitionSpec = "PARTITIONED BY (region)")
      assertSupportLevelIs[Compatible]("part_string")
    }
  }

  private val catalog = "cat"
  private val ns = "db"

  private def withDetectionCatalog(f: File => Unit): Unit = withTempIcebergDir { warehouseDir =>
    withSQLConf(
      s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$catalog.type" -> "hadoop",
      s"spark.sql.catalog.$catalog.warehouse" -> warehouseDir.getAbsolutePath) {
      f(warehouseDir)
    }
  }

  private def createTable(
      warehouseDir: File,
      tableName: String,
      partitionSpec: String,
      properties: Option[String] = None): Unit = {
    val props = properties.map(s => s" TBLPROPERTIES ($s)").getOrElse("")
    spark.sql(s"""
      CREATE TABLE $catalog.$ns.$tableName (
        id INT,
        region STRING,
        amount DOUBLE
      ) USING iceberg
      $partitionSpec
      $props
    """)
  }

  private def insertWriteExec(
      tableName: String,
      allowWriteFailure: Boolean = false): IcebergWriteExec =
    captureWriteExec(tableName, allowWriteFailure) {
      spark.sql(s"INSERT INTO $catalog.$ns.$tableName VALUES (1, 'us', 1.0)")
    }

  private def dfWriteExec(tableName: String, options: (String, String)*): IcebergWriteExec =
    captureWriteExec(tableName, allowWriteFailure = false) {
      val df = spark
        .createDataFrame(Seq((1, "us", 1.0)))
        .toDF("id", "region", "amount")
      val writer = options.foldLeft(df.writeTo(s"$catalog.$ns.$tableName")) { case (w, (k, v)) =>
        w.option(k, v)
      }
      writer.append()
    }

  private def captureWriteExec(tableName: String, allowWriteFailure: Boolean)(
      trigger: => Unit): IcebergWriteExec = {
    val captured =
      new java.util.concurrent.atomic.AtomicReference[org.apache.spark.sql.execution.SparkPlan]()
    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(
          funcName: String,
          qe: org.apache.spark.sql.execution.QueryExecution,
          durationNs: Long): Unit =
        captured.compareAndSet(null, qe.executedPlan)
      override def onFailure(
          funcName: String,
          qe: org.apache.spark.sql.execution.QueryExecution,
          exception: Exception): Unit =
        captured.compareAndSet(null, qe.executedPlan)
    }
    var failure: Option[Throwable] = None
    try org.apache.spark.CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    catch { case _: java.util.concurrent.TimeoutException => () }
    spark.listenerManager.register(listener)
    try {
      try trigger
      catch { case scala.util.control.NonFatal(t) => failure = Some(t) }
      try org.apache.spark.CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
      catch { case _: java.util.concurrent.TimeoutException => () }
    } finally {
      spark.listenerManager.unregister(listener)
    }
    if (!allowWriteFailure) {
      failure.foreach(t => fail(s"write to $tableName failed unexpectedly", t))
    }
    val plan = Option(captured.get())
      .getOrElse(fail(s"No QueryExecution captured for $tableName"))
    findWriteExecOrFail(plan)
  }

  private def findWriteExecOrFail(
      plan: org.apache.spark.sql.execution.SparkPlan): IcebergWriteExec =
    findWriteExec(plan).getOrElse(fail(s"no IcebergWriteExec found in:\n$plan"))

  private def findWriteExec(
      plan: org.apache.spark.sql.execution.SparkPlan): Option[IcebergWriteExec] =
    plan match {
      case e: IcebergWriteExec => Some(e)
      case other =>
        val descend = other.children.iterator ++ wrappedChildren(other).iterator
        descend.flatMap(findWriteExec).toSeq.headOption
    }

  private def wrappedChildren(plan: org.apache.spark.sql.execution.SparkPlan)
      : Iterable[org.apache.spark.sql.execution.SparkPlan] = {
    def viaAccessor(method: String): Option[org.apache.spark.sql.execution.SparkPlan] =
      scala.util
        .Try {
          plan.getClass
            .getMethod(method)
            .invoke(plan)
            .asInstanceOf[org.apache.spark.sql.execution.SparkPlan]
        }
        .toOption
        .filter(_ ne plan)
    Seq("commandPhysicalPlan", "executedPlan", "plan").flatMap(viaAccessor)
  }

  private def assertSupportLevelIs[T <: SupportLevel: scala.reflect.ClassTag](
      tableName: String,
      allowWriteFailure: Boolean = false): Unit = {
    val support =
      CometIcebergNativeWrite.getSupportLevel(insertWriteExec(tableName, allowWriteFailure))
    val expected = scala.reflect.classTag[T].runtimeClass
    assert(
      expected.isInstance(support),
      s"expected ${expected.getSimpleName} for $tableName, got $support")
  }

  private def assertUnsupportedContains(tableName: String, fragments: String*): Unit =
    assertUnsupportedContains(insertWriteExec(tableName), tableName, fragments: _*)

  private def assertUnsupportedContainsAllowingWriteFailure(
      tableName: String,
      fragments: String*): Unit =
    assertUnsupportedContains(
      insertWriteExec(tableName, allowWriteFailure = true),
      tableName,
      fragments: _*)

  private def assertUnsupportedContains(
      writeExec: IcebergWriteExec,
      tableName: String,
      fragments: String*): Unit = {
    val support = CometIcebergNativeWrite.getSupportLevel(writeExec)
    support match {
      case Unsupported(Some(reason)) =>
        fragments.foreach(f =>
          assert(reason.contains(f), s"reason '$reason' missing fragment '$f' for $tableName"))
      case Unsupported(None) =>
        fail(s"Unsupported without a reason string for $tableName")
      case other =>
        fail(s"expected Unsupported for $tableName, got $other")
    }
  }
}
