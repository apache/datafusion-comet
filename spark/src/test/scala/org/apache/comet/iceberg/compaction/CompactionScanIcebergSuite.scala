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

package org.apache.comet.iceberg.compaction

import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometIcebergNativeScanExec

import org.apache.comet.{CometConf, CometIcebergTestBase}
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.iceberg.IcebergReflection

/**
 * Tests that Comet's native Iceberg scan reads a [[CompactionScan]] over a real table: one Spark
 * partition per bin, and the table's rows. This is the read half of a bins job, without the write
 * and the commit.
 */
class CompactionScanIcebergSuite extends CometTestBase with CometIcebergTestBase {

  private val catalog = "test_cat"

  private def withNativeIceberg(body: => Unit): Unit = {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(isSpark40Plus, "Comet's compaction runs on Spark 4.0 and later only")
    withHadoopCatalog(catalog) {
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true")(body)
    }
  }

  private def appendFile(table: String, index: Int): Unit =
    spark
      .range(index * 50L, (index + 1) * 50L)
      .selectExpr("CAST(id AS INT) AS id", "CAST(id AS STRING) AS name")
      .coalesce(1)
      .write
      .format("iceberg")
      .mode("append")
      .saveAsTable(table)

  /** The batch that reads the table's files in bins of `filesPerBin` files each. */
  private def batchOf(icebergTable: AnyRef, filesPerBin: Int): CompactionBatch = {
    val snapshotId = IcebergRewriteReflection.currentSnapshotId(icebergTable).get
    val specId = IcebergRewriteReflection.currentSpecId(icebergTable)
    val files = IcebergRewriteReflection.planFiles(spark, icebergTable, snapshotId, None, specId)
    val bins = files.grouped(filesPerBin).toSeq.map { bin =>
      new BinTasks(new java.util.ArrayList[AnyRef](bin.map(_.task).asJava))
    }
    val schema = IcebergReflection.getSchema(icebergTable).get.asInstanceOf[AnyRef]
    new CompactionBatch(icebergTable, schema, IcebergReflection.toSparkSchema(schema), bins)
  }

  test("the native scan reads each bin as a Spark partition") {
    withNativeIceberg {
      val table = s"$catalog.db.compaction_scan"
      try {
        sql(s"CREATE TABLE $table (id INT, name STRING) USING iceberg")
        (0 until 4).foreach(appendFile(table, _))
        val icebergTable = loadIcebergTable(spark, catalog, "db", "compaction_scan")

        val id = UUID.randomUUID().toString
        CompactionBatches.register(id, batchOf(icebergTable, filesPerBin = 2))
        try {
          val df = spark.read
            .format(classOf[CompactionSource].getName)
            .option(CompactionSource.BatchIdOption, id)
            .load()

          val nativeScans = df.queryExecution.executedPlan.collect {
            case scan: CometIcebergNativeScanExec => scan
          }
          assert(nativeScans.size == 1, df.queryExecution.executedPlan.toString)
          assert(nativeScans.head.numPartitions == 2)
          checkAnswer(df, sql(s"SELECT * FROM $table"))
          assert(df.count() == 200)
        } finally {
          CompactionBatches.remove(id)
        }
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }

  test("the native scan reads a bin of one file as one partition") {
    withNativeIceberg {
      val table = s"$catalog.db.compaction_scan_files"
      try {
        sql(s"CREATE TABLE $table (id INT, name STRING) USING iceberg")
        (0 until 3).foreach(appendFile(table, _))
        val icebergTable = loadIcebergTable(spark, catalog, "db", "compaction_scan_files")

        val id = UUID.randomUUID().toString
        CompactionBatches.register(id, batchOf(icebergTable, filesPerBin = 1))
        try {
          val df = spark.read
            .format(classOf[CompactionSource].getName)
            .option(CompactionSource.BatchIdOption, id)
            .load()
          val nativeScan = df.queryExecution.executedPlan.collectFirst {
            case scan: CometIcebergNativeScanExec => scan
          }
          assert(nativeScan.exists(_.numPartitions == 3), df.queryExecution.executedPlan.toString)
          checkAnswer(df, sql(s"SELECT * FROM $table"))
        } finally {
          CompactionBatches.remove(id)
        }
      } finally {
        sql(s"DROP TABLE IF EXISTS $table")
      }
    }
  }
}
