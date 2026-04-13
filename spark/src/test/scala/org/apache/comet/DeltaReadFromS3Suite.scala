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

import org.apache.spark.SparkConf
import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * Integration tests for reading Delta tables from S3/MinIO via Comet's native Delta scan.
 *
 * Exercises the full cloud-storage path: kernel reads the _delta_log from S3 via object_store,
 * kernel fetches deletion-vector .bin files from S3, and Comet's ParquetSource reads data files
 * from S3. Storage credentials flow through the JNI to both kernel's DefaultEngine and Comet's
 * object_store.
 *
 * Requires Docker for the MinIO testcontainer. Tagged as IntegrationTestSuite so it only runs
 * when explicitly requested (not in the default test suite).
 */
class DeltaReadFromS3Suite extends CometS3TestBase with AdaptiveSparkPlanHelper {

  override protected val testBucketName = "test-delta-bucket"

  private def deltaSparkAvailable: Boolean =
    try {
      Class.forName("org.apache.spark.sql.delta.DeltaParquetFileFormat")
      true
    } catch {
      case _: ClassNotFoundException => false
    }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(CometConf.COMET_DELTA_NATIVE_ENABLED.key, "true")
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    conf.set("spark.databricks.delta.testOnly.dataFileNamePrefix", "")
    conf.set("spark.databricks.delta.testOnly.dvFileNamePrefix", "")
    conf
  }

  private def collectDeltaNativeScans(plan: SparkPlan): Seq[CometDeltaNativeScanExec] = {
    collect(plan) { case scan: CometDeltaNativeScanExec => scan }
  }

  test("create and query simple Delta table from MinIO") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath")

    val tablePath = s"s3a://$testBucketName/delta/simple_table"
    val ss = spark
    import ss.implicits._

    (0 until 20)
      .map(i => (i.toLong, s"name_$i", i * 1.5))
      .toDF("id", "name", "score")
      .repartition(1)
      .write
      .format("delta")
      .mode("overwrite")
      .save(tablePath)

    val (_, cometPlan) = checkSparkAnswer(s"SELECT * FROM delta.`$tablePath`")
    val scans = collectDeltaNativeScans(cometPlan)
    assert(scans.nonEmpty, s"Expected CometDeltaNativeScanExec in plan:\n$cometPlan")
  }

  test("partitioned Delta table on MinIO with filter pushdown") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath")

    val tablePath = s"s3a://$testBucketName/delta/partitioned_table"
    val ss = spark
    import ss.implicits._

    (0 until 40)
      .map(i => (i.toLong, s"name_$i", if (i < 20) "hot" else "cold"))
      .toDF("id", "name", "tier")
      .write
      .partitionBy("tier")
      .format("delta")
      .mode("overwrite")
      .save(tablePath)

    val (_, cometPlan) =
      checkSparkAnswer(s"SELECT * FROM delta.`$tablePath` WHERE tier = 'hot' AND id > 5")
    assert(
      collectDeltaNativeScans(cometPlan).nonEmpty,
      s"Expected native Delta scan:\n$cometPlan")
  }

  test("multi-file Delta table on MinIO") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath")

    val tablePath = s"s3a://$testBucketName/delta/multifile_table"
    val ss = spark
    import ss.implicits._

    (0 until 100)
      .map(i => (i.toLong, s"name_$i"))
      .toDF("id", "name")
      .repartition(4)
      .write
      .format("delta")
      .mode("overwrite")
      .save(tablePath)

    val (_, cometPlan) = checkSparkAnswer(s"SELECT COUNT(*) FROM delta.`$tablePath`")
    // COUNT(*) may be resolved from stats (LocalTableScan), so don't assert scan presence.
    // Just verify correctness.
  }

  test("Delta table with deletion vectors on MinIO") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath")

    val tablePath = s"s3a://$testBucketName/delta/dv_table"
    val ss = spark
    import ss.implicits._

    (0 until 20)
      .map(i => (i.toLong, s"name_$i"))
      .toDF("id", "name")
      .repartition(1)
      .write
      .format("delta")
      .option("delta.enableDeletionVectors", "true")
      .option("delta.minReaderVersion", "3")
      .option("delta.minWriterVersion", "7")
      .mode("overwrite")
      .save(tablePath)

    // Pre-DELETE: native acceleration
    val (_, plan1) = checkSparkAnswer(s"SELECT * FROM delta.`$tablePath`")
    assert(collectDeltaNativeScans(plan1).nonEmpty, s"Expected native scan pre-DELETE:\n$plan1")

    // DELETE + read in the same config scope so useMetadataRowIndex=false
    // applies to BOTH the write and the subsequent read.
    withSQLConf("spark.databricks.delta.deletionVectors.useMetadataRowIndex" -> "false") {
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id % 3 = 0")
      val result = spark.read.format("delta").load(tablePath).collect()
      assert(result.length == 13, s"Expected 13 rows after DELETE, got ${result.length}")
    }
  }
}
