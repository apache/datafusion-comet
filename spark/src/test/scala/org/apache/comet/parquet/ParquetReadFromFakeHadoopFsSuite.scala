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
import java.nio.file.Files
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame, SaveMode}
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, sum}

import org.apache.comet.CometConf
import org.apache.comet.hadoop.fs.FakeHDFSFileSystem

class ParquetReadFromFakeHadoopFsSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private var fake_root_dir: File = _

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.hadoop.fs.fake.impl", "org.apache.comet.hadoop.fs.FakeHDFSFileSystem")
    conf.set("spark.hadoop.fs.defaultFS", FakeHDFSFileSystem.PREFIX)
    conf.set(CometConf.COMET_LIBHDFS_SCHEMES.key, "fake,hdfs")
  }

  override def beforeAll(): Unit = {
    // Initialize fake root dir
    fake_root_dir = Files.createTempDirectory(s"comet_fake_${UUID.randomUUID().toString}").toFile
    // Initialize Spark session
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    if (fake_root_dir != null) FileUtils.deleteDirectory(fake_root_dir)
    super.afterAll()
  }

  private def writeTestParquetFile(filePath: String): Unit = {
    val df = spark.range(0, 1000)
    df.write.format("parquet").mode(SaveMode.Overwrite).save(filePath)
  }

  private def assertCometNativeScanOnFakeFs(df: DataFrame): Unit = {
    val scans = collect(df.queryExecution.executedPlan) { case p: CometNativeScanExec =>
      p
    }
    assert(scans.size == 1)
    assert(
      scans.head.nativeOp.getNativeScan
        .getFilePartitions(0)
        .getPartitionedFile(0)
        .getFilePath
        .startsWith(FakeHDFSFileSystem.PREFIX))
  }

  ignore("test native_datafusion scan on fake fs") {
    val testFilePath =
      s"${FakeHDFSFileSystem.PREFIX}${fake_root_dir.getAbsolutePath}/data/test-file.parquet"
    writeTestParquetFile(testFilePath)
    withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION) {
      val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")))
      assertCometNativeScanOnFakeFs(df)
      assert(df.first().getLong(0) == 499500)
    }
  }
}
