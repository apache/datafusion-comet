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

import org.apache.spark.sql.{CometTestBase, DataFrame, SaveMode}
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, sum}

import org.apache.comet.{CometConf, WithHdfsCluster}

class ParquetReadFromHdfsSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with WithHdfsCluster {

  override protected def createSparkSession: SparkSessionType = {
    // start HDFS cluster and add hadoop conf
    startHdfsCluster()
    val sparkSession = super.createSparkSession
    sparkSession.sparkContext.hadoopConfiguration.addResource(getHadoopConfFile)
    sparkSession
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    stopHdfsCluster()
  }

  private def writeTestParquetFile(filePath: String): Unit = {
    val df = spark.range(0, 1000)
    df.write.format("parquet").mode(SaveMode.Overwrite).save(filePath)
  }

  private def assertCometNativeScanOnHDFS(df: DataFrame): Unit = {
    val scans = collect(df.queryExecution.executedPlan) { case p: CometNativeScanExec =>
      p
    }
    assert(scans.size == 1)
    assert(
      scans.head.nativeOp.getNativeScan
        .getFilePartitions(0)
        .getPartitionedFile(0)
        .getFilePath
        .startsWith("hdfs://"))
  }

  test("test native_datafusion scan on hdfs") {
    withTmpHdfsDir { dir =>
      {
        val testFilePath = dir.toString
        writeTestParquetFile(testFilePath)
        withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION) {
          val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")))
          assertCometNativeScanOnHDFS(df)
          assert(df.first().getLong(0) == 499500)
        }
      }
    }
  }
}
