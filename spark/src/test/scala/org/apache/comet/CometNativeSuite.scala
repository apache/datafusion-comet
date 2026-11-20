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

import scala.concurrent.duration.DurationInt

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.catalyst.expressions.PrettyAttribute
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometExec, CometExecUtils}
import org.apache.spark.sql.comet.execution.arrow.CometArrowStream
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class CometNativeSuite extends CometTestBase {
  test("test handling NPE thrown by JVM") {
    val rdd = spark.range(0, 1).rdd.map { value =>
      val limitOp =
        CometExecUtils.getLimitNativePlan(Seq(PrettyAttribute("test", LongType)), 100).get
      val arrowStream = CometArrowStream.fromColumnarBatchIter(
        new Iterator[ColumnarBatch] {
          override def hasNext: Boolean = true
          override def next(): ColumnarBatch = throw new NullPointerException()
        },
        StructType(Seq(StructField("test", LongType, nullable = false))),
        CometArrowStream.NATIVE_TIMEZONE,
        "test-npe")
      val cometIter =
        CometExec.getCometIterator(Array(arrowStream.asInstanceOf[Object]), 1, limitOp, 1, 0)
      try {
        cometIter.next()
      } finally {
        cometIter.close()
      }
      value
    }

    val exception = intercept[SparkException] {
      rdd.collect()
    }
    assert(exception.getMessage contains "java.lang.NullPointerException")
  }

  test("Comet native should use spark local dir as temp dir") {
    val dirs = SparkEnv.get.blockManager.getLocalDiskDirs
    // The number of DataFusion temp dirs in the Spark local dirs.
    val tempDirs = () =>
      dirs.map { dir =>
        val files = new java.io.File(dir).listFiles()
        files.count(f => f.isDirectory && f.getName.startsWith("datafusion-"))
      }.sum
    // Runs `df` and returns how many temp dirs its tasks see once their first row is ready, while
    // the native plan that produced it is still open.
    val tempDirsWhileRunning = (df: DataFrame) =>
      df.rdd
        .mapPartitions { rows =>
          rows.hasNext
          Iterator(tempDirs())
        }
        .collect()
        .sum
    assert(tempDirs() == 0)

    // A native plan creates its temp dirs when it first spills, so one that never spills creates
    // none.
    withParquetTable((0 until 100000).map(i => (i, i + 1)), "table") {
      assert(tempDirsWhileRunning(spark.table("table").selectExpr("_1 + _2 as value")) == 0)
    }

    withTempPath { path =>
      // A single input file keeps all rows in one task so its sort outgrows the tiny memory pool
      // below and must spill.
      spark
        .createDataFrame((0 until 20000).map(i => (i, "native-sort-spill-" * 16)))
        .coalesce(1)
        .write
        .parquet(path.getAbsolutePath)
      withSQLConf(
        CometConf.COMET_BATCH_SIZE.key -> "1024",
        CometConf.COMET_OFFHEAP_MEMORY_POOL_FRACTION.key -> "0.002",
        CometConf.COMET_RESPECT_DATAFUSION_CONFIGS.key -> "true",
        "spark.comet.datafusion.execution.sort_spill_reservation_bytes" -> "65536") {
        val sorted = spark.read.parquet(path.getAbsolutePath).sortWithinPartitions(col("_1").desc)
        assert(tempDirsWhileRunning(sorted) > 0, "The native sort did not spill to a local dir")
      }
    }

    // The temp dirs are removed once the plans are released. The sort reads its spill files on
    // Tokio tasks that can finish just after the release, so allow them a moment.
    eventually(timeout(10.seconds), interval(100.milliseconds)) {
      assert(tempDirs() == 0)
    }
  }

  test("test maxBroadcastTableSize") {
    withSQLConf("spark.sql.maxBroadcastTableSize" -> "10B") {
      spark.range(0, 1000).createOrReplaceTempView("t1")
      spark.range(0, 100).createOrReplaceTempView("t2")
      val df = spark.sql("select /*+ BROADCAST(t2) */ * from t1 join t2 on t1.id = t2.id")
      val exception = intercept[SparkException] {
        df.collect()
      }
      assert(
        exception.getMessage.contains("Cannot broadcast the table that is larger than 10.0 B"))
      val broadcasts = collect(df.queryExecution.executedPlan) {
        case p: CometBroadcastExchangeExec => p
      }
      assert(broadcasts.size == 1)
    }
  }
}
