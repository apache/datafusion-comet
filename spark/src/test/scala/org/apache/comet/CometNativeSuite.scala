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

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.PrettyAttribute
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometExec, CometExecUtils}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.vectorized.ColumnarBatch

class CometNativeSuite extends CometTestBase {
  test("test handling NPE thrown by JVM") {
    val rdd = spark.range(0, 1).rdd.map { value =>
      val limitOp =
        CometExecUtils.getLimitNativePlan(Seq(PrettyAttribute("test", LongType)), 100).get
      val cometIter = CometExec.getCometIterator(
        Seq(new Iterator[ColumnarBatch] {
          override def hasNext: Boolean = true
          override def next(): ColumnarBatch = throw new NullPointerException()
        }),
        1,
        limitOp,
        1,
        0)
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

  test("handling NPE when closing null handle of parquet reader") {
    assert(NativeBase.isLoaded)
    val exception1 = intercept[NullPointerException] {
      parquet.Native.closeRecordBatchReader(0)
    }
    assert(exception1.getMessage contains "null batch context handle")

    val exception2 = intercept[NullPointerException] {
      parquet.Native.closeColumnReader(0)
    }
    assert(exception2.getMessage contains "null context handle")
  }

  test("Comet native should use spark local dir as temp dir") {
    withParquetTable((0 until 100000).map(i => (i, i + 1)), "table") {
      val dirs = SparkEnv.get.blockManager.getLocalDiskDirs
      dirs.foreach { dir =>
        val files = new java.io.File(dir).listFiles()
        assert(!files.exists(f => f.isDirectory && f.getName.startsWith("datafusion-")))
      }

      // Check if the DataFusion temporary dir exists in the Spark local dirs when a spark job involving
      // Comet native operator is running.
      val observedDataFusionDir = spark
        .table("table")
        .selectExpr("_1 + _2 as value")
        .rdd
        .mapPartitions { _ =>
          dirs.map { dir =>
            val files = new java.io.File(dir).listFiles()
            files.count(f => f.isDirectory && f.getName.startsWith("datafusion-"))
          }.iterator
        }
        .sum()
      assert(observedDataFusionDir > 0)

      // DataFusion temporary dir should be cleaned up after the job is done.
      dirs.foreach { dir =>
        val files = new java.io.File(dir).listFiles()
        assert(!files.exists(f => f.isDirectory && f.getName.startsWith("datafusion-")))
      }
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
