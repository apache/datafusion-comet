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
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.CometSortExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, regexp_replace}

/**
 * A codegen-dispatched expression feeding a native sort in a small off-heap pool.
 *
 * A JVM UDF's output is charged to the Spark task while the UDF holds it. Native operators
 * reserve through a consumer whose `spill` returns 0, so they spill only when their own
 * `try_grow` fails, and by then they have filled the pool. The next UDF allocation then asks for
 * more than Spark has left. Refusing it would fail the task where the sort could have spilled, so
 * the allocation is recorded instead.
 *
 * `greedy_unified` makes Spark's grant the only limit on the sort, so the sort fills the pool
 * within a single task. DataFusion's default 10 MiB merge reservation leaves 6 MiB of the 16 MiB
 * pool for buffered batches. The UDF allocates each output from the size of its input, several
 * times what the sort reserves for the shrunken batch that comes out, so the pool runs short for
 * the UDF (about 115,000 rows in) before it runs short for the sort (about 131,000 rows in).
 */
class CometCodegenMemoryPressureSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometCodegenAssertions {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.memory.offHeap.size", "16m")
      .set(CometConf.COMET_OFFHEAP_MEMORY_POOL_TYPE.key, "greedy_unified")

  test("a UDF feeding a native sort under memory pressure lets the sort spill") {
    val numRows = 160000
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      // 100-character strings that the UDF shrinks to 20 characters.
      spark
        .range(numRows)
        .selectExpr(
          "concat(lpad(cast(id AS string), 10, '0'), repeat('b', 10), repeat('a', 80)) AS s")
        .coalesce(1)
        .write
        .parquet(path)

      val df = spark.read
        .parquet(path)
        .select(regexp_replace(col("s"), "a", "").as("r"))
        .sortWithinPartitions("r")
      var rows: Array[Row] = Array.empty
      assertCodegenRan {
        rows = df.collect()
      }

      val plan = df.queryExecution.executedPlan
      val sorts = collect(plan) { case s: CometSortExec => s }
      assert(sorts.size == 1, s"expected one native sort:\n$plan")
      assert(sorts.head.metrics("spill_count").value > 0, "the native sort should have spilled")
      assert(rows.length == numRows)
      val expected = (0 until numRows).iterator.map(i => f"$i%010d" + "b" * 10)
      assert(rows.iterator.map(_.getString(0)).sameElements(expected))
    }
  }
}
