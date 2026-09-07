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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.Uuid
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.col

/**
 * Bit-for-bit comparisons of seeded `uuid` against Spark.
 *
 * The SQL `uuid(seed)` form only exists in Spark 4.0+, so `uuid_with_seed.sql` carries a
 * `MinSparkVersion: 4.0` marker and is skipped on 3.4 and 3.5. Unseeded `uuid()` cannot be
 * compared across engines. That left the Rust golden constants in
 * `nondetermenistic_funcs/uuid.rs` as the only guard on those profiles, and their provenance is
 * not checkable from the repo -- if they were ever regenerated from the Rust side the test would
 * become circular.
 *
 * `Uuid(Some(seed))` is constructible from Scala on every supported version even where the SQL
 * form is not, so building the expression directly gives all profiles a real cross-engine
 * assertion and removes that dependence.
 */
class CometUuidExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("seeded uuid matches Spark bit for bit") {
    withParquetTable((0 until 20).map(i => (i, i.toString)), "tbl") {
      Seq(0L, -1L, 42L, Long.MinValue, Long.MaxValue).foreach { seed =>
        val df = spark.table("tbl").select(getColumnFromExpression(Uuid(Some(seed))))
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("seeded uuid matches Spark bit for bit across multiple partitions") {
    // Exercises the `seed + partitionIndex` offset that RandomUUIDGenerator applies. A
    // single-partition run agrees with Spark even if the offset were dropped entirely, so
    // repartition first and assert the partition count rather than assuming it.
    withParquetTable((0 until 64).map(i => (i, i.toString)), "tbl") {
      val repartitioned = spark.table("tbl").repartition(4, col("_1"))
      assert(repartitioned.rdd.getNumPartitions > 1)
      Seq(0L, 42L).foreach { seed =>
        checkSparkAnswerAndOperator(
          repartitioned.select(getColumnFromExpression(Uuid(Some(seed)))))
      }
    }
  }
}
