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

package org.apache.comet.exec

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase

import org.apache.comet.CometConf

/**
 * Exercises the off-heap memory pools' check of real native memory usage.
 *
 * The budget is `spark.memory.offHeap.size`, which Spark fixes when the session starts, so these
 * tests need a session of their own rather than a `withSQLConf` override. The size below is
 * smaller than the memory Comet's native code already holds before a query runs, so every
 * reservation crosses the budget and the first one is refused once enforcement is turned on.
 * Deliberately not achieved by lowering `spark.comet.exec.memoryPool.fraction`: that bounds what
 * Comet may reserve, not what the check measures.
 */
class CometMemoryPoolNativeUsageSuite extends CometTestBase {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", "2m")
    conf
  }

  /** A sort, so that an operator actually reserves. */
  private def sortSmallInput(): Unit =
    spark.range(0, 1000).selectExpr("id", "id % 7 AS m").sort("m", "id").collect()

  private def failureMessages(run: => Unit): Seq[String] =
    causeChain(intercept[Throwable](run)).map(t => s"${t.getClass.getName}: ${t.getMessage}")

  test("off-heap pools deny reservations once real native usage exceeds the off-heap size") {
    Seq("fair_unified", "greedy_unified").foreach { poolType =>
      withSQLConf(
        CometConf.COMET_OFFHEAP_MEMORY_POOL_ENFORCE_NATIVE_USAGE.key -> "true",
        CometConf.COMET_OFFHEAP_MEMORY_POOL_TYPE.key -> poolType) {
        val messages = failureMessages(sortSmallInput())
        assert(
          messages.exists(_.contains("native memory in use is")),
          s"expected $poolType to deny the reservation on real native usage, but got:\n  " +
            messages.mkString("\n  "))
      }
    }
  }

  test("the check only observes by default") {
    // The same query under the same off-heap size, so enforcement is the only difference. It is
    // too small for the sort either way, which is what makes the budget bite in the test above;
    // what changes here is who refuses the reservation. Left at its default the check only logs,
    // so the reservation is not held back in Comet and reaches Spark's ledger, which fails it.
    withSQLConf(CometConf.COMET_OFFHEAP_MEMORY_POOL_TYPE.key -> "greedy_unified") {
      val messages = failureMessages(sortSmallInput())
      assert(
        !messages.exists(_.contains("native memory in use is")),
        "the check is not enforcing by default but still refused the reservation:\n  " +
          messages.mkString("\n  "))
      assert(
        messages.exists(_.contains("failed to acquire")),
        "expected the reservation to reach Spark's ledger, but got:\n  " +
          messages.mkString("\n  "))
    }
  }
}
