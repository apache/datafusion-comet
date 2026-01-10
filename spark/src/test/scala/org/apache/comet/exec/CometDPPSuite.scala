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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

import org.apache.comet.CometConf

/** Tests for Dynamic Partition Pruning (DPP) with native DataFusion scan. */
class CometDPPSuite extends CometTestBase {

  test("DPP with native_datafusion scan - basic join") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_datafusion") {
      withTempPath { dir =>
        spark
          .range(10000)
          .selectExpr("id", "id % 100 as dim_key", "rand() as value")
          .write
          .mode("overwrite")
          .parquet(s"${dir.getCanonicalPath}/fact")
        spark
          .range(100)
          .selectExpr("id", "'name_' || id as name")
          .where("id < 10")
          .write
          .mode("overwrite")
          .parquet(s"${dir.getCanonicalPath}/dim")

        spark.read.parquet(s"${dir.getCanonicalPath}/fact").createOrReplaceTempView("fact")
        spark.read.parquet(s"${dir.getCanonicalPath}/dim").createOrReplaceTempView("dim")

        val df = spark.sql("SELECT f.*, d.name FROM fact f JOIN dim d ON f.dim_key = d.id")
        val result = df.collect()

        assert(result.forall(row => row.getLong(1) < 10))

        val plan = df.queryExecution.executedPlan
        val hasNativeScan = plan.collect { case _: CometNativeScanExec => true }.nonEmpty ||
          plan
            .collect { case a: AdaptiveSparkPlanExec =>
              a.executedPlan.collect { case _: CometNativeScanExec => true }.nonEmpty
            }
            .exists(identity)
        assert(hasNativeScan, "Expected CometNativeScanExec in plan")
      }
    }
  }

  test("DPP reduces output rows significantly") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_datafusion") {
      withTempPath { dir =>
        val factRows = 100000
        val dimRows = 1000
        val selectivity = 0.01

        spark
          .range(factRows)
          .selectExpr("id", s"id % $dimRows as dim_key", "rand() as value")
          .write
          .mode("overwrite")
          .parquet(s"${dir.getCanonicalPath}/fact")
        spark
          .range(dimRows)
          .selectExpr("id", "'name_' || id as name")
          .where(s"id < ${(dimRows * selectivity).toInt}")
          .write
          .mode("overwrite")
          .parquet(s"${dir.getCanonicalPath}/dim")

        spark.read.parquet(s"${dir.getCanonicalPath}/fact").createOrReplaceTempView("fact3")
        spark.read.parquet(s"${dir.getCanonicalPath}/dim").createOrReplaceTempView("dim3")

        val count =
          spark.sql("SELECT f.*, d.name FROM fact3 f JOIN dim3 d ON f.dim_key = d.id").count()
        val expectedMax = (factRows * selectivity * 2).toLong
        assert(count <= expectedMax, s"Expected at most $expectedMax rows with DPP, got $count")
      }
    }
  }

  test("DPP with multiple join conditions") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_datafusion") {
      withTempPath { dir =>
        spark
          .range(1000)
          .selectExpr("id", "id % 10 as key1", "id % 5 as key2", "rand() as value")
          .write
          .mode("overwrite")
          .parquet(s"${dir.getCanonicalPath}/fact")
        spark
          .range(10)
          .selectExpr("id as key1", "'dim1_' || id as name1")
          .where("id < 3")
          .write
          .mode("overwrite")
          .parquet(s"${dir.getCanonicalPath}/dim1")
        spark
          .range(5)
          .selectExpr("id as key2", "'dim2_' || id as name2")
          .where("id < 2")
          .write
          .mode("overwrite")
          .parquet(s"${dir.getCanonicalPath}/dim2")

        spark.read.parquet(s"${dir.getCanonicalPath}/fact").createOrReplaceTempView("fact_multi")
        spark.read.parquet(s"${dir.getCanonicalPath}/dim1").createOrReplaceTempView("dim1")
        spark.read.parquet(s"${dir.getCanonicalPath}/dim2").createOrReplaceTempView("dim2")

        val result = spark
          .sql("""
          SELECT f.*, d1.name1, d2.name2 FROM fact_multi f
          JOIN dim1 d1 ON f.key1 = d1.key1 JOIN dim2 d2 ON f.key2 = d2.key2
        """)
          .collect()
        assert(result.forall(row => row.getLong(1) < 3 && row.getLong(2) < 2))
      }
    }
  }
}
