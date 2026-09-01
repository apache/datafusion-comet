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

import java.util.concurrent.TimeUnit

import scala.util.Random

import org.apache.spark.sql.{CometTestBase, DataFrame, QueryTest}
import org.apache.spark.sql.comet.CometColumnarToRowViewExec
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, SchemaGenOptions}

/**
 * Tests for `spark.comet.exec.write.rowView.enabled`, which replaces the UnsafeRow-materializing
 * columnar-to-row transition below a write with a zero-copy row view over the Arrow batch.
 *
 * The bar for every case here is that enabling the config changes nothing observable except the
 * plan: the bytes written must match what the same write produces without it.
 */
class CometWriteRowViewSuite extends CometTestBase {

  import testImplicits._

  test("row view is used for an unpartitioned parquet write") {
    withParquetSource { source =>
      withTempPath { out =>
        val plan = captureWritePlan {
          withRowView(source.write.mode("overwrite").parquet(out.toString))
        }
        assert(
          countRowViews(plan) == 1,
          s"expected a CometColumnarToRowView in the write plan, got:\n$plan")
      }
    }
  }

  test("row view is off by default") {
    withParquetSource { source =>
      withTempPath { out =>
        val plan = captureWritePlan(source.write.mode("overwrite").parquet(out.toString))
        assert(countRowViews(plan) == 0, s"row view should be opt-in, got:\n$plan")
      }
    }
  }

  test("row view is not used for a schema of only flat types") {
    withParquetSource { source =>
      withTempPath { out =>
        val flat = source.selectExpr("id", "int_col", "str_col", "date_col", "ts_col")
        val plan = captureWritePlan {
          withRowView(flat.write.mode("overwrite").parquet(out.toString))
        }
        assert(countRowViews(plan) == 0, s"a flat schema is not worth the row view, got:\n$plan")
        checkAnswer(spark.read.parquet(out.toString), flat)
      }
    }
  }

  test("row view is used when a complex type is nested below a flat top level") {
    withParquetSource { source =>
      withTempPath { out =>
        // The gate looks at top-level fields, so this asserts the common shape where the only
        // complex column sits alongside flat ones.
        val mixed = source.selectExpr("id", "str_col", "struct_col")
        val plan = captureWritePlan {
          withRowView(mixed.write.mode("overwrite").parquet(out.toString))
        }
        assert(countRowViews(plan) == 1, s"expected the row view, got:\n$plan")
        checkAnswer(spark.read.parquet(out.toString), mixed)
      }
    }
  }

  test("row view writes the same data - deeply nested types") {
    withDeeplyNestedSource { source =>
      withTempPath { out =>
        val plan = captureWritePlan {
          withRowView(source.write.mode("overwrite").parquet(out.toString))
        }
        assert(countRowViews(plan) == 1, s"expected the row view, got:\n$plan")
      }
      assertSameWrite(source, "parquet")
    }
  }

  test("row view writes the same data - primitives, strings and nulls") {
    withParquetSource { source =>
      assertSameWrite(source, "parquet")
    }
  }

  test("row view writes the same data - fuzz generated flat schema") {
    withFuzzSource(
      SchemaGenOptions(generateArray = false, generateStruct = false, generateMap = false)) {
      source => assertSameWrite(source, "parquet")
    }
  }

  test("row view writes the same data - fuzz generated nested schema") {
    withFuzzSource(
      SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = true)) {
      source => assertSameWrite(source, "parquet")
    }
  }

  test("row view writes the same data - orc and json") {
    withParquetSource { source =>
      assertSameWrite(source, "orc")
      assertSameWrite(source, "json")
    }
  }

  test("row view respects maxRecordsPerFile") {
    withParquetSource { source =>
      Seq("100", "0").foreach { maxRecords =>
        withTempPath { out =>
          withSQLConf("spark.sql.files.maxRecordsPerFile" -> maxRecords) {
            withRowView(source.write.mode("overwrite").parquet(out.toString))
          }
          checkAnswer(spark.read.parquet(out.toString), source)
        }
      }
    }
  }

  test("row view is not used for partitioned or bucketed writes") {
    withParquetSource { source =>
      withTempPath { out =>
        val plan = captureWritePlan {
          withRowView(source.write.mode("overwrite").partitionBy("part").parquet(out.toString))
        }
        assert(countRowViews(plan) == 0, s"partitioned write must not use the row view:\n$plan")
        // `schema` pins the partition column back to string; reading a partitioned directory
        // otherwise infers `part` as int.
        checkAnswer(spark.read.schema(source.schema).parquet(out.toString), source)
      }
    }

    withParquetSource { source =>
      withTable("bucketed") {
        val plan = captureWritePlan {
          withRowView(
            source.write
              .mode("overwrite")
              .bucketBy(4, "id")
              .format("parquet")
              .saveAsTable("bucketed"))
        }
        assert(countRowViews(plan) == 0, s"bucketed write must not use the row view:\n$plan")
        checkAnswer(spark.table("bucketed"), source)
      }
    }
  }

  /**
   * Writes `source` with and without the row view and requires the results to be identical, both
   * in content and in row count. The baseline is written by the same Comet plan with the config
   * off, so any difference is attributable to the transition and not to the scan.
   */
  private def assertSameWrite(source: DataFrame, format: String): Unit = {
    withTempPath { baseline =>
      withTempPath { rowView =>
        source.write.mode("overwrite").format(format).save(baseline.toString)
        withRowView(source.write.mode("overwrite").format(format).save(rowView.toString))

        val expected = spark.read.schema(source.schema).format(format).load(baseline.toString)
        val actual = spark.read.schema(source.schema).format(format).load(rowView.toString)
        assert(actual.count() == expected.count(), s"row count differs for $format")
        QueryTest.checkAnswer(actual, expected.collect().toSeq)
      }
    }
  }

  private def withRowView[T](f: => T): T =
    withSQLConf(CometConf.COMET_WRITE_ROW_VIEW_ENABLED.key -> "true")(f)

  /**
   * Materializes a small table on disk and hands back a DataFrame reading it, so the write under
   * test is fed by a Comet columnar scan rather than a row-based local relation.
   */
  private def withParquetSource(f: DataFrame => Unit): Unit = {
    withTempPath { dir =>
      val df = spark
        .range(2000)
        .selectExpr(
          "id",
          "cast(id as int) as int_col",
          "cast(id as short) as short_col",
          "cast(id % 2 as boolean) as bool_col",
          "cast(id as double) as double_col",
          "cast(id as decimal(20,4)) as dec_col",
          "cast(id as string) as str_col",
          "case when id % 7 = 0 then null else concat('v_', cast(id as string)) end as null_str",
          "cast(cast(id as string) as binary) as bin_col",
          "date_add(to_date('2024-01-01'), cast(id % 365 as int)) as date_col",
          "timestamp_micros(id * 1000000) as ts_col",
          "named_struct('a', cast(id as int), 'b', cast(id as string)) as struct_col",
          "array(cast(id as int), cast(id + 1 as int)) as arr_col",
          "map('k', cast(id as string)) as map_col",
          "cast(id % 3 as string) as part")
      df.write.mode("overwrite").parquet(dir.toString)
      f(spark.read.parquet(dir.toString))
    }
  }

  /** Four levels of nesting, mixing struct, array and map at each level. */
  private def withDeeplyNestedSource(f: DataFrame => Unit): Unit = {
    withTempPath { dir =>
      val df = spark
        .range(1000)
        .selectExpr(
          "id",
          """named_struct(
               'l1', named_struct(
                 'l2', named_struct(
                   'l3', named_struct('v', cast(id as int), 'n', concat('x_', cast(id as string))),
                   'arr', array(cast(id as int), cast(id + 1 as int))),
                 'c', cast(id % 100 as int)),
               'id', id) as deep_struct""",
          """array(
               named_struct('id', cast(id as int),
                            'tags', array(concat('t_', cast(id as string)))),
               named_struct('id', cast(id + 1 as int), 'tags', array('t_x', 't_y'))
             ) as arr_of_structs""",
          """map('k', array(named_struct('a', cast(id as int),
                                        'b', cast(id as string)))) as map_of_arr_structs""",
          // A null at every nesting level, which is where the offset bookkeeping differs most.
          "case when id % 5 = 0 then null else array(array(cast(id as int))) end as nested_null")
      df.write.mode("overwrite").parquet(dir.toString)
      f(spark.read.parquet(dir.toString))
    }
  }

  private def withFuzzSource(schemaOptions: SchemaGenOptions)(f: DataFrame => Unit): Unit = {
    withTempPath { dir =>
      val schema = FuzzDataGenerator.generateSchema(schemaOptions)
      val df = FuzzDataGenerator.generateDataFrame(
        new Random(42),
        spark,
        schema,
        1000,
        DataGenOptions(generateNegativeZero = false))
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        df.write.mode("overwrite").parquet(dir.toString)
      }
      f(spark.read.parquet(dir.toString))
    }
  }

  /** `AdaptiveSparkPlanExec` is a leaf node, so a plain `foreach` stops at the AQE boundary. */
  private def flatten(plan: SparkPlan): Seq[SparkPlan] = plan match {
    case a: AdaptiveSparkPlanExec => a +: flatten(a.executedPlan)
    case p => p +: p.children.flatMap(flatten)
  }

  private def countRowViews(plan: SparkPlan): Int =
    flatten(plan).count(_.isInstanceOf[CometColumnarToRowViewExec])

  private def captureWritePlan(writeOp: => Unit): SparkPlan = {
    @volatile var capturedPlan: Option[QueryExecution] = None
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        capturedPlan = Some(qe)
      override def onFailure(
          funcName: String,
          qe: QueryExecution,
          exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)
    try {
      writeOp
      // The listener fires asynchronously off the listener bus, which is not reachable from this
      // package, so poll for it.
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
      while (capturedPlan.isEmpty && System.nanoTime() < deadline) {
        Thread.sleep(50)
      }
      assert(capturedPlan.isDefined, "no execution plan captured for the write")
      capturedPlan.get.executedPlan
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }
}
