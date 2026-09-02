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

import org.apache.spark.sql.{CometTestBase, DataFrame, QueryTest}
import org.apache.spark.sql.comet.CometRowViewWriteFilesExec
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

/**
 * Tests for `spark.comet.exec.write.rowView.enabled`, which replaces Spark's `WriteFilesExec`
 * with a Comet node that drives Spark's own `OutputWriter` from Arrow batches instead of from
 * materialized `UnsafeRow`s.
 *
 * The bar for every case here is that enabling the config changes nothing observable except the
 * plan: the data written must match what the same Comet plan produces with the config off. The
 * baseline is deliberately the same plan rather than vanilla Spark, so any difference is
 * attributable to the write node and not to the scan.
 */
class CometWriteRowViewSuite extends CometTestBase {

  /**
   * The node replaces `WriteFilesExec` by extending `WriteFilesExecBase`, which Spark 4.0
   * introduced. On 3.4 / 3.5 `V1WritesUtils.getWriteFilesOpt` matches the concrete case class
   * instead, so the rewrite is declined there and there is nothing to assert.
   */
  private def testRowView(name: String)(f: => Unit): Unit = test(name) {
    assume(isSpark40Plus, "row view writes require Spark 4.0+")
    f
  }

  testRowView("row view write is off by default") {
    withParquetSource { source =>
      withTempPath { out =>
        val plan = captureWritePlan(source.write.mode("overwrite").parquet(out.toString))
        assert(countRowViewWrites(plan) == 0, s"row view write should be opt-in, got:\n$plan")
      }
    }
  }

  testRowView("row view write is used for an unpartitioned parquet write") {
    withParquetSource { source =>
      withTempPath { out =>
        val plan = captureWritePlan {
          withRowView(source.write.mode("overwrite").parquet(out.toString))
        }
        assert(
          countRowViewWrites(plan) == 1,
          s"expected a CometRowViewWriteFiles in the write plan, got:\n$plan")
      }
    }
  }

  testRowView("row view write is used for a dynamically partitioned write") {
    withParquetSource { source =>
      withTempPath { out =>
        val plan = captureWritePlan {
          withRowView(source.write.mode("overwrite").partitionBy("part").parquet(out.toString))
        }
        assert(
          countRowViewWrites(plan) == 1,
          s"expected a CometRowViewWriteFiles for the partitioned write, got:\n$plan")
      }
    }
  }

  testRowView("row view write is used for a bucketed write") {
    withParquetSource { source =>
      // `INSERT INTO` on a pre-created bucketed table rather than `bucketBy(...).saveAsTable`:
      // the latter nests the write inside a `SaveAsV1TableCommand` whose inner plan the query
      // execution listener does not surface, so the plan assertion could not be made.
      withTable("bucketed_row_view") {
        spark.sql("""
          CREATE TABLE bucketed_row_view (
            id BIGINT,
            struct_col STRUCT<a: INT, b: STRING>,
            arr_col ARRAY<INT>)
          USING parquet
          CLUSTERED BY (id) INTO 4 BUCKETS""")
        val projected = source.select("id", "struct_col", "arr_col")
        projected.createOrReplaceTempView("bucket_src")

        val plan = captureWritePlan {
          withRowView(
            spark.sql("INSERT INTO bucketed_row_view SELECT id, struct_col, arr_col FROM " +
              "bucket_src"))
        }
        assert(
          countRowViewWrites(plan) == 1,
          s"expected a CometRowViewWriteFiles for the bucketed write, got:\n$plan")
        checkAnswer(spark.table("bucketed_row_view"), projected)
      }
    }
  }

  testRowView("row view write is declined for a schema of only flat data columns") {
    withParquetSource { source =>
      withTempPath { out =>
        val flat = source.selectExpr("id", "int_col", "str_col", "date_col", "ts_col")
        val plan = captureWritePlan {
          withRowView(flat.write.mode("overwrite").parquet(out.toString))
        }
        assert(
          countRowViewWrites(plan) == 0,
          s"a flat schema is not worth the row view, got:\n$plan")
        checkAnswer(spark.read.parquet(out.toString), flat)
      }
    }
  }

  testRowView("row view write is used for a flat partitioned write") {
    withParquetSource { source =>
      // A partitioned write removes `BaseDynamicPartitionDataWriter.getOutputRow` as well as the
      // transition, which is worth doing even when every data column is flat. Measured at 5% on
      // this shape by `CometWriteRowViewBenchmark`, against a 1% noise floor.
      val flat = source.selectExpr("id", "int_col", "str_col", "part")
      withTempPath { out =>
        val plan = captureWritePlan {
          withRowView(flat.write.mode("overwrite").partitionBy("part").parquet(out.toString))
        }
        assert(
          countRowViewWrites(plan) == 1,
          s"a flat partitioned write still removes a projection, got:\n$plan")
      }
      assertSameWrite(
        flat,
        (df, path) => df.write.mode("overwrite").partitionBy("part").parquet(path),
        path => spark.read.schema(flat.schema).parquet(path).select(flat.columns.map(col): _*))
    }
  }

  testRowView("row view write is declined when concurrent output file writers are enabled") {
    withParquetSource { source =>
      withTempPath { out =>
        // Above 0, V1Writes plants no sort and FileFormatWriter picks
        // DynamicPartitionDataConcurrentWriter, whose spill path is typed on UnsafeRow.
        withSQLConf("spark.sql.maxConcurrentOutputFileWriters" -> "10") {
          val plan = captureWritePlan {
            withRowView(source.write.mode("overwrite").partitionBy("part").parquet(out.toString))
          }
          assert(
            countRowViewWrites(plan) == 0,
            s"concurrent output writers must decline the row view, got:\n$plan")
        }
        checkAnswer(
          spark.read
            .schema(source.schema)
            .parquet(out.toString)
            .select(source.columns.map(col): _*),
          source)
      }
    }
  }

  testRowView("row view write produces the same data - unpartitioned") {
    withParquetSource { source =>
      assertSameWrite(
        source,
        (df, path) => df.write.mode("overwrite").parquet(path),
        path => spark.read.schema(source.schema).parquet(path))
    }
  }

  testRowView("row view write produces the same data - dynamic partitions") {
    withParquetSource { source =>
      assertSameWrite(
        source,
        (df, path) => df.write.mode("overwrite").partitionBy("part").parquet(path),
        // `schema` pins the partition column back to string; reading a partitioned directory
        // otherwise infers `part` as int.
        path =>
          spark.read.schema(source.schema).parquet(path).select(source.columns.map(col): _*))
    }
  }

  testRowView("row view write produces the same data - two partition columns") {
    withParquetSource { source =>
      val partitioned = source.withColumn("part2", expr("cast(id % 7 as string)"))
      assertSameWrite(
        partitioned,
        (df, path) => df.write.mode("overwrite").partitionBy("part", "part2").parquet(path),
        path =>
          spark.read
            .schema(partitioned.schema)
            .parquet(path)
            .select(partitioned.columns.map(col): _*))
    }
  }

  testRowView("row view write produces the same data - deeply nested types with partitions") {
    withDeeplyNestedSource { source =>
      val partitioned = source.withColumn("part", expr("cast(id % 4 as string)"))
      assertSameWrite(
        partitioned,
        (df, path) => df.write.mode("overwrite").partitionBy("part").parquet(path),
        path =>
          spark.read
            .schema(partitioned.schema)
            .parquet(path)
            .select(partitioned.columns.map(col): _*))
    }
  }

  testRowView("row view write produces the same data - null and empty partition values") {
    // Empty strings become null partition directories via V1Writes' Empty2Null projection, and
    // nulls land in __HIVE_DEFAULT_PARTITION__. Both are computed from the row this node hands
    // to the writer, so they exercise the partition-value path rather than the payload.
    withNullPartitionSource { source =>
      assertSameWrite(
        source,
        (df, path) => df.write.mode("overwrite").partitionBy("part").parquet(path),
        path =>
          spark.read.schema(source.schema).parquet(path).select(source.columns.map(col): _*))
    }
  }

  testRowView("row view write produces the same data - maxRecordsPerFile across partitions") {
    withParquetSource { source =>
      Seq("100", "0").foreach { maxRecords =>
        withSQLConf("spark.sql.files.maxRecordsPerFile" -> maxRecords) {
          assertSameWrite(
            source,
            (df, path) => df.write.mode("overwrite").partitionBy("part").parquet(path),
            path =>
              spark.read.schema(source.schema).parquet(path).select(source.columns.map(col): _*))
        }
      }
    }
  }

  testRowView("row view write produces the same data - orc") {
    withParquetSource { source =>
      assertSameWrite(
        source,
        (df, path) => df.write.mode("overwrite").partitionBy("part").format("orc").save(path),
        path =>
          spark.read
            .schema(source.schema)
            .format("orc")
            .load(path)
            .select(source.columns.map(col): _*))
    }
  }

  testRowView("row view write produces the same partition layout") {
    withParquetSource { source =>
      withTempPath { baseline =>
        withTempPath { rowView =>
          source.write.mode("overwrite").partitionBy("part").parquet(baseline.toString)
          withRowView(
            source.write.mode("overwrite").partitionBy("part").parquet(rowView.toString))
          assert(
            partitionDirs(rowView.toString) == partitionDirs(baseline.toString),
            "partition directories differ")
        }
      }
    }
  }

  /**
   * Writes `source` with and without the row view write and requires the results to be identical,
   * both in content and in row count.
   */
  private def assertSameWrite(
      source: DataFrame,
      write: (DataFrame, String) => Unit,
      read: String => DataFrame): Unit = {
    withTempPath { baseline =>
      withTempPath { rowView =>
        write(source, baseline.toString)
        withRowView(write(source, rowView.toString))

        val expected = read(baseline.toString)
        val actual = read(rowView.toString)
        assert(actual.count() == expected.count(), "row count differs")
        QueryTest.checkAnswer(actual, expected.collect().toSeq)
      }
    }
  }

  private def partitionDirs(path: String): Set[String] = {
    val root = new java.io.File(path)
    Option(root.listFiles())
      .map(_.filter(_.isDirectory).map(_.getName).toSet)
      .getOrElse(Set.empty)
  }

  private def withRowView(f: => Unit): Unit =
    withSQLConf(CometConf.COMET_EXEC_WRITE_ROW_VIEW_ENABLED.key -> "true")(f)

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

  /** Partition column carrying nulls and empty strings alongside ordinary values. */
  private def withNullPartitionSource(f: DataFrame => Unit): Unit = {
    withTempPath { dir =>
      val df = spark
        .range(1000)
        .selectExpr(
          "id",
          "named_struct('a', cast(id as int), 'b', cast(id as string)) as struct_col",
          "array(cast(id as int)) as arr_col",
          """case when id % 3 = 0 then null
                  when id % 3 = 1 then ''
                  else cast(id % 5 as string) end as part""")
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

  /** `AdaptiveSparkPlanExec` is a leaf node, so a plain `foreach` stops at the AQE boundary. */
  private def flatten(plan: SparkPlan): Seq[SparkPlan] = plan match {
    case a: AdaptiveSparkPlanExec => a +: flatten(a.executedPlan)
    case p => p +: p.children.flatMap(flatten)
  }

  private def countRowViewWrites(plan: SparkPlan): Int =
    flatten(plan).count(_.isInstanceOf[CometRowViewWriteFilesExec])

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
