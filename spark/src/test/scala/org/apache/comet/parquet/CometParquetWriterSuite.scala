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

import scala.util.Random

import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.comet.{CometBatchScanExec, CometNativeScanExec, CometNativeWriteExec, CometScanExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, SchemaGenOptions}

class CometParquetWriterSuite extends CometTestBase {

  import testImplicits._

  test("basic parquet write") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create test data and write it to a temp parquet file first
      withTempPath { inputDir =>
        val inputPath = createTestData(inputDir)

        withSQLConf(
          CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax",
          CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true") {

          writeWithCometNativeWriteExec(inputPath, outputPath)

          verifyWrittenFile(outputPath)
        }
      }
    }
  }

  test("basic parquet write with native scan child") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create test data and write it to a temp parquet file first
      withTempPath { inputDir =>
        val inputPath = createTestData(inputDir)

        withSQLConf(
          CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax",
          CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true") {

          withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_datafusion") {
            val capturedPlan = writeWithCometNativeWriteExec(inputPath, outputPath)
            capturedPlan.foreach { plan =>
              val hasNativeScan = plan.exists {
                case _: CometNativeScanExec => true
                case _ => false
              }

              assert(
                hasNativeScan,
                s"Expected CometNativeScanExec in the plan, but got:\n${plan.treeString}")
            }

            verifyWrittenFile(outputPath)
          }
        }
      }
    }
  }

  test("basic parquet write with repartition") {
    withTempPath { dir =>
      // Create test data and write it to a temp parquet file first
      withTempPath { inputDir =>
        val inputPath = createTestData(inputDir)
        Seq(true, false).foreach(adaptive => {
          // Create a new output path for each AQE value
          val outputPath = new File(dir, s"output_aqe_$adaptive.parquet").getAbsolutePath

          withSQLConf(
            CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
            "spark.sql.adaptive.enabled" -> adaptive.toString,
            SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax",
            CometConf.getOperatorAllowIncompatConfigKey(
              classOf[DataWritingCommandExec]) -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true") {

            writeWithCometNativeWriteExec(inputPath, outputPath, Some(10))
            verifyWrittenFile(outputPath)
          }
        })
      }
    }
  }

  test("parquet write with array type") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = Seq((1, Seq(1, 2, 3)), (2, Seq(4, 5)), (3, Seq[Int]()), (4, Seq(6, 7, 8, 9)))
        .toDF("id", "values")

      writeComplexTypeData(df, outputPath, 4)
    }
  }

  test("parquet write with struct type") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df =
        Seq((1, ("Alice", 30)), (2, ("Bob", 25)), (3, ("Charlie", 35))).toDF("id", "person")

      writeComplexTypeData(df, outputPath, 3)
    }
  }

  test("parquet write with map type") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = Seq(
        (1, Map("a" -> 1, "b" -> 2)),
        (2, Map("c" -> 3)),
        (3, Map[String, Int]()),
        (4, Map("d" -> 4, "e" -> 5, "f" -> 6))).toDF("id", "properties")

      writeComplexTypeData(df, outputPath, 4)
    }
  }

  test("parquet write with array of structs") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = Seq(
        (1, Seq(("Alice", 30), ("Bob", 25))),
        (2, Seq(("Charlie", 35))),
        (3, Seq[(String, Int)]())).toDF("id", "people")

      writeComplexTypeData(df, outputPath, 3)
    }
  }

  test("parquet write with struct containing array") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = spark.sql("""
        SELECT
          1 as id,
          named_struct('name', 'Team A', 'scores', array(95, 87, 92)) as team
        UNION ALL SELECT
          2 as id,
          named_struct('name', 'Team B', 'scores', array(88, 91)) as team
        UNION ALL SELECT
          3 as id,
          named_struct('name', 'Team C', 'scores', array(100)) as team
      """)

      writeComplexTypeData(df, outputPath, 3)
    }
  }

  test("parquet write with map with struct values") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = spark.sql("""
        SELECT
          1 as id,
          map('emp1', named_struct('name', 'Alice', 'age', 30),
              'emp2', named_struct('name', 'Bob', 'age', 25)) as employees
        UNION ALL SELECT
          2 as id,
          map('emp3', named_struct('name', 'Charlie', 'age', 35)) as employees
      """)

      writeComplexTypeData(df, outputPath, 2)
    }
  }

  test("parquet write with deeply nested types") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create deeply nested structure: array of maps containing arrays
      val df = spark.sql("""
        SELECT
          1 as id,
          array(
            map('key1', array(1, 2, 3), 'key2', array(4, 5)),
            map('key3', array(6, 7, 8, 9))
          ) as nested_data
        UNION ALL SELECT
          2 as id,
          array(
            map('key4', array(10, 11))
          ) as nested_data
      """)

      writeComplexTypeData(df, outputPath, 2)
    }
  }

  test("parquet write with nullable complex types") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Test nulls at various levels
      val df = spark.sql("""
        SELECT
          1 as id,
          array(1, null, 3) as arr_with_nulls,
          named_struct('a', 1, 'b', cast(null as int)) as struct_with_nulls,
          map('x', 1, 'y', cast(null as int)) as map_with_nulls
        UNION ALL SELECT
          2 as id,
          cast(null as array<int>) as arr_with_nulls,
          cast(null as struct<a:int, b:int>) as struct_with_nulls,
          cast(null as map<string, int>) as map_with_nulls
        UNION ALL SELECT
          3 as id,
          array(4, 5, 6) as arr_with_nulls,
          named_struct('a', 7, 'b', 8) as struct_with_nulls,
          map('z', 9) as map_with_nulls
      """)

      writeComplexTypeData(df, outputPath, 3)
    }
  }

  test("parquet write with decimal types within complex types") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = spark.sql("""
        SELECT
          1 as id,
          array(cast(1.23 as decimal(10,2)), cast(4.56 as decimal(10,2))) as decimal_arr,
          named_struct('amount', cast(99.99 as decimal(10,2))) as decimal_struct,
          map('price', cast(19.99 as decimal(10,2))) as decimal_map
        UNION ALL SELECT
          2 as id,
          array(cast(7.89 as decimal(10,2))) as decimal_arr,
          named_struct('amount', cast(0.01 as decimal(10,2))) as decimal_struct,
          map('price', cast(0.50 as decimal(10,2))) as decimal_map
      """)

      writeComplexTypeData(df, outputPath, 2)
    }
  }

  test("parquet write with temporal types within complex types") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = spark.sql("""
        SELECT
          1 as id,
          array(date'2024-01-15', date'2024-02-20') as date_arr,
          named_struct('ts', timestamp'2024-01-15 10:30:00') as ts_struct,
          map('event', timestamp'2024-03-01 14:00:00') as ts_map
        UNION ALL SELECT
          2 as id,
          array(date'2024-06-30') as date_arr,
          named_struct('ts', timestamp'2024-07-04 12:00:00') as ts_struct,
          map('event', timestamp'2024-12-25 00:00:00') as ts_map
      """)

      writeComplexTypeData(df, outputPath, 2)
    }
  }

  test("parquet write with empty arrays and maps") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = Seq(
        (1, Seq[Int](), Map[String, Int]()),
        (2, Seq(1, 2), Map("a" -> 1)),
        (3, Seq[Int](), Map[String, Int]())).toDF("id", "arr", "mp")

      writeComplexTypeData(df, outputPath, 3)
    }
  }

  // ignored: native_comet scan is no longer supported
  ignore("native write falls back when scan produces non-Arrow data") {
    // This test verifies that when a native scan (like native_comet) doesn't support
    // certain data types (complex types), the native write correctly falls back to Spark
    // instead of failing at runtime with "Comet execution only takes Arrow Arrays" error.
    withTempPath { dir =>
      val inputPath = new File(dir, "input.parquet").getAbsolutePath
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create data with complex types and write without Comet
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = Seq((1, Seq(1, 2, 3)), (2, Seq(4, 5)), (3, Seq(6, 7, 8, 9)))
          .toDF("id", "values")
        df.write.parquet(inputPath)
      }

      // With native Parquet write enabled but using native_comet scan which doesn't
      // support complex types, the scan falls back to Spark. The native write should
      // detect this and also fall back to Spark instead of failing at runtime.
      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        // Use native_comet which doesn't support complex types
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_comet") {

        val plan =
          captureWritePlan(path => spark.read.parquet(inputPath).write.parquet(path), outputPath)

        // Verify NO CometNativeWriteExec in the plan (should have fallen back to Spark)
        val hasNativeWrite = plan.exists {
          case _: CometNativeWriteExec => true
          case d: DataWritingCommandExec =>
            d.child.exists(_.isInstanceOf[CometNativeWriteExec])
          case _ => false
        }

        assert(
          !hasNativeWrite,
          "Expected fallback to Spark write (no CometNativeWriteExec), but found native write " +
            s"in plan:\n${plan.treeString}")

        // Verify the data was written correctly
        val result = spark.read.parquet(outputPath).collect()
        assert(result.length == 3, "Expected 3 rows to be written")
      }
    }
  }

  test("parquet write complex types fuzz test") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Generate test data with complex types enabled
      val schema = FuzzDataGenerator.generateSchema(
        SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = true))
      val df = FuzzDataGenerator.generateDataFrame(
        new Random(42),
        spark,
        schema,
        500,
        DataGenOptions(generateNegativeZero = false))

      writeComplexTypeData(df, outputPath, 500)
    }
  }

// NATIVE COMET WRITER TESTS WHICH FAIL IN SPARK
  // https://github.com/apache/datafusion-comet/issues/3417
  ignore("Spark compat: empty file should be skipped while write to file") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTempPath { path =>
        spark.range(100).repartition(10).where("id = 50").write.parquet(path.toString)
        val partFiles = path
          .listFiles()
          .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
        assert(partFiles.length === 2)
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3418
  ignore("Spark compat: SPARK-33901 ctas should not change table's schema") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTable("t1", "t2") {
        sql("CREATE TABLE t1(i CHAR(5), c VARCHAR(4)) USING parquet")
        sql("CREATE TABLE t2 USING parquet AS SELECT * FROM t1")
        checkAnswer(
          sql("desc t2").selectExpr("data_type").where("data_type like '%char%'"),
          Seq(Row("char(5)"), Row("varchar(4)")))
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3419
  ignore("Spark compat: SPARK-37160 CREATE TABLE AS SELECT with CHAR_AS_VARCHAR") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTable("t1", "t2") {
        sql("CREATE TABLE t1(col CHAR(5)) USING parquet")
        withSQLConf(SQLConf.CHAR_AS_VARCHAR.key -> "true") {
          sql("CREATE TABLE t2 USING parquet AS SELECT * FROM t1")
          checkAnswer(
            sql("desc t2").selectExpr("data_type").where("data_type like '%char%'"),
            Seq(Row("varchar(5)")))
        }
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3420
  ignore("Spark compat: SPARK-29174 Support LOCAL in INSERT OVERWRITE DIRECTORY") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTempPath { dir =>
        val path = dir.toURI.getPath
        withTable("tab1", "tab2") {
          sql(s"""create table tab1 (a int) using parquet location '$path'""")
          sql("insert into tab1 values(1)")
          checkAnswer(sql("select * from tab1"), Seq(Row(1)))
          sql("create table tab2 (a int) using parquet")
          sql("insert into tab2 values(2)")
          sql(s"""insert overwrite local directory '$path' using parquet select * from tab2""")
          sql("refresh table tab1")
          checkAnswer(sql("select * from tab1"), Seq(Row(2)))
        }
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3421
  ignore("Spark compat: SPARK-38336 INSERT INTO with default columns positive tests") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTable("t") {
        sql("create table t(i boolean, s bigint) using parquet")
        sql("insert into t(i) values(true)")
        checkAnswer(spark.table("t"), Row(true, null))
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3422
  ignore("Spark compat: SPARK-38811 INSERT INTO on ALTER TABLE ADD COLUMNS positive tests") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTable("t") {
        sql("create table t(i boolean) using parquet")
        sql("alter table t add column s string default concat('abc', 'def')")
        sql("insert into t values(true, default)")
        checkAnswer(spark.table("t"), Row(true, "abcdef"))
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3423
  ignore("Spark compat: SPARK-43071 INSERT INTO from non-projection queries") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTable("t1", "t2") {
        sql("create table t1(i boolean, s bigint default 42) using parquet")
        sql("insert into t1 values (true, 41), (false, default)")
        sql("create table t2(i boolean, s bigint) using parquet")
        sql("insert into t2 select * from t1 order by s")
        checkAnswer(spark.table("t2"), Seq(Row(true, 41), Row(false, 42)))
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3424
  ignore("Spark compat: Insert overwrite table command should output correct schema basic") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTable("tbl", "tbl2") {
        withView("view1") {
          val df = spark.range(10).toDF("id")
          df.write.format("parquet").saveAsTable("tbl")
          spark.sql("CREATE VIEW view1 AS SELECT id FROM tbl")
          spark.sql("CREATE TABLE tbl2(ID long) USING parquet")
          spark.sql("INSERT OVERWRITE TABLE tbl2 SELECT ID FROM view1")
          checkAnswer(spark.table("tbl2"), (0 until 10).map(Row(_)))
        }
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3425
  ignore("Spark compat: parquet timestamp conversion") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTempPath { dir =>
        spark
          .range(1)
          .selectExpr("current_timestamp() as ts")
          .write
          .parquet(dir.toString + "/spark")
        val result = spark.read.parquet(dir.toString + "/spark").collect()
        assert(result.length == 1)
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3426
  ignore("Spark compat: INSERT INTO TABLE - complex type but different names") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTable("tab1", "tab2") {
        sql("""CREATE TABLE tab1 (s struct<a: string, b: string>) USING parquet""")
        sql("""CREATE TABLE tab2 (s struct<c: string, d: string>) USING parquet""")
        sql("INSERT INTO tab1 VALUES (named_struct('a', 'x', 'b', 'y'))")
        sql("INSERT INTO tab2 SELECT * FROM tab1")
        checkAnswer(spark.table("tab2"), Row(Row("x", "y")))
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3427
  ignore("Spark compat: Write Spark version into Parquet metadata") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTempPath { dir =>
        spark.range(1).repartition(1).write.parquet(dir.getAbsolutePath)
        val files = dir.listFiles().filter(_.getName.endsWith(".parquet"))
        assert(files.nonEmpty, "Expected parquet files to be written")
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3428
  ignore("Spark compat: write path implements onTaskCommit API correctly") {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        spark.range(10).repartition(10).write.mode("overwrite").parquet(path)
        val files = new File(path).listFiles().filter(_.getName.startsWith("part-"))
        assert(files.length > 0, "Expected part files to be written")
      }
    }
  }

  // COMET NATIVE WRITER Spark 4.x test failures
  // https://github.com/apache/datafusion-comet/issues/3429
  ignore("Spark compat: ctas with union") {
    assume(isSpark40Plus)
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTable("t") {
        sql("CREATE TABLE t USING parquet AS SELECT 1 AS c UNION ALL SELECT 2")
        checkAnswer(spark.table("t"), Seq(Row(1), Row(2)))
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3430
  ignore("Spark compat: SPARK-48817 test multi insert") {
    assume(isSpark40Plus)
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true") {
      withTable("t1", "t2") {
        sql("CREATE TABLE t1(a INT) USING parquet")
        sql("CREATE TABLE t2(a INT) USING parquet")
        sql("FROM (SELECT 1 AS a) src INSERT INTO t1 SELECT a INSERT INTO t2 SELECT a")
        checkAnswer(spark.table("t1"), Row(1))
        checkAnswer(spark.table("t2"), Row(1))
      }
    }
  }

  private def createTestData(inputDir: File): String = {
    val inputPath = new File(inputDir, "input.parquet").getAbsolutePath
    val schema = FuzzDataGenerator.generateSchema(
      SchemaGenOptions(generateArray = false, generateStruct = false, generateMap = false))
    val df = FuzzDataGenerator.generateDataFrame(
      new Random(42),
      spark,
      schema,
      1000,
      DataGenOptions(generateNegativeZero = false))
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Denver") {
      df.write.parquet(inputPath)
    }
    inputPath
  }

  /**
   * Captures the execution plan during a write operation.
   *
   * @param writeOp
   *   The write operation to execute (takes output path as parameter)
   * @param outputPath
   *   The path to write to
   * @return
   *   The captured execution plan
   */
  private def captureWritePlan(writeOp: String => Unit, outputPath: String): SparkPlan = {
    var capturedPlan: Option[QueryExecution] = None

    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        if (funcName == "save" || funcName.contains("command")) {
          capturedPlan = Some(qe)
        }
      }

      override def onFailure(
          funcName: String,
          qe: QueryExecution,
          exception: Exception): Unit = {}
    }

    spark.listenerManager.register(listener)

    try {
      writeOp(outputPath)

      // Wait for listener to be called with timeout
      val maxWaitTimeMs = 15000
      val checkIntervalMs = 100
      val maxIterations = maxWaitTimeMs / checkIntervalMs
      var iterations = 0

      while (capturedPlan.isEmpty && iterations < maxIterations) {
        Thread.sleep(checkIntervalMs)
        iterations += 1
      }

      assert(
        capturedPlan.isDefined,
        s"Listener was not called within ${maxWaitTimeMs}ms - no execution plan captured")

      stripAQEPlan(capturedPlan.get.executedPlan)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  private def assertHasCometNativeWriteExec(plan: SparkPlan): Unit = {
    var nativeWriteCount = 0
    plan.foreach {
      case _: CometNativeWriteExec =>
        nativeWriteCount += 1
      case d: DataWritingCommandExec =>
        d.child.foreach {
          case _: CometNativeWriteExec =>
            nativeWriteCount += 1
          case _ =>
        }
      case _ =>
    }

    assert(
      nativeWriteCount == 1,
      s"Expected exactly one CometNativeWriteExec in the plan, but found $nativeWriteCount:\n${plan.treeString}")
  }

  private def writeWithCometNativeWriteExec(
      inputPath: String,
      outputPath: String,
      num_partitions: Option[Int] = None): Option[SparkPlan] = {
    val df = spark.read.parquet(inputPath)

    val plan = captureWritePlan(
      path => num_partitions.fold(df)(n => df.repartition(n)).write.parquet(path),
      outputPath)

    assertHasCometNativeWriteExec(plan)

    Some(plan)
  }

  private def verifyWrittenFile(outputPath: String): Unit = {
    // Verify the data was written correctly
    val resultDf = spark.read.parquet(outputPath)
    assert(resultDf.count() == 1000, "Expected 1000 rows to be written")

    // Verify multiple part files were created
    val outputDir = new File(outputPath)
    val partFiles = outputDir.listFiles().filter(_.getName.startsWith("part-"))
    // With 1000 rows and default parallelism, we should get multiple partitions
    assert(partFiles.length > 1, "Expected multiple part files to be created")

    // read with and without Comet and compare
    val sparkRows = readSparkRows(outputPath)
    val cometRows = readCometRows(outputPath)
    val schema = spark.read.parquet(outputPath).schema
    compareRows(schema, sparkRows, cometRows)
  }

  private def writeComplexTypeData(
      inputDf: DataFrame,
      outputPath: String,
      expectedRows: Int): Unit = {
    withTempPath { inputDir =>
      val inputPath = new File(inputDir, "input.parquet").getAbsolutePath

      // First write the input data without Comet
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "false",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Denver") {
        inputDf.write.parquet(inputPath)
      }

      // read the generated Parquet file and write with Comet native writer
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        // enable experimental native writes
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        // explicitly set scan impl to override CI defaults
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> "auto",
        // Disable unsigned small int safety check for ShortType columns
        CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key -> "false",
        // use a different timezone to make sure that timezone handling works with nested types
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax") {

        val parquetDf = spark.read.parquet(inputPath)

        // Capture plan and verify CometNativeWriteExec is used
        val plan = captureWritePlan(path => parquetDf.write.parquet(path), outputPath)
        assertHasCometNativeWriteExec(plan)
      }

      // Verify round-trip: read with Spark and Comet, compare results
      val sparkRows = readSparkRows(outputPath)
      val cometRows = readCometRows(outputPath)
      assert(sparkRows.length == expectedRows, s"Expected $expectedRows rows")
      val schema = spark.read.parquet(outputPath).schema
      compareRows(schema, sparkRows, cometRows)
    }
  }

  private def compareRows(
      schema: StructType,
      sparkRows: Array[Row],
      cometRows: Array[Row]): Unit = {
    import scala.jdk.CollectionConverters._
    // Convert collected rows back to DataFrames for checkAnswer
    val sparkDf = spark.createDataFrame(sparkRows.toSeq.asJava, schema)
    val cometDf = spark.createDataFrame(cometRows.toSeq.asJava, schema)
    checkAnswer(sparkDf, cometDf)
  }

  private def hasCometScan(plan: SparkPlan): Boolean = {
    stripAQEPlan(plan).exists {
      case _: CometScanExec => true
      case _: CometNativeScanExec => true
      case _: CometBatchScanExec => true
      case _ => false
    }
  }

  private def hasSparkScan(plan: SparkPlan): Boolean = {
    stripAQEPlan(plan).exists {
      case _: FileSourceScanExec => true
      case _ => false
    }
  }

  private def readSparkRows(path: String): Array[Row] = {
    var rows: Array[Row] = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val df = spark.read.parquet(path)
      val plan = df.queryExecution.executedPlan
      assert(
        hasSparkScan(plan) && !hasCometScan(plan),
        s"Expected Spark scan (not Comet) when COMET_ENABLED=false:\n${plan.treeString}")
      rows = df.collect()
    }
    rows
  }

  private def readCometRows(path: String): Array[Row] = {
    var rows: Array[Row] = null
    withSQLConf(
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
      // Override CI setting to use a scan impl that supports complex types
      CometConf.COMET_NATIVE_SCAN_IMPL.key -> "auto") {
      val df = spark.read.parquet(path)
      val plan = df.queryExecution.executedPlan
      assert(
        hasCometScan(plan),
        s"Expected Comet scan when COMET_NATIVE_SCAN_ENABLED=true:\n${plan.treeString}")
      rows = df.collect()
    }
    rows
  }

}
