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

package org.apache.spark.sql.comet

import java.util.{Base64, Collections}

import scala.sys.process._

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, UserDefinedPythonFunction}
import org.apache.spark.sql.functions.{array, col, expr, length, lit, map, struct, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType, TimeType, VariantType, YearMonthIntervalType}

import org.apache.comet.{CometConf, CometExplainInfo, NativeBase}
import org.apache.comet.CometSparkSessionExtensions.isSpark42Plus

class CometArrowPythonUdfSuite extends CometTestBase {

  private def python: String = sys.env.getOrElse("PYSPARK_PYTHON", "python3")

  private def pythonVersion: String =
    Seq(python, "-c", "import sys; print('%d.%d' % sys.version_info[:2])").!!.trim

  /** Pickles `(function, returnType)` with PySpark's cloudpickle, as PySpark does. */
  private def pickledCommand(function: String, returnType: String): Array[Byte] = {
    val code =
      "import base64, pyspark.cloudpickle as cloudpickle, pyarrow as pa, " +
        "pyarrow.compute as pc; from pyspark.sql.types import *; " +
        s"print(base64.b64encode(cloudpickle.dumps(($function, $returnType))).decode())"
    Base64.getDecoder.decode(Seq(python, "-c", code).!!.trim)
  }

  private def arrowUdf(
      name: String,
      command: Array[Byte],
      returnType: DataType,
      evalType: Int = PythonEvalType.SQL_SCALAR_ARROW_UDF,
      env: java.util.Map[String, String] = Collections.emptyMap[String, String]()) = {
    val function = SimplePythonFunction(
      command,
      env,
      Collections.emptyList[String](),
      python,
      pythonVersion,
      Collections.emptyList(),
      null)
    UserDefinedPythonFunction(name, function, returnType, evalType, udfDeterministic = true)
  }

  private def isNative(df: DataFrame): Boolean =
    df.queryExecution.executedPlan.collect { case _: CometArrowEvalPythonExec => true }.nonEmpty

  private def fallbackReasons(df: DataFrame): Set[String] =
    df.queryExecution.executedPlan
      .collectFirst { case op: ArrowEvalPythonExec =>
        op.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty[String])
      }
      .getOrElse(Set.empty)

  test("scalar Arrow UDF falls back when the native feature is unavailable") {
    assume(!NativeBase.supportsPythonUdf())

    val function = SimplePythonFunction(
      Array.emptyByteArray,
      Collections.emptyMap[String, String](),
      Collections.emptyList[String](),
      "python3",
      "3.13",
      Collections.emptyList(),
      null)
    val udf = UserDefinedPythonFunction(
      "arrow_udf",
      function,
      LongType,
      PythonEvalType.SQL_SCALAR_ARROW_UDF,
      udfDeterministic = true)

    withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true") {
      val source = spark.range(1, 2)
      val plan = source.select(udf(source.col("id"))).queryExecution.executedPlan
      assert(plan.collect { case _: CometArrowEvalPythonExec => true }.isEmpty)
    }
  }

  test("scalar Arrow UDF executes in the native pipeline") {
    assume(NativeBase.supportsPythonUdf(), "native library was built without python-udf")

    val python = sys.env.getOrElse("PYSPARK_PYTHON", "python3")
    val code =
      "import base64, pyspark.cloudpickle as cloudpickle, pyarrow.compute as pc; " +
        "from pyspark.sql.types import LongType; " +
        "print(base64.b64encode(cloudpickle.dumps((pc.negate, LongType()))).decode())"
    val command = Base64.getDecoder.decode(Seq(python, "-c", code).!!.trim)
    val pythonVersion =
      Seq(python, "-c", "import sys; print('%d.%d' % sys.version_info[:2])").!!.trim
    val function = SimplePythonFunction(
      command,
      Collections.emptyMap[String, String](),
      Collections.emptyList[String](),
      python,
      pythonVersion,
      Collections.emptyList(),
      null)
    val udf = UserDefinedPythonFunction(
      "negate_arrow",
      function,
      LongType,
      PythonEvalType.SQL_SCALAR_ARROW_UDF,
      udfDeterministic = true)

    withSQLConf(
      CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val source = spark.range(1, 5)
      val df = source.select(udf(source.col("id")))
      assert(df.queryExecution.executedPlan.collect { case _: CometArrowEvalPythonExec =>
        true
      }.nonEmpty)
      checkAnswer(df, Seq(Row(-1L), Row(-2L), Row(-3L), Row(-4L)))

      val twoResults =
        source.select(udf(source.col("id")).as("first"), udf(source.col("id") + 1L).as("second"))
      val nativeUdfs = twoResults.queryExecution.executedPlan.collect {
        case op: CometArrowEvalPythonExec => op
      }
      assert(nativeUdfs.exists(_.nativeOp.getArrowPythonUdf.getFunctionsCount == 2))
      checkAnswer(twoResults, Seq(Row(-1L, -2L), Row(-2L, -3L), Row(-3L, -4L), Row(-4L, -5L)))

      val withSubquery = spark.range(4).select(udf(expr("id + (SELECT max(id) FROM range(8))")))
      val subqueryPlan = withSubquery.queryExecution.executedPlan
      assert(subqueryPlan.collect { case _: CometArrowEvalPythonExec => true }.nonEmpty)
      checkAnswer(withSubquery, Seq(Row(-7L), Row(-8L), Row(-9L), Row(-10L)))
    }

    withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "false") {
      val source = spark.range(1, 2)
      val plan = source.select(udf(source.col("id"))).queryExecution.executedPlan
      assert(plan.collect { case _: CometArrowEvalPythonExec => true }.isEmpty)
    }

    withSQLConf(
      CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true",
      SQLConf.ARROW_EXECUTION_USE_LARGE_VAR_TYPES.key -> "true") {
      val source = spark.range(1, 2)
      val plan = source.select(udf(source.col("id"))).queryExecution.executedPlan
      assert(plan.collect { case _: CometArrowEvalPythonExec => true }.isEmpty)
    }
  }

  test("native Arrow UDF plan hides commands and compares UDF identity without plan IDs") {
    assume(NativeBase.supportsPythonUdf(), "native library was built without python-udf")

    val secret = "private_arrow_udf_command"
    val function = SimplePythonFunction(
      secret.getBytes(java.nio.charset.StandardCharsets.UTF_8),
      Collections.emptyMap[String, String](),
      Collections.emptyList[String](),
      "python3",
      "3.13",
      Collections.emptyList(),
      null)
    val udf = UserDefinedPythonFunction(
      "secret_arrow",
      function,
      LongType,
      PythonEvalType.SQL_SCALAR_ARROW_UDF,
      udfDeterministic = true)

    withSQLConf(
      CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val source = spark.range(1, 2)
      val plan = source.select(udf(source.col("id"))).queryExecution.executedPlan
      val native = plan.collectFirst { case op: CometArrowEvalPythonExec => op }.get
      assert(!plan.treeString.contains(secret))
      assert(!native.toString.contains(secret))

      val differentPlanId = native.copy(nativeOp =
        native.nativeOp.toBuilder.setPlanId(native.nativeOp.getPlanId + 1).build())
      assert(native == differentPlanId)
      assert(native.hashCode() == differentPlanId.hashCode())

      val otherFunction = SimplePythonFunction(
        "different_arrow_udf_command".getBytes(java.nio.charset.StandardCharsets.UTF_8),
        Collections.emptyMap[String, String](),
        Collections.emptyList[String](),
        "python3",
        "3.13",
        Collections.emptyList(),
        null)
      val otherUdf = UserDefinedPythonFunction(
        "secret_arrow",
        otherFunction,
        LongType,
        PythonEvalType.SQL_SCALAR_ARROW_UDF,
        udfDeterministic = true)
      val otherPlan = source.select(otherUdf(source.col("id"))).queryExecution.executedPlan
      val otherNative = otherPlan.collectFirst { case op: CometArrowEvalPythonExec => op }.get
      assert(native.copy(udfs = otherNative.udfs) != native)

      val differentBatchSize = native.nativeOp.toBuilder
      differentBatchSize.getArrowPythonUdfBuilder.setMaxRecordsPerBatch(
        native.nativeOp.getArrowPythonUdf.getMaxRecordsPerBatch + 1)
      assert(native.copy(nativeOp = differentBatchSize.build()) != native)

      val sameFunctionPlan = source.select(udf(source.col("id"))).queryExecution.executedPlan
      val sameFunctionNative =
        sameFunctionPlan.collectFirst { case op: CometArrowEvalPythonExec => op }.get
      assert(native.udfs != sameFunctionNative.udfs)
      assert(native.sameResult(sameFunctionNative))
    }
  }

  test("native Arrow UDF preserves every accepted scalar type and nulls") {
    assume(NativeBase.supportsPythonUdf(), "native library was built without python-udf")

    val python = sys.env.getOrElse("PYSPARK_PYTHON", "python3")
    val pythonVersion =
      Seq(python, "-c", "import sys; print('%d.%d' % sys.version_info[:2])").!!.trim
    val cases: Seq[(DataType, String, String)] = Seq(
      (BooleanType, "BooleanType()", "true"),
      (ByteType, "ByteType()", "7"),
      (ShortType, "ShortType()", "123"),
      (IntegerType, "IntegerType()", "1234"),
      (LongType, "LongType()", "12345"),
      (FloatType, "FloatType()", "1.25"),
      (DoubleType, "DoubleType()", "1.25"),
      (StringType, "StringType()", "hello"),
      (BinaryType, "BinaryType()", "hello"),
      (DecimalType(12, 2), "DecimalType(12, 2)", "12.34"),
      (DateType, "DateType()", "2024-01-02"),
      (TimestampNTZType, "TimestampNTZType()", "2024-01-02 03:04:05.123456"))

    // Three-row Arrow batches over eight rows pass every type to Python at non-zero
    // offsets (booleans at offsets that are not byte aligned), and identity results
    // return to the JVM as slices.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "3") {
      val source = spark.range(0, 8, 1, 1)
      cases.foreach { case (dataType, pythonType, value) =>
        val code =
          "import base64, pyspark.cloudpickle as cloudpickle, pyarrow as pa; " +
            "from pyspark.sql.types import *; " +
            s"print(base64.b64encode(cloudpickle.dumps((lambda a: a, $pythonType))).decode()); " +
            "print(base64.b64encode(cloudpickle.dumps((" +
            "lambda a: pa.array([str(a.type)] * len(a)), StringType()))).decode())"
        val commands =
          Seq(python, "-c", code).!!.trim.linesIterator.map(Base64.getDecoder.decode).toSeq
        def arrowUdf(name: String, command: Array[Byte], returnType: DataType) = {
          val function = SimplePythonFunction(
            command,
            Collections.emptyMap[String, String](),
            Collections.emptyList[String](),
            python,
            pythonVersion,
            Collections.emptyList(),
            null)
          UserDefinedPythonFunction(
            name,
            function,
            returnType,
            PythonEvalType.SQL_SCALAR_ARROW_UDF,
            udfDeterministic = true)
        }
        val identity = arrowUdf("identity_arrow", commands.head, dataType)
        val describeType = arrowUdf("arrow_input_type", commands(1), StringType)
        val input = source.select(
          when(source.col("id") % 3L === 1L, lit(null).cast(dataType))
            .otherwise(lit(value).cast(dataType))
            .as("value"))
        val expected =
          withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "false") {
            input
              .select(identity(input.col("value")), describeType(input.col("value")))
              .collect()
              .toSeq
          }
        withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true") {
          val df = input.select(identity(input.col("value")), describeType(input.col("value")))
          assert(
            df.queryExecution.executedPlan.collect { case _: CometArrowEvalPythonExec =>
              true
            }.nonEmpty,
            s"Native Arrow UDF was not selected for $dataType")
          checkAnswer(df, expected)
        }
      }
    }
  }

  test("native Arrow UDF falls back for schemas and options with different Spark semantics") {
    assume(NativeBase.supportsPythonUdf(), "native library was built without python-udf")

    val function = SimplePythonFunction(
      Array.emptyByteArray,
      Collections.emptyMap[String, String](),
      Collections.emptyList[String](),
      "python3",
      "3.11",
      Collections.emptyList(),
      null)
    def arrowUdf(returnType: org.apache.spark.sql.types.DataType) = UserDefinedPythonFunction(
      "planning_only",
      function,
      returnType,
      PythonEvalType.SQL_SCALAR_ARROW_UDF,
      udfDeterministic = true)

    withSQLConf(
      CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val source = spark.range(1)
      val timestamp = source.col("id").cast(TimestampType)
      val plans = Seq(
        source.select(arrowUdf(LongType)(timestamp)),
        source.select(arrowUdf(LongType)(array(source.col("id")))),
        source.select(arrowUdf(LongType)(map(source.col("id"), source.col("id")))),
        source.select(arrowUdf(LongType)(struct(source.col("id")))),
        source.select(arrowUdf(ArrayType(LongType))(source.col("id"))),
        source.select(arrowUdf(MapType(LongType, LongType))(source.col("id"))),
        source.select(
          arrowUdf(StructType(Seq(StructField("value", LongType))))(source.col("id"))),
        source.select(arrowUdf(TimestampType)(source.col("id"))),
        source.select(arrowUdf(TimeType(6))(source.col("id"))),
        source.select(arrowUdf(VariantType)(source.col("id"))),
        source.select(arrowUdf(YearMonthIntervalType())(source.col("id"))),
        source.select(arrowUdf(CalendarIntervalType)(source.col("id"))))
      plans.foreach { df =>
        val plan = df.queryExecution.executedPlan
        assert(plan.collect { case _: CometArrowEvalPythonExec => true }.isEmpty)
      }
      withSQLConf("spark.sql.pyspark.udf.profiler" -> "perf") {
        val plan = source.select(arrowUdf(LongType)(source.col("id"))).queryExecution.executedPlan
        assert(plan.collect { case _: CometArrowEvalPythonExec => true }.isEmpty)
      }
      withSQLConf(SQLConf.PYTHON_WORKER_LOGGING_ENABLED.key -> "true") {
        val df = source.select(arrowUdf(LongType)(source.col("id")))
        assert(!isNative(df))
        assert(fallbackReasons(df).exists(_.contains("Python worker logging")))
      }

      Seq(
        Collections.singletonMap("PYTHONHASHSEED", "123"),
        Collections.singletonMap("CUSTOM_PYTHON_SETTING", "value")).foreach { env =>
        val withEnvironment = SimplePythonFunction(
          Array.emptyByteArray,
          env,
          Collections.emptyList[String](),
          "python3",
          "3.11",
          Collections.emptyList(),
          null)
        val udf = UserDefinedPythonFunction(
          "planning_only",
          withEnvironment,
          LongType,
          PythonEvalType.SQL_SCALAR_ARROW_UDF,
          udfDeterministic = true)
        val plan = source.select(udf(source.col("id"))).queryExecution.executedPlan
        assert(plan.collect { case _: CometArrowEvalPythonExec => true }.isEmpty)
      }
    }
  }

  test("native Arrow UDF falls back with a reason that fits the UDF") {
    val command = Array.emptyByteArray
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val source = spark.range(1)
      val pandas = source.select(
        arrowUdf("pandas", command, LongType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)(
          source.col("id")))
      val pandasReasons = fallbackReasons(pandas)
      assert(pandasReasons.exists(_.contains("Only scalar @arrow_udf")), pandasReasons)
      assert(
        !pandasReasons.exists(_.contains(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key)),
        pandasReasons)

      if (NativeBase.supportsPythonUdf()) {
        withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "false") {
          val arrow = source.select(arrowUdf("arrow", command, LongType)(source.col("id")))
          assert(
            fallbackReasons(arrow).exists(
              _.contains(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key)))
        }
      }
    }
  }

  test("native Arrow UDF accepts spark.executorEnv entries set on the executor") {
    assume(NativeBase.supportsPythonUdf(), "native library was built without python-udf")

    val (key, value) = sys.env.find(_._1 == "PATH").get
    val conf = spark.sparkContext.conf
    withSQLConf(
      CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val source = spark.range(1)
      def plan(env: (String, String)) =
        source.select(
          arrowUdf(
            "env",
            Array.emptyByteArray,
            LongType,
            env = Collections.singletonMap(env._1, env._2))(source.col("id")))
      try {
        assert(!isNative(plan(key -> value)))
        conf.set(s"spark.executorEnv.$key", value)
        assert(isNative(plan(key -> value)))
        // This test runs in local mode, where the executor is the driver process.
        conf.set("spark.executorEnv.COMET_ARROW_UDF_UNSET", "value")
        assert(!isNative(plan("COMET_ARROW_UDF_UNSET" -> "value")))
        // The embedded interpreter cannot honor another hash seed.
        conf.set("spark.executorEnv.PYTHONHASHSEED", "123")
        assert(!isNative(plan("PYTHONHASHSEED" -> "123")))
      } finally {
        conf.remove(s"spark.executorEnv.$key")
        conf.remove("spark.executorEnv.COMET_ARROW_UDF_UNSET")
        conf.remove("spark.executorEnv.PYTHONHASHSEED")
      }
    }
  }

  test("native Arrow UDF reads a multi-row-group Parquet scan and reports metrics") {
    assume(NativeBase.supportsPythonUdf(), "native library was built without python-udf")

    val negate = arrowUdf("negate_arrow", pickledCommand("pc.negate", "LongType()"), LongType)
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark
        .range(0, 5000, 1, 1)
        .write
        .option("parquet.block.size", 512)
        .parquet(path)
      val rowGroups = dir.listFiles().filter(_.getName.endsWith(".parquet")).map { file =>
        val reader = ParquetFileReader.open(HadoopInputFile
          .fromPath(new Path(file.getCanonicalPath), spark.sessionState.newHadoopConf()))
        try reader.getRowGroups.size
        finally reader.close()
      }
      assert(rowGroups.sum > 1)

      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "777") {
        val input = spark.read.parquet(path)
        val expected =
          withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "false") {
            input.select(col("id"), negate(col("id"))).collect().toSeq
          }
        withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true") {
          val df = input.select(col("id"), negate(col("id")))
          assert(isNative(df))
          checkAnswer(df, expected)
          val node = df.queryExecution.executedPlan.collectFirst {
            case op: CometArrowEvalPythonExec => op
          }.get
          assert(node.metrics("output_rows").value == 5000)
          assert(node.metrics("python_time").value > 0)
          assert(node.metrics("elapsed_compute").value >= node.metrics("python_time").value)
        }
      }
    }
  }

  test("native Arrow UDF decodes invalid UTF-8 returned by an unsafe cast") {
    assume(NativeBase.supportsPythonUdf(), "native library was built without python-udf")

    val unsafeCast = arrowUdf(
      "unsafe_utf8",
      pickledCommand("lambda a: pc.cast(a, pa.string(), safe=False)", "StringType()"),
      StringType)
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val input = spark
        .range(0, 3, 1, 1)
        .select(expr("CASE id WHEN 0 THEN X'6F6B' WHEN 1 THEN X'FF' ELSE X'61FF62' END").as("b"))
      def query = {
        val result = input.select(unsafeCast(col("b")).as("s"))
        result.select(col("s"), length(col("s")))
      }
      val expected =
        withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "false") {
          query.collect().toSeq
        }
      withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true") {
        val df = query
        assert(isNative(df))
        checkAnswer(df, expected)
      }
    }
  }

  test("native Arrow UDF errors include the Python traceback") {
    assume(NativeBase.supportsPythonUdf(), "native library was built without python-udf")

    val failing = arrowUdf("failing", pickledCommand("lambda a: 1 // 0", "LongType()"), LongType)
    withSQLConf(
      CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.range(1).select(failing(col("id")))
      assert(isNative(df))
      val message = intercept[Exception](df.collect()).getMessage
      assert(message.contains("Traceback (most recent call last)"), message)
      assert(message.contains("ZeroDivisionError"), message)
    }
  }

  test("native Arrow UDF accepts array-like results on Spark 4.2") {
    assume(NativeBase.supportsPythonUdf(), "native library was built without python-udf")
    assume(isSpark42Plus, "Spark 4.1 requires a pyarrow.Array result")

    val toList =
      arrowUdf("to_list", pickledCommand("lambda a: a.to_pylist()", "LongType()"), LongType)
    val toNumpy = arrowUdf(
      "to_numpy",
      pickledCommand("lambda a: a.to_numpy(zero_copy_only=False) * 2", "LongType()"),
      LongType)
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val source = spark.range(1, 5, 1, 1)
      def query = source.select(toList(col("id")), toNumpy(col("id")))
      val expected =
        withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "false") {
          query.collect().toSeq
        }
      assert(expected == Seq(Row(1L, 2L), Row(2L, 4L), Row(3L, 6L), Row(4L, 8L)))
      withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true") {
        val df = query
        assert(isNative(df))
        checkAnswer(df, expected)
      }
    }
  }
}
