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

package org.apache.spark.sql.benchmark

import org.apache.arrow.vector.{BigIntVector, BitVectorHelper, ValueVector}
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.LongType

import org.apache.comet.CometConf
import org.apache.comet.udf.{CometJvmUDF, CometUDF}

/**
 * Benchmark of a vectorized JVM UDF registered through `CometJvmUDF` against the same function
 * written as an ordinary Spark UDF, which runs either on Spark or, with Comet enabled, through
 * the codegen dispatcher. Each case is one function in three forms:
 *
 *   - `Spark`: an ordinary Scala UDF with Comet disabled.
 *   - `Comet, codegen dispatch`: the same Scala UDF, called once per row from a kernel the
 *     dispatcher compiles.
 *   - `Comet, vectorized UDF`: a `CometUDF` called once per batch, looping over the Arrow buffers
 *     itself.
 *
 * The vectorized form reads and writes the value buffers directly and copies the validity bitmap
 * in one call, which is what the API exists to allow. Its arguments are evaluated natively, so
 * only the column itself crosses into the JVM.
 *
 * A last row runs the query without the UDF. Its time is the floor every Comet row shares, so the
 * difference between it and a UDF row is what that form of the UDF costs.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometJvmUdfBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometJvmUdfBenchmark-**results.txt".
 */
object CometJvmUdfBenchmark extends CometBenchmarkBase {

  import CometJvmUdfBenchmarkUdfs._

  private val Rows = 4L * 1024 * 1024

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    spark.udf.register("spark_add_one", (x: Long) => x + 1)
    spark.udf.register("spark_mix", (x: Long) => CometBenchmarkBase.mix64(x))
    CometJvmUDF.register(spark, "jvm_add_one", classOf[AddOneUdf], Seq(LongType), LongType)
    CometJvmUDF.register(spark, "jvm_mix", classOf[MixUdf], Seq(LongType), LongType)

    withTempPath { dir =>
      withTempTable(tbl, "parquetV1Table") {
        spark.range(Rows).createOrReplaceTempView(tbl)
        // Every tenth value is null, so both forms pay for null handling.
        prepareTable(dir, spark.sql(s"SELECT IF(id % 10 = 0, NULL, id * 7919) AS c FROM $tbl"))

        verifyFormsAgree("add_one")
        verifyFormsAgree("mix")
        runCase("x + 1", "add_one")
        runCase("64-bit mix (about 10 arithmetic ops)", "mix")
      }
    }
  }

  // `max` consumes every value without overflowing, which would compare engines' overflow
  // handling rather than their UDFs.
  private def query(udf: String): String = s"SELECT max($udf(c)) FROM parquetV1Table"

  private def cometConfigs(dispatch: Boolean): Seq[(String, String)] = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> dispatch.toString)

  /**
   * One form of a function: the UDF name prefix to call it by, the confs to run it under, and
   * whether Comet has to run every operator of the query.
   */
  private case class Form(
      label: String,
      udfPrefix: String,
      configs: Seq[(String, String)],
      native: Boolean)

  private val forms = Seq(
    Form("Spark", "spark_", Seq(CometConf.COMET_ENABLED.key -> "false"), native = false),
    Form("Comet, codegen dispatch", "spark_", cometConfigs(dispatch = true), native = true),
    Form("Comet, vectorized UDF", "jvm_", cometConfigs(dispatch = false), native = true))

  /**
   * A timing means nothing if one form computed something else, or ran in Spark: a vectorized UDF
   * that Spark evaluates runs one row at a time and still returns the right answer.
   */
  private def verifyFormsAgree(fn: String): Unit = {
    val results = forms.map(f => f.label -> collect(f, fn))
    val (_, expected) = results.head
    results.tail.foreach { case (label, rows) =>
      assert(rows == expected, s"$fn: $label returned $rows, not $expected")
    }
  }

  /** `withSQLConf` returns `Unit` on Spark 3.x, so the rows leave through a local. */
  private def collect(form: Form, fn: String): Seq[Row] = {
    var rows: Seq[Row] = Nil
    withSQLConf(form.configs: _*) {
      val df = spark.sql(query(form.udfPrefix + fn))
      rows = df.collect().toSeq
      if (form.native) {
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        findFirstNonCometOperator(plan).foreach { op =>
          throw new IllegalStateException(
            s"$fn: ${form.label} ran ${op.nodeName} in Spark:\n${plan.treeString}")
        }
      }
    }
    rows
  }

  private def runCase(name: String, fn: String): Unit = {
    val benchmark = new Benchmark(name, Rows, output = output)
    forms.foreach { form =>
      benchmark.addCase(form.label) { _ =>
        withSQLConf(form.configs: _*) {
          spark.sql(query(form.udfPrefix + fn)).noop()
        }
      }
    }
    benchmark.addCase("Comet, no UDF (scan and max only)") { _ =>
      withSQLConf(cometConfigs(dispatch = false): _*) {
        spark.sql("SELECT max(c) FROM parquetV1Table").noop()
      }
    }
    benchmark.run()
  }
}

/** The vectorized forms, top-level so that the UDF bridge can instantiate them by name. */
object CometJvmUdfBenchmarkUdfs {

  /**
   * Apply `f` to every value of a `BigIntVector` column, reading and writing the value buffers
   * directly and copying the validity bitmap whole. A null slot's value is computed and then
   * masked, which is cheaper than a branch per row.
   */
  private def mapLongs(inputs: Array[ValueVector], numRows: Int, f: Long => Long): ValueVector = {
    val in = inputs(0).asInstanceOf[BigIntVector]
    val out = new BigIntVector("result", in.getAllocator)
    out.allocateNew(numRows)
    val src = in.getDataBuffer
    val dst = out.getDataBuffer
    var i = 0
    while (i < numRows) {
      val offset = i * 8L
      dst.setLong(offset, f(src.getLong(offset)))
      i += 1
    }
    val validityBytes = BitVectorHelper.getValidityBufferSize(numRows).toLong
    out.getValidityBuffer.setBytes(0, in.getValidityBuffer, 0, validityBytes)
    out.setValueCount(numRows)
    out
  }

  class AddOneUdf extends CometUDF {
    override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector =
      mapLongs(inputs, numRows, _ + 1)
  }

  class MixUdf extends CometUDF {
    override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector =
      mapLongs(inputs, numRows, CometBenchmarkBase.mix64)
  }
}
