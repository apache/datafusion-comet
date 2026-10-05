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
    spark.udf.register("spark_mix", (x: Long) => mix(x))
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

  private val sparkConfigs = Seq(CometConf.COMET_ENABLED.key -> "false")

  private def cometConfigs(dispatch: Boolean): Seq[(String, String)] = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> dispatch.toString)

  /** A timing means nothing if one form computed something else. */
  private def verifyFormsAgree(fn: String): Unit = {
    val expected = collect(sparkConfigs, query(s"spark_$fn"))
    val dispatched = collect(cometConfigs(dispatch = true), query(s"spark_$fn"))
    val vectorized = collect(cometConfigs(dispatch = false), query(s"jvm_$fn"))
    assert(dispatched == expected, s"$fn: codegen dispatch returned $dispatched, not $expected")
    assert(vectorized == expected, s"$fn: vectorized UDF returned $vectorized, not $expected")
  }

  /** `withSQLConf` returns `Unit` on Spark 3.x, so the rows leave through a local. */
  private def collect(configs: Seq[(String, String)], sql: String): Seq[Row] = {
    var rows: Seq[Row] = Nil
    withSQLConf(configs: _*) {
      rows = spark.sql(sql).collect().toSeq
    }
    rows
  }

  private def runCase(name: String, fn: String): Unit = {
    val benchmark = new Benchmark(name, Rows, output = output)
    benchmark.addCase("Spark") { _ =>
      withSQLConf(sparkConfigs: _*) {
        spark.sql(query(s"spark_$fn")).noop()
      }
    }
    benchmark.addCase("Comet, codegen dispatch") { _ =>
      withSQLConf(cometConfigs(dispatch = true): _*) {
        spark.sql(query(s"spark_$fn")).noop()
      }
    }
    benchmark.addCase("Comet, vectorized UDF") { _ =>
      // The dispatcher is off, so a plan that needed it would fall back to Spark and the
      // vectorized UDF's catalog stub would fail the case rather than time the wrong thing.
      withSQLConf(cometConfigs(dispatch = false): _*) {
        spark.sql(query(s"jvm_$fn")).noop()
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

  /** A stand-in for a function with some arithmetic in it: SplitMix64's finalizer. */
  def mix(x: Long): Long = {
    var z = x + 0x9e3779b97f4a7c15L
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)
  }

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
      mapLongs(inputs, numRows, mix)
  }
}
