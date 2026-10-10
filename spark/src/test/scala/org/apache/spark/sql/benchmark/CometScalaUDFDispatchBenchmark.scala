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

import org.apache.arrow.vector.{BigIntVector, ValueVector, VarBinaryVector}
import org.apache.spark.SparkEnv
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, AttributeSeq, BindReferences, Expression}
import org.apache.spark.sql.execution.adaptive.QueryStageExec

import org.apache.comet.{CometArrowAllocator, CometConf}
import org.apache.comet.codegen.CometBatchKernelCodegen
import org.apache.comet.codegen.CometBatchKernelCodegen.ArrowColumnSpec
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * What a row-level Scala UDF costs on the codegen dispatcher, split into the cost paid per batch
 * and the cost paid per row.
 *
 * Part 1 runs `SELECT max(f(c))` over a Parquet `bigint` column end to end at several batch
 * sizes. The total of a cost paid once per batch grows as batches shrink, and the total of a cost
 * paid per row does not. A tenth of the rows are null. Spark wraps a UDF with a primitive
 * parameter in a null check, `if(isnull(c), null, f(knownnotnull(c)))`, which runs in the
 * dispatcher's kernel with the call; a UDF with a boxed parameter gets no such wrapper and
 * converts its input through Spark's encoder instead.
 *
 * Part 2 calls the dispatcher directly on the same in-memory batch over and over, with no Spark
 * query and no native code, and does the same with the generated kernel it runs. The difference
 * between the two is the dispatcher's own work for each batch: reading its arguments and finding
 * the kernel.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometScalaUDFDispatchBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometScalaUDFDispatchBenchmark-**results.txt".
 */
object CometScalaUDFDispatchBenchmark extends CometBenchmarkBase {

  private val Rows = 4L * 1024 * 1024
  private val BatchSizes = Seq(1024, 8192, 65536)
  private val LayerBatchRows = Seq(1024, 8192)

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    spark.udf.register("prim_add_one", (x: Long) => x + 1)
    spark.udf.register(
      "boxed_add_one",
      (x: java.lang.Long) => if (x == null) null else java.lang.Long.valueOf(x + 1))

    withTempPath { dir =>
      withTempTable(tbl, "parquetV1Table") {
        spark.range(Rows).createOrReplaceTempView(tbl)
        prepareTable(dir, spark.sql(s"SELECT IF(id % 10 = 0, NULL, id * 7919) AS c FROM $tbl"))

        checkPlans()
        BatchSizes.foreach(endToEnd)
        LayerBatchRows.foreach(jvmLayers)
      }
    }
  }

  /** `dispatch` turns the dispatcher on; the cases without a UDF leave it off. */
  private case class EndToEndCase(label: String, f: String, dispatch: Boolean)

  private val endToEndCases = Seq(
    EndToEndCase("dispatch, primitive param", "prim_add_one(c)", dispatch = true),
    EndToEndCase("dispatch, boxed param", "boxed_add_one(c)", dispatch = true),
    EndToEndCase("native c + 1", "c + 1", dispatch = false),
    EndToEndCase("no function, max(c)", "c", dispatch = false))

  private def configs(dispatch: Boolean, batchSize: Int): Seq[(String, String)] = Seq(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_BATCH_SIZE.key -> batchSize.toString,
    CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> dispatch.toString)

  private def query(f: String): String = s"SELECT max($f) FROM parquetV1Table"

  /** Fails unless Comet runs every operator of every case. */
  private def checkPlans(): Unit = endToEndCases.foreach {
    case EndToEndCase(label, f, dispatch) =>
      withSQLConf(configs(dispatch, BatchSizes.head): _*) {
        val df = spark.sql(query(f))
        df.noop()
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        // A query stage is a leaf, so check the plan inside each one separately.
        val stagePlans = collect(plan) { case s: QueryStageExec => s.plan }
        (plan +: stagePlans)
          .flatMap(findFirstNonCometOperator(_, classOf[QueryStageExec]))
          .headOption
          .foreach { op =>
            throw new IllegalStateException(s"$label ran ${op.nodeName} in Spark:\n$plan")
          }
      }
  }

  private def endToEnd(batchSize: Int): Unit = {
    val benchmark =
      new Benchmark(s"max(f(c)), batch size $batchSize", Rows, minNumIters = 5, output = output)
    endToEndCases.foreach { case EndToEndCase(label, f, dispatch) =>
      benchmark.addCase(label) { _ =>
        withSQLConf(configs(dispatch, batchSize): _*) {
          spark.sql(query(f)).noop()
        }
      }
    }
    benchmark.run()
  }

  /**
   * The bound subtree the serde would dispatch for `f`, and its closure-serialized bytes. For a
   * primitive parameter that is Spark's null check together with the call.
   */
  private def boundUdf(f: String): (Expression, Array[Byte]) = {
    val plan = spark.sql(s"SELECT $f FROM parquetV1Table").queryExecution.optimizedPlan
    val dispatched = plan.expressions.collectFirst { case Alias(child, _) => child }.get
    val attrs = dispatched.collect { case a: AttributeReference => a }.distinct
    val bound = BindReferences.bindReference(dispatched, AttributeSeq(attrs))
    val buffer = SparkEnv.get.closureSerializer.newInstance().serialize(bound)
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    (bound, bytes)
  }

  private def binaryScalar(value: Array[Byte]): VarBinaryVector = {
    val v = new VarBinaryVector("arg", CometArrowAllocator)
    v.allocateNew()
    v.setSafe(0, value)
    v.setValueCount(1)
    v
  }

  /** The arguments the serde ships ahead of the data columns. */
  private def dispatcherArgs(bytes: Array[Byte]): Seq[VarBinaryVector] =
    Seq(CometScalaUDFCodegen.digest(bytes), bytes).map(binaryScalar)

  private def jvmLayers(batchRows: Int): Unit = {
    val in = new BigIntVector("c", CometArrowAllocator)
    in.allocateNew(batchRows)
    var i = 0
    while (i < batchRows) {
      if (i % 10 == 0) in.setNull(i) else in.set(i, i * 7919L)
      i += 1
    }
    in.setValueCount(batchRows)
    val inputs: Array[ValueVector] = Array(in)
    val batchesPerIter = (Rows / batchRows).toInt

    val benchmark =
      new Benchmark(s"JVM layers, $batchRows-row batches", Rows, minNumIters = 5, output = output)

    def addBatchCase(label: String)(evaluate: () => ValueVector): Unit =
      benchmark.addCase(label) { _ =>
        var b = 0
        while (b < batchesPerIter) {
          evaluate().close()
          b += 1
        }
      }

    val toClose = Seq("prim_add_one(c)" -> "primitive param", "boxed_add_one(c)" -> "boxed param")
      .flatMap { case (f, param) =>
        val (bound, bytes) = boundUdf(f)
        val specs = IndexedSeq(ArrowColumnSpec(classOf[BigIntVector], nullable = true))
        val kernel = CometBatchKernelCodegen.compile(bound, specs).newInstance()
        kernel.init(0)
        val field = CometBatchKernelCodegen.toFfiArrowField("r", bound.dataType, bound.nullable)
        addBatchCase(s"generated kernel only, $param") { () =>
          val out = CometBatchKernelCodegen.allocateOutput(field, batchRows, 0)
          kernel.process(inputs, out, batchRows)
          out.setValueCount(batchRows)
          out
        }

        val leading = dispatcherArgs(bytes)
        val dispatcher = new CometScalaUDFCodegen
        val dispatchInputs: Array[ValueVector] = (leading :+ in).toArray
        addBatchCase(s"dispatcher evaluate, $param (${bytes.length}-byte expression)") { () =>
          dispatcher.evaluate(dispatchInputs, batchRows)
        }
        leading
      }

    try benchmark.run()
    finally {
      toClose.foreach(_.close())
      in.close()
    }
  }
}
