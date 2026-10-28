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

import java.io.File
import java.net.{ServerSocket, Socket}
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.CometNativeExecutorsExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, count, max, sum}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

/**
 * Runs queries on a Comet Native Executor cluster (see
 * https://github.com/apache/datafusion-comet/issues/7288): a comet-scheduler and a comet-executor
 * started from the binaries in the directory `COMET_NATIVE_EXECUTORS_BIN_DIR` names. The tests
 * are skipped when it isn't set. To run them:
 *
 * {{{
 * cargo build --manifest-path native/Cargo.toml -p datafusion-comet --features native-executors
 * CARGO_TARGET_DIR=native/target cargo build --manifest-path native/native-executors/Cargo.toml
 * COMET_NATIVE_EXECUTORS_BIN_DIR=native/target/debug ./mvnw test -Dtest=none \
 *   -Dsuites=org.apache.comet.exec.CometNativeExecutorsSuite
 * }}}
 */
class CometNativeExecutorsSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val binDir: Option[File] =
    sys.env.get("COMET_NATIVE_EXECUTORS_BIN_DIR").map(new File(_).getAbsoluteFile)
  private var processes: Seq[Process] = Nil
  private var schedulerUrl: String = _

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(CometConf.COMET_SHUFFLE_MODE.key, "native")
    conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "4")
    conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    binDir.foreach(startCluster)
  }

  override def afterAll(): Unit = {
    try {
      // The executor first, so that it doesn't report to a scheduler that has gone.
      processes.reverse.foreach { process =>
        process.destroy()
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
          process.destroyForcibly()
        }
      }
    } finally {
      super.afterAll()
    }
  }

  private def startCluster(dir: File): Unit = {
    val schedulerPort = freePort()
    val flightPort = freePort()
    val executorPort = freePort()
    val executorGrpcPort = freePort()
    val logs = Files.createTempDirectory("comet-native-executors").toFile
    logInfo(s"Comet Native Executor cluster logs: $logs")
    start(
      new File(dir, "comet-scheduler"),
      Map(
        "COMET_SCHEDULER_BIND_HOST" -> "127.0.0.1",
        "COMET_SCHEDULER_PORT" -> schedulerPort.toString,
        "COMET_SCHEDULER_FLIGHT_PORT" -> flightPort.toString),
      new File(logs, "scheduler.log"))
    waitForPort(schedulerPort)
    waitForPort(flightPort)
    start(
      new File(dir, "comet-executor"),
      Map(
        "COMET_EXECUTOR_BIND_HOST" -> "127.0.0.1",
        "COMET_EXECUTOR_PORT" -> executorPort.toString,
        "COMET_EXECUTOR_GRPC_PORT" -> executorGrpcPort.toString,
        "COMET_SCHEDULER_HOST" -> "127.0.0.1",
        "COMET_SCHEDULER_PORT" -> schedulerPort.toString,
        "COMET_EXECUTOR_TASKS" -> "4",
        "COMET_EXECUTOR_WORK_DIR" -> new File(logs, "work").getPath),
      new File(logs, "executor.log"))
    waitForPort(executorPort)
    schedulerUrl = s"http://127.0.0.1:$flightPort"
  }

  private def start(binary: File, env: Map[String, String], log: File): Unit = {
    val builder = new ProcessBuilder(binary.getPath)
    env.foreach { case (key, value) => builder.environment().put(key, value) }
    builder.redirectErrorStream(true).redirectOutput(log)
    processes :+= builder.start()
  }

  private def freePort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }

  private def waitForPort(port: Int): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60)
    while (Try(new Socket("127.0.0.1", port).close()).isFailure) {
      assert(System.nanoTime() < deadline, s"Nothing is listening on port $port")
      Thread.sleep(100)
    }
  }

  private def nativeExecutorsTest(name: String)(body: => Unit): Unit = test(name) {
    assume(binDir.isDefined, "COMET_NATIVE_EXECUTORS_BIN_DIR is not set")
    body
  }

  private def onCluster[T](body: => T): T = withSQLConf(
    CometConf.COMET_NATIVE_EXECUTORS_ENABLED.key -> "true",
    CometConf.COMET_NATIVE_EXECUTORS_SCHEDULER_URL.key -> schedulerUrl)(body)

  /** Checks that the cluster runs `query` and returns what Spark returns. */
  private def checkOnCluster(query: => DataFrame): Unit = {
    val expected = query.collect().toSeq
    onCluster {
      val df = query
      checkAnswer(df, expected)
      val plan = df.queryExecution.executedPlan
      assert(collect(plan) { case exec: CometNativeExecutorsExec => exec }.size == 1, plan)
    }
  }

  /** A Parquet table with several files, read as several partitions. */
  private def withTable(body: String => Unit): Unit = withTempPath { dir =>
    spark
      .range(0, 10000, 1, 8)
      .selectExpr("id", "id % 7 AS k", "CAST(id AS DOUBLE) * 1.5 AS v")
      .write
      .parquet(dir.getCanonicalPath)
    withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> "16384") {
      body(dir.getCanonicalPath)
    }
  }

  nativeExecutorsTest("scan, filter and project") {
    withTable { path =>
      checkOnCluster(spark.read.parquet(path).where("k > 2").selectExpr("id", "v * 2 AS w"))
    }
  }

  nativeExecutorsTest("aggregate across a hash shuffle") {
    withTable { path =>
      checkOnCluster(spark.read.parquet(path).groupBy("k").agg(sum("v"), count("*")))
    }
  }

  nativeExecutorsTest("aggregate across a hash shuffle without AQE") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTable { path =>
        checkOnCluster(spark.read.parquet(path).groupBy("k").agg(sum("v"), max("id")))
      }
    }
  }

  nativeExecutorsTest("global aggregate across a single-partition shuffle") {
    withTable { path =>
      checkOnCluster(spark.read.parquet(path).agg(sum("id"), max("v"), count("*")))
    }
  }

  Seq(true, false).foreach { sortMerge =>
    val join = if (sortMerge) "sort-merge" else "hash"
    nativeExecutorsTest(s"shuffled $join join") {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PREFER_SORTMERGEJOIN.key -> sortMerge.toString) {
        withTable { path =>
          val left = spark.read.parquet(path).select("id", "k")
          val right = spark.read
            .parquet(path)
            .where("id < 10")
            .select(col("k").as("k2"), col("v"))
          checkOnCluster(left.join(right, left("k") === right("k2")).select("id", "v"))
        }
      }
    }
  }

  /**
   * TPC-H's `lineitem`: the table under `SPARK_TPCH_DATA` when it is set, as for
   * `CometTPCHQuerySuite`, and otherwise generated data with the same schema.
   */
  private def withLineitem(body: => Unit): Unit = sys.env.get("SPARK_TPCH_DATA") match {
    case Some(path) =>
      spark.read.parquet(s"$path/lineitem").createOrReplaceTempView("lineitem")
      withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> "32m")(body)
    case None =>
      withTempPath { dir =>
        spark
          .range(0, 100000, 1, 8)
          .selectExpr(
            "CAST(id % 50 + 1 AS DECIMAL(15, 2)) AS l_quantity",
            "CAST((id % 1000) * 1.37 + 900 AS DECIMAL(15, 2)) AS l_extendedprice",
            "CAST((id % 11) / 100.0 AS DECIMAL(15, 2)) AS l_discount",
            "CAST((id % 9) / 100.0 AS DECIMAL(15, 2)) AS l_tax",
            "ELT(CAST(id % 3 AS INT) + 1, 'A', 'N', 'R') AS l_returnflag",
            "ELT(CAST(id % 2 AS INT) + 1, 'F', 'O') AS l_linestatus",
            "DATE_ADD(DATE '1992-01-01', CAST(id % 2500 AS INT)) AS l_shipdate")
          .write
          .parquet(dir.getCanonicalPath)
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("lineitem")
        withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> "131072")(body)
      }
  }

  private val q1Aggregate = """
    |select
    |  l_returnflag,
    |  l_linestatus,
    |  sum(l_quantity) as sum_qty,
    |  sum(l_extendedprice) as sum_base_price,
    |  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    |  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    |  avg(l_quantity) as avg_qty,
    |  avg(l_extendedprice) as avg_price,
    |  avg(l_discount) as avg_disc,
    |  count(*) as count_order
    |from
    |  lineitem
    |where
    |  l_shipdate <= date '1998-12-01' - interval '90' day
    |group by
    |  l_returnflag,
    |  l_linestatus""".stripMargin

  nativeExecutorsTest("TPC-H q1 without its order by") {
    withLineitem {
      checkOnCluster(sql(q1Aggregate))
    }
  }

  nativeExecutorsTest("TPC-H q1") {
    withLineitem {
      // A new Dataset each time, since a Dataset keeps the plan it was first run with.
      def q1 = sql(s"$q1Aggregate\norder by\n  l_returnflag,\n  l_linestatus")
      checkOnCluster(q1)
      // checkAnswer ignores the order of the rows.
      val expected = q1.collect().toSeq
      onCluster(assert(q1.collect().toSeq == expected))
    }
  }

  nativeExecutorsTest("a query the cluster cannot run fails, naming the operator") {
    withTable { path =>
      onCluster {
        // A small join is broadcast, and the prototype doesn't support broadcast exchanges.
        val left = spark.read.parquet(path)
        val right = spark.read.parquet(path).where("id < 10").select(col("k").as("k2"))
        val error =
          intercept[Exception](left.join(right, left("k") === right("k2")).collect())
        val messages = Iterator
          .iterate[Throwable](error)(_.getCause)
          .takeWhile(_ != null)
          .map(e => String.valueOf(e.getMessage))
          .toSeq
        assert(
          messages.exists(message =>
            message.contains("Comet Native Executors cannot run this query") &&
              message.contains("CometBroadcastExchange")),
          error)
      }
    }
  }
}
