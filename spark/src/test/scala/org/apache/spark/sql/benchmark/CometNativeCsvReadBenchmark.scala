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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.benchmark.CometNativeCsvReadBenchmark.TPCHSchemas._
import org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmarkArguments
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf

/**
 * @param tableName
 *   Name of the TPC-H table. Must match one of the standard table names: region, nation, part,
 *   supplier, partsupp, customer, orders, lineitem.
 *
 * @param schema
 *   Table data structure in Spark StructType format.
 */
case class NativeCsvReadConfig(tableName: String, schema: StructType)

/**
 * Benchmark to measure Comet csv read performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometNativeCsvReadBenchmark -- --data-location
 * /tmp/tpcds` Results will be written to
 * "spark/benchmarks/CometNativeCsvReadBenchmark-**results.txt".
 */
object CometNativeCsvReadBenchmark extends CometBenchmarkBase {

  private def runNativeCsvBenchmark(
      dataLocation: String,
      tableName: String,
      schema: StructType,
      valuesPerPartition: Int,
      numIters: Int): Unit = {
    val benchmark =
      new Benchmark(s"Native csv read - `$tableName` table", valuesPerPartition, output = output)
    val filePath = s"$dataLocation/$tableName.csv"
    benchmark.addCase("Spark", numIters) { _ =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read
          .schema(schema)
          .options(Map("header" -> "true", "delimiter" -> ","))
          .csv(filePath)
          .noop()
      }
    }
    benchmark.addCase("Native", numIters) { _ =>
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_CSV_V2_NATIVE_ENABLED.key -> "true",
        SQLConf.USE_V1_SOURCE_LIST.key -> "") {
        spark.read
          .schema(schema)
          .options(Map("header" -> "true", "delimiter" -> ","))
          .csv(filePath)
          .noop()
      }
    }
    benchmark.run()
  }

  private val testCases = Seq(
    NativeCsvReadConfig("orders", ordersSchema),
    NativeCsvReadConfig("region", regionSchema),
    NativeCsvReadConfig("nation", nationSchema),
    NativeCsvReadConfig("part", partSchema),
    NativeCsvReadConfig("supplier", supplierSchema),
    NativeCsvReadConfig("partsupp", partsuppSchema),
    NativeCsvReadConfig("customer", customerSchema),
    NativeCsvReadConfig("lineitem", lineitemSchema))

  override def runCometBenchmark(args: Array[String]): Unit = {
    val benchmarkArgs = new TPCDSQueryBenchmarkArguments(args)
    val valuesPerPartition = 1024 * 1024 * 2
    val numIters = 1
    testCases.foreach { config =>
      runNativeCsvBenchmark(
        benchmarkArgs.dataLocation,
        config.tableName,
        config.schema,
        valuesPerPartition,
        numIters)
    }
  }

  object TPCHSchemas {

    val regionSchema: StructType = new StructType()
      .add("r_regionkey", IntegerType, nullable = true)
      .add("r_name", StringType, nullable = true)
      .add("r_comment", StringType, nullable = true)

    val nationSchema: StructType = new StructType()
      .add("n_nationkey", IntegerType, nullable = true)
      .add("n_name", StringType, nullable = true)
      .add("n_regionkey", IntegerType, nullable = true)
      .add("n_comment", StringType, nullable = true)

    val partSchema: StructType = new StructType()
      .add("p_partkey", IntegerType, nullable = true)
      .add("p_name", StringType, nullable = true)
      .add("p_mfgr", StringType, nullable = true)
      .add("p_brand", StringType, nullable = true)
      .add("p_type", StringType, nullable = true)
      .add("p_size", IntegerType, nullable = true)
      .add("p_container", StringType, nullable = true)
      .add("p_retailprice", DoubleType, nullable = true)
      .add("p_comment", StringType, nullable = true)

    val supplierSchema: StructType = new StructType()
      .add("s_suppkey", IntegerType, nullable = true)
      .add("s_name", StringType, nullable = true)
      .add("s_address", StringType, nullable = true)
      .add("s_nationkey", IntegerType, nullable = true)
      .add("s_phone", StringType, nullable = true)
      .add("s_acctbal", DoubleType, nullable = true)
      .add("s_comment", StringType, nullable = true)

    val partsuppSchema: StructType = new StructType()
      .add("ps_partkey", IntegerType, nullable = true)
      .add("ps_suppkey", IntegerType, nullable = true)
      .add("ps_availqty", IntegerType, nullable = true)
      .add("ps_supplycost", DoubleType, nullable = true)
      .add("ps_comment", StringType, nullable = true)

    val customerSchema: StructType = new StructType()
      .add("c_custkey", IntegerType, nullable = true)
      .add("c_name", StringType, nullable = true)
      .add("c_address", StringType, nullable = true)
      .add("c_nationkey", IntegerType, nullable = true)
      .add("c_phone", StringType, nullable = true)
      .add("c_acctbal", DoubleType, nullable = true)
      .add("c_mktsegment", StringType, nullable = true)
      .add("c_comment", StringType, nullable = true)

    val ordersSchema: StructType = new StructType()
      .add("o_orderkey", IntegerType, nullable = true)
      .add("o_custkey", IntegerType, nullable = true)
      .add("o_orderstatus", StringType, nullable = true)
      .add("o_totalprice", DoubleType, nullable = true)
      .add("o_orderdate", DateType, nullable = true)
      .add("o_orderpriority", StringType, nullable = true)
      .add("o_clerk", StringType, nullable = true)
      .add("o_shippriority", IntegerType, nullable = true)
      .add("o_comment", StringType, nullable = true)

    val lineitemSchema: StructType = new StructType()
      .add("l_orderkey", IntegerType, nullable = true)
      .add("l_partkey", IntegerType, nullable = true)
      .add("l_suppkey", IntegerType, nullable = true)
      .add("l_linenumber", IntegerType, nullable = true)
      .add("l_quantity", IntegerType, nullable = true)
      .add("l_extendedprice", DoubleType, nullable = true)
      .add("l_discount", DoubleType, nullable = true)
      .add("l_tax", DoubleType, nullable = true)
      .add("l_returnflag", StringType, nullable = true)
      .add("l_linestatus", StringType, nullable = true)
      .add("l_shipdate", DateType, nullable = true)
      .add("l_commitdate", DateType, nullable = true)
      .add("l_receiptdate", DateType, nullable = true)
      .add("l_shipinstruct", StringType, nullable = true)
      .add("l_shipmode", StringType, nullable = true)
      .add("l_comment", StringType, nullable = true)
  }
}
