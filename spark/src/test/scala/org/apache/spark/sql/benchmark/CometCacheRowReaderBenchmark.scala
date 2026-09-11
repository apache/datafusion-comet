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

import java.nio.charset.StandardCharsets

import org.apache.spark.benchmark.BenchmarkBase
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer
import org.apache.spark.sql.execution.ColumnarToRowExec
import org.apache.spark.sql.execution.columnar.{CometInMemoryRelationHelper, DefaultCachedBatch, DefaultCachedBatchSerializer, InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Compare Spark consumers of Comet and Spark caches (issue #5485).
 *
 * Arguments: [spark|comet|all] [rows] [iterations] [all|mixed|numeric]. Run one format per JVM in
 * alternating order on main and the patch. Cache creation and validation are outside timing.
 */
object CometCacheRowReaderBenchmark extends BenchmarkBase {
  private val warmups = 5

  override def runBenchmarkSuite(args: Array[String]): Unit = {
    require(args.length <= 4, "Expected format, rows, iterations, schema")
    val format = args.headOption.getOrElse("all")
    val rows = args.lift(1).map(_.toLong).getOrElse(5000000L)
    val iterations = args.lift(2).map(_.toInt).getOrElse(15)
    val schema = args.lift(3).getOrElse("all")
    require(Set("all", "spark", "comet").contains(format))
    require(Set("all", "mixed", "numeric").contains(schema))
    require(rows > 0 && iterations > 0)

    emit("CACHE_SAMPLE,format,schema,query,rows,iteration,elapsed_ns")
    val formats =
      if (format == "all") Seq("spark", "comet") else Seq(format)
    val schemas = if (schema == "all") Seq("mixed", "numeric") else Seq(schema)
    formats.foreach { name =>
      CometInMemoryRelationHelper.clearSerializer()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      val serializer = if (name == "spark") {
        classOf[DefaultCachedBatchSerializer].getName
      } else {
        classOf[ArrowCachedBatchSerializer].getName
      }
      val spark = SparkSession
        .builder()
        .master("local[1]")
        .appName(getClass.getSimpleName)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.cache.serializer", serializer)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
        .config("spark.io.compression.codec", "lz4")
        .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
        .config(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
        .config(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key, "false")
        .config(SQLConf.CODEGEN_FACTORY_MODE.key, "CODEGEN_ONLY")
        .config(CometConf.COMET_ENABLED.key, "false")
        .config(CometConf.COMET_EXEC_ENABLED.key, "false")
        .withExtensions(new CometSparkSessionExtensions)
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      try {
        emit(s"CACHE_ENV,$name,Spark=${spark.version},Java=${System.getProperty("java.version")}")
        schemas.foreach(runSchema(spark, name, _, rows, iterations, serializer))
      } finally {
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        CometInMemoryRelationHelper.clearSerializer()
      }
    }
  }

  private def runSchema(
      spark: SparkSession,
      format: String,
      schema: String,
      rows: Long,
      iterations: Int,
      serializer: String): Unit = {
    val mixed = schema == "mixed"
    val first = Seq("id", "id % 1000 AS k", "id + 1 AS v")
    val rest = if (mixed) {
      Seq(
        "concat('str_a_', cast(id % 100000 as string)) AS s1",
        "concat('str_b_', cast(id % 7919 as string)) AS s2",
        "concat('str_c_', cast(id as string)) AS s3")
    } else {
      Seq("id % 100000 AS n1", "id % 7919 AS n2", "id * 3 AS n3")
    }
    val source = spark.range(0, rows, 1, 16).selectExpr((first ++ rest): _*)
    val columns = source.columns.toSeq
    val three = if (mixed) Seq("id", "s1", "s2") else columns.take(3)
    val projections = Seq("count" -> Seq.empty[String], "long" -> Seq("id")) ++
      (if (mixed) Seq("string" -> Seq("s1")) else Seq.empty) ++
      Seq("three" -> three, "all" -> columns)
    def expressions(selected: Seq[String]): Seq[String] = {
      if (selected.isEmpty) Seq("count(*)")
      else
        selected.map { name =>
          if (name.startsWith("s")) s"sum(length($name))" else s"sum($name)"
        }
    }
    // Obtain the expected values before the relation is cached, using Spark's ordinary row plan.
    val expected = projections.map { case (_, selected) =>
      source.selectExpr(expressions(selected): _*).collect()
    }
    val cached = source.persist(StorageLevel.MEMORY_ONLY)
    try {
      assert(cached.count() == rows)
      val relation = cached.queryExecution.withCachedData.collectFirst {
        case relation: InMemoryRelation => relation
      }.get
      val builder = relation.cacheBuilder
      assert(builder.serializer.getClass.getName == serializer)
      val batches = builder.cachedColumnBuffers
      val batchSummary = batches
        .map { batch =>
          // Spark's sizeInBytes comes from statistics; measure its encoded column buffers.
          val bytes = batch match {
            case b: DefaultCachedBatch => b.buffers.map(_.length.toLong).sum
            case _ => batch.sizeInBytes
          }
          (batch.getClass.getSimpleName, batch.numRows.toLong, bytes)
        }
        .collect()
      val expectedClass = if (format == "spark") "DefaultCachedBatch" else "CometCachedBatch"
      assert(batchSummary.forall(_._1 == expectedClass), "Wrong cached payload format")
      assert(batchSummary.map(_._2).sum == rows)
      val storage = spark.sparkContext.getRDDStorageInfo.find(_.id == batches.id).get
      assert(storage.numCachedPartitions == batches.getNumPartitions && storage.diskSize == 0)
      emit(
        s"CACHE_STORAGE,$format,$schema,${batchSummary.length}," +
          s"${batchSummary.map(_._3).sum},${storage.memSize}")

      projections.zip(expected).foreach { case ((name, selected), answer) =>
        val query = cached.selectExpr(expressions(selected): _*)
        val plan = query.queryExecution.executedPlan
        val scans = plan.collect { case scan: InMemoryTableScanExec => scan }
        assert(scans.size == 1, s"Expected one Spark cache scan:\n$plan")
        val scan = scans.head
        assert(scan.attributes.map(_.name).toSet == selected.toSet, s"Wrong projection:\n$plan")
        assert(!scan.supportsColumnar, s"Expected the cache row reader:\n$plan")
        assert(!plan.exists(_.isInstanceOf[ColumnarToRowExec]), s"Unexpected transition:\n$plan")
        assert(!plan.exists(_.getClass.getName.startsWith("org.apache.spark.sql.comet.")))
        emit(s"CACHE_PLAN,$format,$schema,$name,columns=${selected.size}\n$plan")
        runQuery(query, answer, format, schema, name, rows, iterations)
      }
    } finally cached.unpersist(blocking = true)
  }

  private def runQuery(
      query: DataFrame,
      expected: Array[Row],
      format: String,
      schema: String,
      name: String,
      rows: Long,
      iterations: Int): Unit = {
    (0 until warmups).foreach { _ => assert(query.collect().sameElements(expected)) }
    (0 until iterations).foreach { i =>
      val start = System.nanoTime()
      val actual = query.collect()
      val elapsed = System.nanoTime() - start
      assert(actual.sameElements(expected), s"Wrong result for $format/$schema/$name")
      emit(s"CACHE_SAMPLE,$format,$schema,$name,$rows,$i,$elapsed")
    }
    emit(s"CACHE_RESULT,$format,$schema,$name,${expected.mkString(";")}")
  }

  private def emit(line: String): Unit = {
    println(line)
    output.foreach(_.write((line + "\n").getBytes(StandardCharsets.UTF_8)))
  }
}
