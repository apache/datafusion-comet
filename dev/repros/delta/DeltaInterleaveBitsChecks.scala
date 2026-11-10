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

package org.apache.spark.sql.comet.repro


import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Literal}
import org.apache.spark.sql.classic.ClassicConversions._

import org.apache.spark.sql.delta.expressions.InterleaveBits
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Selected reference checks for #7573; no expression implementation is added. */
object DeltaInterleaveBitsChecks {
  def main(args: Array[String]): Unit = {
    require(args.length == 2, "usage: stock|comet <fresh output>")
    val mode = args(0); require(Set("stock", "comet")(mode))
    val out = Paths.get(args(1)); Files.createDirectory(out)
    val spark = SparkSession.builder
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension" +
        (if (mode == "comet") ",org.apache.comet.CometSparkSessionExtensions" else ""))
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.comet.enabled", (mode == "comet").toString)
      .config("spark.comet.exec.enabled", (mode == "comet").toString)
      .config("spark.comet.explain.fallback.enabled", "true")
      .config("spark.shuffle.manager", if (mode == "comet")
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"
        else "org.apache.spark.shuffle.sort.SortShuffleManager")
      .config("spark.memory.offHeap.enabled", "true").config("spark.memory.offHeap.size", "512m")
      .config("spark.sql.adaptive.enabled", "false").config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._
    var passed = 0
    def check(name: String)(body: => Unit): Unit = {
      try { body; Files.write(out.resolve(name + ".passed"), Array.emptyByteArray); passed += 1 }
      catch { case e: Throwable => Files.write(out.resolve(name + ".error.txt"), e.toString.getBytes(UTF_8)); throw e }
    }
    def query(name: String, df: DataFrame)(assertRows: Array[Row] => Unit): Unit = check(name) {
      val rows = df.collect(); assertRows(rows)
      Files.write(out.resolve(name + ".rows.json"), rows.map(_.json).sorted.mkString("[", ",", "]").getBytes(UTF_8))
      Files.write(out.resolve(name + ".schema.json"), df.schema.json.getBytes(UTF_8))
      Files.write(out.resolve(name + ".plan.txt"), df.queryExecution.executedPlan.toString.getBytes(UTF_8))
    }
    try {
      val control = spark.range(20).selectExpr("sum(id) total")
      query("control", control) { rows => require(rows.length == 1 && rows.head.getLong(0) == 190L)
        if (mode == "comet") require(control.queryExecution.executedPlan.toString.contains("Comet")) }
      def oracle(values: Seq[java.lang.Integer]): Array[Byte] = {
        val bytes = new Array[Byte](4 * values.size); var at = 0
        for (bit <- 31 to 0 by -1; v <- values) { val n = if (v == null) 0 else v.intValue()
          if (((n >>> bit) & 1) != 0) bytes(at / 8) = (bytes(at / 8) | (1 << (7-at%8))).toByte
          at += 1 }
        bytes
      }
      def hexBytes(b: Array[Byte]): String = b.map(x => f"${x & 255}%02X").mkString
      val x = 0x000ff0ff; val y = 0xfff00f00
      check("interleave_asymmetric_oracle") {
        require(hexBytes(oracle(Seq(x,y))) == "555555AAAA55AAAA")
        require(!oracle(Seq(x,y)).sameElements(oracle(Seq(y,x))))
        require(hexBytes(oracle(Seq(-1,null))) == "AAAAAAAAAAAAAAAA")
        require(hexBytes(oracle(Seq(null,-1))) == "5555555555555555")
        require(!InterleaveBits(Seq(Literal("x"))).checkInputDataTypes().isSuccess)
      }
      val random = new scala.util.Random(7573L)
      val pairs = Seq[(java.lang.Integer,java.lang.Integer)]((x,y),(y,x),(null,-1),(-1,null),(Int.MinValue,Int.MaxValue),(0,0)) ++
        (0 until 256).map(i => (if (i % 17 == 0) null else Integer.valueOf(random.nextInt()), if (i % 19 == 0) null else Integer.valueOf(random.nextInt())))
      val pairSchema = StructType(Seq(StructField("x",IntegerType),StructField("y",IntegerType)))
      val pairFrame = spark.createDataFrame(spark.sparkContext.parallelize(pairs.map {case (a,b) => Row(a,b)},2),pairSchema)
      val pairPath = out.resolve("pairs").toString; pairFrame.write.parquet(pairPath)
      val inputs = spark.read.parquet(pairPath)
      val expectedPairCounts = pairs.groupBy(p => p).map { case (pair, values) => pair -> values.size }
      def assertInputPairs(rows: Array[Row]): Unit = {
        val actual = rows.toSeq.map(r => (
          if (r.isNullAt(0)) null else Integer.valueOf(r.getInt(0)),
          if (r.isNullAt(1)) null else Integer.valueOf(r.getInt(1))))
        require(actual.groupBy(p => p).map { case (pair, values) => pair -> values.size } == expectedPairCounts,
          "input pairs changed, duplicated or disappeared")
      }
      def checkMixedRows(rows: Array[Row], literalFirst: Boolean): Unit = {
        require(rows.length == pairs.size, "mixed expression output cardinality changed")
        assertInputPairs(rows)
        rows.foreach { r =>
            val value = if (literalFirst) {if (r.isNullAt(1)) null else Integer.valueOf(r.getInt(1))} else {if (r.isNullAt(0)) null else Integer.valueOf(r.getInt(0))}
            require(r.getAs[Array[Byte]](2).sameElements(oracle(if (literalFirst) Seq(Integer.valueOf(-1),value) else Seq(value,Integer.valueOf(-1))))) }
      }
      check("interleave_mixed_validator_rejects_incomplete") {
        for (bad <- Seq(Array.empty[Row],Array(Row(x,y,oracle(Seq(Integer.valueOf(x),Integer.valueOf(-1))))), Array.fill(pairs.size)(Row(x,y,oracle(Seq(Integer.valueOf(x),Integer.valueOf(-1))))))) {
          var rejected = false
          try checkMixedRows(bad,false) catch { case _: IllegalArgumentException => rejected = true }
          assert(rejected,"mixed validator accepted empty, truncated or duplicated output")
        }
      }
      for (fast <- Seq(false,true)) {
        spark.conf.set(DeltaSQLConf.FAST_INTERLEAVE_BITS_ENABLED.key,fast.toString)
        for (dims <- Seq(1,2)) {
          val key = Column(InterleaveBits(Seq("x","y").take(dims).map(UnresolvedAttribute(_)))).as("key")
          val df = inputs.select(col("x"),col("y"),key)
          require(df.schema("key").dataType == BinaryType && !df.schema("key").nullable)
          query(s"interleave_columns_${dims}_$fast",df)(rows => {require(rows.length == pairs.size);assertInputPairs(rows);rows.foreach { r =>
            val values = (0 until dims).map(i => if (r.isNullAt(i)) null else Integer.valueOf(r.getInt(i)))
            require(r.getAs[Array[Byte]](2).sameElements(oracle(values))) }})
        }
        for (literalFirst <- Seq(false,true)) {
          val children = if (literalFirst) Seq(Literal(-1),UnresolvedAttribute("y")) else Seq(UnresolvedAttribute("x"),Literal(-1))
          val df = inputs.select(col("x"),col("y"),Column(InterleaveBits(children)).as("key"))
          require(df.schema("key").dataType == BinaryType && !df.schema("key").nullable)
          query(s"interleave_mixed_${literalFirst}_$fast",df)(rows => checkMixedRows(rows,literalFirst))
        }
        spark.conf.set("spark.sql.optimizer.excludedRules","org.apache.spark.sql.catalyst.optimizer.ConstantFolding")
        for (dims <- Seq(1,2)) {
          val literal = inputs.limit(1).select(Column(InterleaveBits(Seq(x,y).take(dims).map(Literal(_)))).as("key"))
          require(literal.queryExecution.optimizedPlan.exists(p => p.expressions.exists(_.exists(_.isInstanceOf[InterleaveBits]))),"literal expression folded despite exclusion")
          require(literal.schema("key").dataType == BinaryType && !literal.schema("key").nullable)
          query(s"interleave_literals_unfolded_${dims}_$fast",literal)(rows => {
            require(rows.length == 1)
            require(rows.head.getAs[Array[Byte]](0).sameElements(oracle(Seq(x,y).take(dims).map(Integer.valueOf))))
          })
        }
        spark.conf.unset("spark.sql.optimizer.excludedRules")
      }
      require(passed == 15, s"expected 15 named cases, observed $passed")
      Files.write(out.resolve("summary.json"),s"{\"mode\":\"$mode\",\"passed\":$passed,\"native_acceptance\":false}".getBytes(UTF_8))
      println(s"PASS DeltaInterleaveBitsChecks $mode cases=$passed (reference/fallback only)")
    } finally spark.stop()
  }
}
