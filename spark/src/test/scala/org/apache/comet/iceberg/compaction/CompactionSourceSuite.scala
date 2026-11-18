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

package org.apache.comet.iceberg.compaction

import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Tests the Spark side of the compaction source: how a registered batch reads through
 * `DataFrameReader`, without Iceberg. What Comet's native scan makes of it is covered by
 * `CometIcebergCompactionSuite`.
 */
class CompactionSourceSuite extends CometTestBase {

  private val schema = StructType(
    Seq(StructField("id", IntegerType, nullable = false), StructField("name", StringType)))

  private def newBatch(bins: Seq[String]*): CompactionBatch = {
    val tasks = bins.map { bin =>
      new BinTasks(new java.util.ArrayList[AnyRef](bin.map(_.asInstanceOf[AnyRef]).asJava))
    }
    new CompactionBatch("table", "iceberg schema", schema, tasks)
  }

  private def withBatch(batch: CompactionBatch)(body: String => Unit): Unit = {
    val id = UUID.randomUUID().toString
    CompactionBatches.register(id, batch)
    try body(id)
    finally CompactionBatches.remove(id)
  }

  private def read(id: String) =
    spark.read
      .format(classOf[CompactionSource].getName)
      .option(CompactionSource.BatchIdOption, id)
      .load()

  test("a batch reads with its schema and one Spark partition per bin") {
    withBatch(newBatch(Seq("a", "b"), Seq("c"), Seq("d", "e", "f"))) { id =>
      val df = read(id)
      assert(
        df.schema.map(f => (f.name, f.dataType, f.nullable)) ==
          schema.map(f => (f.name, f.dataType, f.nullable)))

      val scans = df.queryExecution.sparkPlan.collect { case scan: BatchScanExec => scan }
      assert(scans.size == 1)
      assert(scans.head.scan.isInstanceOf[CompactionScan])
      assert(scans.head.inputRDD.getNumPartitions == 3)
    }
  }

  test("a bin is the task group of its partition") {
    withBatch(newBatch(Seq("a", "b"), Seq("c"))) { id =>
      val scan = read(id).queryExecution.sparkPlan.collect { case s: BatchScanExec => s }.head
      val partitions = scan.scan.toBatch.planInputPartitions()
      val bins = partitions.map(_.asInstanceOf[CompactionInputPartition].taskGroup.tasks.asScala)
      assert(bins.map(_.toSeq).toSeq == Seq(Seq("a", "b"), Seq("c")))
    }
  }

  test("the scan exposes the accessors Comet reads off an Iceberg scan") {
    val batch = newBatch(Seq("a", "b"), Seq("c"))
    val scan = new CompactionScan(batch)
    assert(scan.table() == "table")
    assert(scan.expectedSchema() == "iceberg schema")
    assert(scan.tasks().asScala.toSeq == Seq("a", "b", "c"))
    assert(scan.readSchema() == schema)
    // Comet finds these by name, so a rename breaks the native scan without breaking the build.
    Seq("table", "expectedSchema", "tasks").foreach { name =>
      assert(classOf[CompactionScan].getMethods.exists(_.getName == name), name)
    }
    assert(classOf[CompactionInputPartition].getMethods.exists(_.getName == "taskGroup"))
    assert(classOf[BinTasks].getMethods.exists(_.getName == "tasks"))
  }

  test("scans of one batch are equal and scans of two batches are not") {
    val batch = newBatch(Seq("a"))
    assert(new CompactionScan(batch) == new CompactionScan(batch))
    assert(new CompactionScan(batch).hashCode == new CompactionScan(batch).hashCode)
    assert(new CompactionScan(batch) != new CompactionScan(newBatch(Seq("a"))))
  }

  test("a batch that is not registered cannot be read") {
    val error = intercept[Exception](read("no-such-batch"))
    val messages =
      Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).map(_.getMessage)
    assert(messages.exists(m => m != null && m.contains("'no-such-batch'")))
  }

  test("Spark itself cannot read a compaction scan") {
    withBatch(newBatch(Seq("a"))) { id =>
      val scan = read(id).queryExecution.sparkPlan.collect { case s: BatchScanExec => s }.head
      val factory = scan.scan.toBatch.createReaderFactory()
      val partition = scan.scan.toBatch.planInputPartitions().head
      intercept[UnsupportedOperationException](factory.createReader(partition))
    }
  }
}
