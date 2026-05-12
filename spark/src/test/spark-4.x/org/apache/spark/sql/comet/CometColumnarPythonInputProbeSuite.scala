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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.concurrent.atomic.AtomicReference

import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

/**
 * Probe test for the invariant `CometColumnarPythonInput` relies on: `makeTransferPair` between a
 * Comet `ColumnarBatch`'s field vectors and a `VectorSchemaRoot` allocated from
 * `ArrowUtils.rootAllocator` must succeed, and the resulting Arrow IPC stream must round-trip to
 * equivalent row counts and values.
 *
 * If this test starts failing because of allocator changes, `CometColumnarPythonInput` must grow
 * a per-buffer copy fallback before the regression lands on main.
 */
class CometColumnarPythonInputProbeSuite extends CometTestBase {

  test("Comet vectors transfer-pair into ArrowUtils.rootAllocator VSR and round-trip via IPC") {
    withTempPath { path =>
      val pathStr = path.getCanonicalPath
      spark
        .range(0, 1000, 1, 1)
        .selectExpr("id AS id", "CAST(id AS DOUBLE) * 1.5 AS value")
        .write
        .mode("overwrite")
        .parquet(pathStr)

      val df = spark.read.parquet(pathStr)
      val childSchema = df.schema
      val wireSchema = StructType(Array(StructField("struct", childSchema)))
      // Capture timezone on the driver before entering the partition closure.
      val timeZoneId = conf.sessionLocalTimeZone

      // ColumnarBatch is not serializable, so we can't use take()/collect(). Run all Arrow
      // operations inside foreachPartition. In local mode this executes in the same JVM.
      // A shared AtomicReference carries any failure back to the test thread.
      val failureRef = new AtomicReference[Throwable](null)

      df.queryExecution.executedPlan
        .collectFirst { case n: CometNativeExec => n }
        .getOrElse(fail("Expected CometNativeExec in plan"))
        .executeColumnar()
        .foreachPartition { (batches: Iterator[ColumnarBatch]) =>
          if (batches.hasNext) {
            val batch = batches.next()
            try {
              val arrowSchema = Utils.toArrowSchema(wireSchema, timeZoneId)
              val rootAlloc = org.apache.spark.sql.util.ArrowUtils.rootAllocator
              val allocator = rootAlloc.newChildAllocator("probe-allocator", 0, Long.MaxValue)
              val root = VectorSchemaRoot.create(arrowSchema, allocator)
              try {
                val structVec = root.getVector(0).asInstanceOf[StructVector]
                var i = 0
                while (i < batch.numCols()) {
                  val src = batch.column(i).asInstanceOf[ArrowColumnVector].getValueVector
                  val dst = structVec.getChildByOrdinal(i).asInstanceOf[FieldVector]
                  src.makeTransferPair(dst).transfer()
                  i += 1
                }
                structVec.setValueCount(batch.numRows())
                root.setRowCount(batch.numRows())

                val baos = new ByteArrayOutputStream()
                val writer = new ArrowStreamWriter(root, null, baos)
                writer.start()
                writer.writeBatch()
                writer.end()

                val readAllocator =
                  rootAlloc.newChildAllocator("probe-read", 0, Long.MaxValue)
                try {
                  val reader = new ArrowStreamReader(
                    new ByteArrayInputStream(baos.toByteArray),
                    readAllocator)
                  try {
                    if (!reader.loadNextBatch()) {
                      failureRef.set(
                        new AssertionError("IPC round-trip: expected at least one record batch"))
                    } else {
                      val readRoot = reader.getVectorSchemaRoot
                      if (readRoot.getRowCount != batch.numRows()) {
                        failureRef.set(
                          new AssertionError(
                            s"row count mismatch: read=${readRoot.getRowCount}, " +
                              s"expected=${batch.numRows()}"))
                      }
                    }
                  } finally {
                    reader.close()
                  }
                } finally {
                  readAllocator.close()
                }
              } finally {
                root.close()
                allocator.close()
                batch.close()
              }
            } catch {
              case t: Throwable => failureRef.set(t)
            }
          }
        }

      val failure = failureRef.get()
      if (failure != null) throw failure
    }
  }
}
