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

import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

/**
 * Creates export functions for zero-copy transfer of unshaded Arrow vectors through the C Data
 * Interface.
 *
 * This lives in the spark module where unshaded Arrow types are directly available, eliminating
 * the need for reflection. The export function writes into pre-allocated C Data struct addresses
 * provided by the common module's shaded side.
 *
 * At runtime, arrow-c-data may not be on the classpath (Spark does not bundle it). The
 * [[cDataAvailable]] check ensures graceful degradation to the copy-based path.
 */
object ArrowCDataExport {

  /** Whether unshaded arrow-c-data classes are available on the runtime classpath. */
  private lazy val cDataAvailable: Boolean = {
    try {
      Class.forName("org.apache.arrow.c.Data") // scalastyle:ignore classforname
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  /**
   * Returns an export function if the batch is entirely backed by [[ArrowColumnVector]] and the
   * arrow-c-data library is available at runtime. Returns [[None]] otherwise.
   *
   * The returned function has signature `(colIndex, arrayAddr, schemaAddr) => Unit`. When called,
   * it exports the unshaded Arrow vector at the given column index into the C Data structs at the
   * provided memory addresses.
   */
  def makeExportFn(batch: ColumnarBatch): Option[(Int, Long, Long) => Unit] = {
    if (!cDataAvailable) return None
    if (batch.numCols() == 0) return None

    var i = 0
    while (i < batch.numCols()) {
      if (!batch.column(i).isInstanceOf[ArrowColumnVector]) return None
      i += 1
    }

    Some(CDataExporter.exportFn(batch))
  }

  /**
   * Isolated object that references arrow-c-data classes. The JVM will not load this object (and
   * therefore will not attempt to resolve [[org.apache.arrow.c.Data]] etc.) until it is first
   * accessed, which only happens after [[cDataAvailable]] confirms the classes exist.
   */
  private object CDataExporter {
    def exportFn(batch: ColumnarBatch): (Int, Long, Long) => Unit = {
      (colIdx: Int, arrayAddr: Long, schemaAddr: Long) =>
        {
          val arrowCol = batch.column(colIdx).asInstanceOf[ArrowColumnVector]
          val fv =
            arrowCol.getValueVector.asInstanceOf[org.apache.arrow.vector.FieldVector]
          org.apache.arrow.c.Data.exportVector(
            fv.getAllocator,
            fv,
            null,
            org.apache.arrow.c.ArrowArray.wrap(arrayAddr),
            org.apache.arrow.c.ArrowSchema.wrap(schemaAddr))
        }
    }
  }
}
