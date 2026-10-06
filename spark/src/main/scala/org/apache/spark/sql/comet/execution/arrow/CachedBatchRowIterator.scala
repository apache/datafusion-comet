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

package org.apache.spark.sql.comet.execution.arrow

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, CodeGeneratorWithInterpretedFallback, InterpretedUnsafeProjection, LeafExpression, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Reads vectors directly into Spark's reusable UnsafeRow buffer. The input iterator owns the
 * batches and releases them on advancement or task completion. As with Spark's cache reader,
 * callers must copy rows they retain across next(), but the returned row owns its variable-width
 * values and remains valid when hasNext() releases the batch that supplied them.
 *
 * The generated reader hands each column read to GenerateUnsafeProjection as an expression, so it
 * splits the field writes of a wide projection into methods of bounded size, as it does for any
 * Spark projection. If a generated method still exceeds the huge-method limit, the reader backs
 * off the way WholeStageCodegenExec does, here to Spark's projection of each batch row.
 */
private[arrow] class CachedBatchRowIterator(attributes: Seq[Attribute])
    extends CodeGeneratorWithInterpretedFallback[Iterator[ColumnarBatch], Iterator[InternalRow]] {

  private def fields: Seq[BoundReference] = attributes.zipWithIndex.map { case (attr, i) =>
    BoundReference(i, attr.dataType, attr.nullable)
  }

  override protected def createCodeGeneratedObject(
      batches: Iterator[ColumnarBatch]): Iterator[InternalRow] = {
    val ctx = new CodegenContext
    val vectorClass = classOf[ColumnVector].getName
    val batchClass = classOf[ColumnarBatch].getName
    val columns = ctx.addMutableState(
      s"$vectorClass[]",
      "columns",
      v => s"$v = new $vectorClass[${attributes.length}];",
      forceInline = true)
    val rowId = ctx.addMutableState(CodeGenerator.JAVA_INT, "rowId", forceInline = true)
    val reads = attributes.zipWithIndex.map { case (attr, i) =>
      VectorValue(s"$columns[$i]", rowId, attr.dataType, attr.nullable)
    }
    // With ctx.currentVars unset, GenerateUnsafeProjection splits the field writes into methods
    // that take the input row as their argument. The reads above ignore it.
    val projection = GenerateUnsafeProjection.createCode(ctx, reads)
    val batchesRef = ctx.addReferenceObj("batches", batches, "scala.collection.Iterator")
    val code = s"""
      public Object generate(Object[] references) {
        return new SpecificCachedBatchRowIterator(references);
      }

      class SpecificCachedBatchRowIterator extends scala.collection.AbstractIterator {
        private final Object[] references;
        private final scala.collection.Iterator batches;
        private int numRows = 0;
        ${ctx.declareMutableStates()}

        public SpecificCachedBatchRowIterator(Object[] references) {
          this.references = references;
          this.batches = $batchesRef;
          ${ctx.initMutableStates()}
        }

        public boolean hasNext() {
          while ($rowId >= numRows && batches.hasNext()) {
            $batchClass batch = ($batchClass) batches.next();
            numRows = batch.numRows();
            $rowId = 0;
            for (int ordinal = 0; ordinal < $columns.length; ordinal++) {
              $columns[ordinal] = batch.column(ordinal);
            }
          }
          return $rowId < numRows;
        }

        public InternalRow next() {
          if (!hasNext()) throw new java.util.NoSuchElementException();
          InternalRow ${ctx.INPUT_ROW} = null;
          ${projection.code}
          $rowId++;
          return ${projection.value};
        }

        ${ctx.declareAddedFunctions()}
      }
    """
    val (compiled, stats) =
      CodeGenerator.compile(new CodeAndComment(code, ctx.getPlaceHolderToComments()))
    // Honor spark.sql.codegen.hugeMethodLimit as whole-stage codegen does, but never go above
    // HotSpot's own limit: the config defaults to the largest method the JVM accepts, while this
    // runs once per row and HotSpot never JIT-compiles a method longer than
    // DEFAULT_JVM_HUGE_METHOD_LIMIT bytes.
    val limit =
      math.min(SQLConf.get.hugeMethodLimit, CodeGenerator.DEFAULT_JVM_HUGE_METHOD_LIMIT)
    if (stats.maxMethodCodeSize > limit) {
      logInfo(
        s"Generated cache reader for ${attributes.length} columns has a " +
          s"${stats.maxMethodCodeSize}-byte method, above the $limit-byte limit; " +
          "projecting cached rows with UnsafeProjection instead")
      new ProjectedRows(batches, UnsafeProjection.create(fields))
    } else {
      compiled.generate(ctx.references.toArray).asInstanceOf[Iterator[InternalRow]]
    }
  }

  override protected def createInterpretedObject(
      batches: Iterator[ColumnarBatch]): Iterator[InternalRow] =
    new ProjectedRows(batches, InterpretedUnsafeProjection.createProjection(fields))
}

/**
 * Projects each batch row through `projection`, which reuses one UnsafeRow and owns the values it
 * writes, under the same contract as the generated reader.
 */
private[arrow] class ProjectedRows(
    batches: Iterator[ColumnarBatch],
    private[arrow] val projection: UnsafeProjection)
    extends Iterator[InternalRow] {
  private var batch: ColumnarBatch = _
  private var rowId = 0
  private var numRows = 0

  override def hasNext: Boolean = {
    while (rowId >= numRows && batches.hasNext) {
      batch = batches.next()
      numRows = batch.numRows()
      rowId = 0
    }
    rowId < numRows
  }

  override def next(): InternalRow = {
    if (!hasNext) throw new NoSuchElementException
    val row = projection(batch.getRow(rowId))
    rowId += 1
    row
  }
}

/**
 * The current row of one column of the batch a generated reader is reading. It exists only to be
 * code generated, as an expression rather than through ctx.currentVars, which would stop
 * GenerateUnsafeProjection from splitting the writer and leave every field in next().
 */
private case class VectorValue(
    column: String,
    rowId: String,
    dataType: DataType,
    nullable: Boolean)
    extends LeafExpression {

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException(s"$nodeName is only code generated")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)
    val value = CodeGenerator.getValueFromVector(column, dataType, rowId)
    if (nullable) {
      ev.copy(code = code"""
        boolean ${ev.isNull} = $column.isNullAt($rowId);
        $javaType ${ev.value} = ${ev.isNull} ? ${CodeGenerator.defaultValue(dataType)} : ($value);
      """)
    } else {
      ev.copy(code = code"$javaType ${ev.value} = $value;", isNull = FalseLiteral)
    }
  }
}
