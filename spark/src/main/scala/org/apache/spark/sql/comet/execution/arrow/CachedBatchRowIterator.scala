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
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, CodeGeneratorWithInterpretedFallback, InterpretedUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Reads vectors directly into Spark's reusable UnsafeRow buffer. The input iterator owns the
 * batches and releases them on advancement or task completion. As with Spark's cache reader,
 * callers must copy rows they retain across next(), but the returned row owns its variable-width
 * values and remains valid when hasNext() releases the batch that supplied them.
 */
private[arrow] class CachedBatchRowIterator(attributes: Seq[Attribute])
    extends CodeGeneratorWithInterpretedFallback[Iterator[ColumnarBatch], Iterator[InternalRow]] {

  private def fields: Seq[BoundReference] = attributes.zipWithIndex.map { case (attr, i) =>
    BoundReference(i, attr.dataType, attr.nullable)
  }

  override protected def createCodeGeneratedObject(
      batches: Iterator[ColumnarBatch]): Iterator[InternalRow] = {
    val ctx = new CodegenContext
    val columns = attributes.indices.map { i =>
      ctx.addMutableState(classOf[ColumnVector].getName, s"column$i")
    }
    ctx.currentVars = attributes.zip(columns).map { case (attr, column) =>
      val value = JavaCode.variable(ctx.freshName("value"), attr.dataType)
      val getter = CodeGenerator.getValueFromVector(column, attr.dataType, "rowId")
      val javaType = CodeGenerator.javaType(attr.dataType)
      if (attr.nullable) {
        val isNull = JavaCode.isNullVariable(ctx.freshName("isNull"))
        ExprCode(
          code"""
            boolean $isNull = $column.isNullAt(rowId);
            $javaType $value = $isNull ? ${CodeGenerator.defaultValue(attr.dataType)} : ($getter);
          """,
          isNull,
          value)
      } else {
        ExprCode(code"$javaType $value = $getter;", FalseLiteral, value)
      }
    }
    val projection = GenerateUnsafeProjection.createCode(ctx, fields)
    val bindColumns = columns.zipWithIndex
      .map { case (column, i) =>
        s"$column = batch.column($i);"
      }
      .mkString("\n")
    val code = s"""
      public Object generate(Object[] references) {
        return new SpecificCachedBatchRowIterator((scala.collection.Iterator) references[0]);
      }

      class SpecificCachedBatchRowIterator extends scala.collection.AbstractIterator {
        private final scala.collection.Iterator batches;
        private int rowId = 0;
        private int numRows = 0;
        ${ctx.declareMutableStates()}

        public SpecificCachedBatchRowIterator(scala.collection.Iterator batches) {
          this.batches = batches;
          ${ctx.initMutableStates()}
        }

        public boolean hasNext() {
          while (rowId >= numRows && batches.hasNext()) {
            ${classOf[ColumnarBatch].getName} batch =
              (${classOf[ColumnarBatch].getName}) batches.next();
            numRows = batch.numRows();
            rowId = 0;
            $bindColumns
          }
          return rowId < numRows;
        }

        public InternalRow next() {
          if (!hasNext()) throw new java.util.NoSuchElementException();
          ${projection.code}
          rowId++;
          return ${projection.value};
        }

        ${ctx.declareAddedFunctions()}
      }
    """
    val (compiled, _) =
      CodeGenerator.compile(new CodeAndComment(code, ctx.getPlaceHolderToComments()))
    compiled.generate(Array[Any](batches)).asInstanceOf[Iterator[InternalRow]]
  }

  override protected def createInterpretedObject(
      batches: Iterator[ColumnarBatch]): Iterator[InternalRow] = {
    val toUnsafe = InterpretedUnsafeProjection.createProjection(fields)
    batches.flatMap { batch =>
      new Iterator[InternalRow] {
        private var rowId = 0
        override def hasNext: Boolean = rowId < batch.numRows()
        override def next(): InternalRow = {
          if (!hasNext) throw new NoSuchElementException
          val row = toUnsafe(batch.getRow(rowId))
          rowId += 1
          row
        }
      }
    }
  }
}
