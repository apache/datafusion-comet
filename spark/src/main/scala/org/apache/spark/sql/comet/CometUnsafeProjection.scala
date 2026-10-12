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

import java.util.{LinkedHashMap => JLinkedHashMap, Map => JMap}
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, CodeGeneratorWithInterpretedFallback, InterpretedUnsafeProjection, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext, CodeGenerator, GeneratedClass, GenerateUnsafeProjection}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

/**
 * Creates the projection that copies each row of a batch into an UnsafeRow, as
 * `UnsafeProjection.create(output, output)` does, but generates and compiles its class only once
 * per executor for each column layout.
 *
 * `UnsafeProjection.create` generates the projection's Java source on every call, and only the
 * compiled class is cached. `CometColumnarToRowExec` calls it once per partition whenever it runs
 * outside whole-stage codegen, which Spark skips for a schema with more fields than
 * `spark.sql.codegen.maxFields`, nested fields included. Spark's own scans fall back to rows for
 * such schemas, so its `ColumnarToRowExec` rarely meets one, but Comet's operators always produce
 * batches. The source grows with the number of nested fields, and each level of nesting repeats
 * the work of splitting its children into methods: for 100 columns nested three levels deep,
 * generating it took about 19 ms in every partition.
 *
 * Every call returns a new instance of the shared class, so callers own their projection as they
 * do one from `UnsafeProjection.create`.
 */
private[comet] object CometUnsafeProjection
    extends CodeGeneratorWithInterpretedFallback[Seq[BoundReference], UnsafeProjection] {

  /**
   * What the generated source depends on: the type and nullability of each column, and the method
   * size at which `CodegenContext.splitExpressions` splits the field writes.
   */
  private case class Layout(columns: Seq[(DataType, Boolean)], methodSplitThreshold: Int)

  /** The same bound as Spark's compiled class cache, `spark.sql.codegen.cache.maxEntries`. */
  private[comet] val MaxCachedClasses = 100

  /** Least recently used first. Guarded by `classes.synchronized`. */
  private val classes = new JLinkedHashMap[Layout, GeneratedClass](16, 0.75f, true) {
    override def removeEldestEntry(eldest: JMap.Entry[Layout, GeneratedClass]): Boolean =
      size() > MaxCachedClasses
  }

  private val classesGenerated = new AtomicLong(0)

  /** How many projection classes this executor has generated, for tests. */
  private[comet] def generatedClassCount: Long = classesGenerated.get()

  /** A projection of rows with the columns of `output` to UnsafeRows. */
  def create(output: Seq[Attribute]): UnsafeProjection =
    createObject(output.zipWithIndex.map { case (attr, ordinal) =>
      BoundReference(ordinal, attr.dataType, attr.nullable)
    })

  override protected def createCodeGeneratedObject(
      columns: Seq[BoundReference]): UnsafeProjection = {
    val layout =
      Layout(columns.map(c => (c.dataType, c.nullable)), SQLConf.get.methodSplitThreshold)
    val cached = classes.synchronized(classes.get(layout))
    if (cached != null) {
      cached.generate(Array.empty[Any]).asInstanceOf[UnsafeProjection]
    } else {
      // Generated outside the lock: tasks that miss on the same layout at once each generate
      // it, as every task does with UnsafeProjection.create.
      val (generated, references) = generate(columns)
      // A stored class is instantiated without references, so store only one that needs none.
      // GenerateUnsafeProjection references no objects for bound columns.
      if (references.isEmpty) {
        val _ = classes.synchronized(classes.putIfAbsent(layout, generated))
      }
      generated.generate(references).asInstanceOf[UnsafeProjection]
    }
  }

  override protected def createInterpretedObject(columns: Seq[BoundReference]): UnsafeProjection =
    InterpretedUnsafeProjection.createProjection(columns)

  /**
   * Generates and compiles the class that `GenerateUnsafeProjection.create` does, from the same
   * template, which is repeated here because that method returns only an instance. Returns the
   * class with the objects its instances reference.
   */
  private def generate(columns: Seq[BoundReference]): (GeneratedClass, Array[Any]) = {
    val ctx = new CodegenContext
    val eval = GenerateUnsafeProjection.createCode(ctx, columns)
    val body =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new SpecificUnsafeProjection(references);
         |}
         |
         |class SpecificUnsafeProjection extends ${classOf[UnsafeProjection].getName} {
         |
         |  private Object[] references;
         |  ${ctx.declareMutableStates()}
         |
         |  public SpecificUnsafeProjection(Object[] references) {
         |    this.references = references;
         |    ${ctx.initMutableStates()}
         |  }
         |
         |  public void initialize(int partitionIndex) {
         |    ${ctx.initPartition()}
         |  }
         |
         |  // Scala.Function1 need this
         |  public java.lang.Object apply(java.lang.Object row) {
         |    return apply((InternalRow) row);
         |  }
         |
         |  public UnsafeRow apply(InternalRow ${ctx.INPUT_ROW}) {
         |    ${eval.code}
         |    return ${eval.value};
         |  }
         |
         |  ${ctx.declareAddedFunctions()}
         |}
       """.stripMargin
    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(body, ctx.getPlaceHolderToComments()))
    val (generatedClass, _) = CodeGenerator.compile(code)
    classesGenerated.incrementAndGet()
    (generatedClass, ctx.references.toArray)
  }
}
