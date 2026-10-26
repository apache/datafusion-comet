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

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.arrow.vector.{BitVector, ValueVector, VarBinaryVector}
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, BindReferences, Expression, Generator, GenericInternalRow, Literal, Nondeterministic, Unevaluable}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.comet.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.ExecSubqueryExpression
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.CometConf
import org.apache.comet.codegen.CometBatchKernelCodegen
import org.apache.comet.serde.{CometInvokeTargets, ExprOuterClass}
import org.apache.comet.serde.CometScalaUDF.closureSerialize
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}
import org.apache.comet.udf.{CometUDF, NativeUdfCall}
import org.apache.comet.udf.codegen.CometScalaUDFCodegen
import org.apache.comet.vector.CometVector

/**
 * Runs a Spark [[Generator]] for Comet's native generate operator. For each input row it returns
 * the rows the generator emits as one `array<struct>` element, which the native operator unnests
 * and flattens the way it does for `inline`. An input row that emits nothing gets an empty array,
 * so `outer` keeps it as one row of nulls.
 *
 * Arg 0 is the digest of the closure-serialized bound generator, arg 1 the serialized bytes, and
 * arg 2 a boolean scalar that is true for the call the native operator makes once its input is
 * exhausted. That call returns one element holding the rows of `Generator.terminate`. Args 3..N
 * are the columns the bound generator reads, in ordinal order.
 *
 * A generator can keep state across rows and emit rows from `terminate`, as a Hive UDTF does, so
 * every native plan gets its own instance: the key is the digest and the plan, and `releasePlan`
 * drops the plan's instances. The serde ships the bound generator rather than evaluating it on
 * the driver, so this works in cluster mode without driver-side state. `evaluate` is synchronized
 * for the reason `CometScalaUDFCodegen.evaluate` is.
 */
class CometGeneratorUDF extends CometUDF {
  import CometGeneratorUDF._

  private val generators = mutable.HashMap.empty[(ByteBuffer, Long), BoundGenerator]

  override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector =
    evaluate(inputs, numRows, partitionIndex = 0, planId = NoPlan)

  override def evaluate(
      inputs: Array[ValueVector],
      numRows: Int,
      partitionIndex: Int,
      planId: Long): ValueVector = this.synchronized {
    require(
      inputs.length >= 3,
      "CometGeneratorUDF requires the generator digest, its serialized bytes and the terminate " +
        s"flag, got ${inputs.length} inputs")
    val digest = ByteBuffer.wrap(binaryScalar(inputs(0)))
    val bound = generators.getOrElseUpdate(
      (digest, planId),
      BoundGenerator(deserialize(binaryScalar(inputs(1))), partitionIndex))

    val output = CometBatchKernelCodegen.allocateOutput(bound.outputField, numRows, 0)
    try {
      val writer = ArrowWriter.createFieldWriter(output)
      val outputRow = new GenericInternalRow(1)
      def writeRows(rows: Any): Unit = {
        val copies = new ArrayBuffer[Any]
        iteratorOf(rows).foreach(row => copies += row.copy())
        outputRow.update(0, new GenericArrayData(copies.toArray))
        writer.write(outputRow, 0)
      }

      if (inputs(2).asInstanceOf[BitVector].get(0) == 1) {
        writeRows(bound.generator.terminate())
      } else {
        val columns: Array[ColumnVector] =
          inputs.drop(3).map(v => CometVector.getVector(v, null): ColumnVector)
        val batch = new ColumnarBatch(columns, numRows)
        var i = 0
        while (i < numRows) {
          writeRows(bound.generator.eval(batch.getRow(i)))
          i += 1
        }
      }
      writer.finish()
      output
    } catch {
      case t: Throwable =>
        output.close()
        throw t
    }
  }

  override def releasePlan(planId: Long): Unit = this.synchronized {
    generators.keys.filter(_._2 == planId).toList.foreach(generators.remove)
  }

  private def deserialize(bytes: Array[Byte]): Generator = {
    val loader = Option(Thread.currentThread().getContextClassLoader)
      .getOrElse(classOf[Generator].getClassLoader)
    SparkEnv.get.closureSerializer
      .newInstance()
      .deserialize[Generator](ByteBuffer.wrap(bytes), loader)
  }
}

object CometGeneratorUDF {

  /** The plan id of calls from outside a native plan, such as unit tests. */
  private val NoPlan = -1L

  /** Numbers each generator the serde ships; see [[calls]]. */
  private val nextOccurrence = new AtomicLong()

  /** Why `generator` cannot run through [[CometGeneratorUDF]], or `None` when it can. */
  def declineReason(generator: Generator): Option[String] = {
    val name = generator.nodeName
    if (!CometConf.COMET_EXEC_JVM_GENERATOR_ENABLED.get()) {
      return Some(
        s"Unsupported generator: $name. Set ${CometConf.COMET_EXEC_JVM_GENERATOR_ENABLED.key}" +
          "=true to run it on the JVM inside Comet")
    }
    // The generator is serialized at plan time, before any subquery in it has run, and an
    // unevaluable or native-only node has nothing to run on the JVM. Attributes are unevaluable
    // only until they are bound, which `calls` does.
    generator
      .find {
        case _: Attribute => false
        case _: ExecSubqueryExpression | _: Unevaluable | _: NativeUdfCall => true
        case _ => false
      }
      .map(node => s"$name: ${node.nodeName} cannot run inside a JVM generator")
      .orElse(CometInvokeTargets.declineReason(generator))
      .orElse(Some(s"$name: unsupported output type ${outputType(generator).catalogString}")
        .filter(_ => serializeDataType(outputType(generator)).isEmpty))
  }

  /**
   * The `JvmScalarUdf` calls that run `generator` through [[CometGeneratorUDF]]: one evaluated
   * for each input batch, and the one the native operator makes once its input is exhausted.
   * Fails with the reason when `generator` cannot be shipped.
   */
  def calls(
      generator: Generator,
      inputs: Seq[Attribute]): Either[String, (ExprOuterClass.Expr, ExprOuterClass.Expr)] = {
    val name = generator.nodeName
    // Bind against only the attributes the generator reads, which become the data args.
    val attrs = generator.collect { case a: AttributeReference => a }.distinct
    val bound = BindReferences.bindReference(generator, AttributeSeq(attrs))
    val bytes =
      try {
        closureSerialize(bound)
      } catch {
        case NonFatal(e) =>
          return Left(
            s"$name could not be closure-serialized (${e.getClass.getSimpleName}: " +
              s"${e.getMessage})")
      }
    // The digest also covers a number unique to this generator, so two identical generators in
    // one plan get their own instances and state.
    val occurrence = ByteBuffer.allocate(8).putLong(nextOccurrence.getAndIncrement()).array()
    val digest = CometScalaUDFCodegen.digest(bytes ++ occurrence)
    val returnType = serializeDataType(outputType(generator)).getOrElse {
      return Left(s"$name: unsupported output type ${outputType(generator).catalogString}")
    }

    def call(
        terminate: Boolean,
        dataArgs: Seq[Expression]): Either[String, ExprOuterClass.Expr] = {
      val args = (Seq(Literal(digest), Literal(bytes), Literal(terminate)) ++ dataArgs)
        .map(exprToProto(_, inputs))
      if (args.exists(_.isEmpty)) {
        Left(s"$name: an argument could not be passed to the JVM generator")
      } else {
        val udf = ExprOuterClass.JvmScalarUdf
          .newBuilder()
          .setClassName(classOf[CometGeneratorUDF].getName)
          .addAllArgs(args.flatten.asJava)
          .setReturnType(returnType)
          .setReturnNullable(false)
        Right(ExprOuterClass.Expr.newBuilder().setJvmScalarUdf(udf).build())
      }
    }
    for {
      process <- call(terminate = false, attrs)
      terminate <- call(terminate = true, Nil)
    } yield (process, terminate)
  }

  /**
   * A generator ready to run in one native plan, and the Arrow field its output is exported as.
   */
  private case class BoundGenerator(generator: Generator, outputField: Field)

  private object BoundGenerator {
    def apply(generator: Generator, partitionIndex: Int): BoundGenerator = {
      generator.foreach {
        case n: Nondeterministic => n.initialize(partitionIndex)
        case _ =>
      }
      BoundGenerator(generator, outputField(generator))
    }
  }

  /** The type of each element of the output: the rows one input row generates. */
  def outputType(generator: Generator): ArrayType =
    ArrayType(generator.elementSchema, containsNull = false)

  private def outputField(generator: Generator): Field =
    CometBatchKernelCodegen.toFfiArrowField("generated", outputType(generator), nullable = false)

  private def binaryScalar(v: ValueVector): Array[Byte] = {
    val vec = v.asInstanceOf[VarBinaryVector]
    require(
      vec.getValueCount >= 1 && !vec.isNull(0),
      "CometGeneratorUDF requires a binary scalar")
    vec.get(0)
  }

  /** `Generator.eval` returns a `TraversableOnce` on Scala 2.12 and an `IterableOnce` on 2.13. */
  private def iteratorOf(rows: Any): Iterator[InternalRow] = rows match {
    case it: Iterator[_] => it.asInstanceOf[Iterator[InternalRow]]
    case it: Iterable[_] => it.iterator.asInstanceOf[Iterator[InternalRow]]
    case opt: Option[_] => opt.iterator.asInstanceOf[Iterator[InternalRow]]
    case other =>
      throw new IllegalStateException(s"Unexpected generator output ${other.getClass.getName}")
  }
}
