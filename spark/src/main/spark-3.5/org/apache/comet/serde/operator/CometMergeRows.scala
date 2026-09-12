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

package org.apache.comet.serde.operator

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.MergeRows
import org.apache.spark.sql.comet.{CometMergeRowsExec, SerializedPlan}
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, MapType, StructType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.ConfigEntry
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.{MergeInstruction, MergeOutputRow, Operator}
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

/**
 * Serde for Spark's `MergeRowsExec` (Spark 3.5+). An instruction is encoded as a condition plus
 * zero, one, or two output rows for Discard, Keep, or Split.
 */
object CometMergeRows extends CometOperatorSerde[MergeRowsExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_MERGE_ROWS_ENABLED)

  override def getSupportLevel(op: MergeRowsExec): SupportLevel = {
    if (!cardinalityCheckSatisfied(op)) {
      Unsupported(Some(cardinalityCheckFallbackReason))
    } else if (!instructionShapesSatisfied(op)) {
      Unsupported(Some(instructionShapeFallbackReason))
    } else {
      Compatible(Some(compatibilityNote))
    }
  }

  override def convert(
      op: MergeRowsExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[Operator] = {
    val input = op.child.output
    val expectedOutputTypes = op.output.map(_.dataType)

    def convertInstruction(instr: MergeRows.Instruction): Option[MergeInstruction] = {
      if (!instructionShapeSatisfied(instr, expectedOutputTypes)) {
        return None
      }

      val condition = exprToProto(instr.condition, input)
      val outputs = instr.outputs.map { row =>
        val exprs = row.map(exprToProto(_, input))
        if (exprs.forall(_.isDefined)) {
          Some(MergeOutputRow.newBuilder().addAllExprs(exprs.map(_.get).asJava).build())
        } else {
          None
        }
      }

      if (condition.isDefined && outputs.forall(_.isDefined)) {
        Some(
          MergeInstruction
            .newBuilder()
            .setCondition(condition.get)
            .addAllOutputs(outputs.map(_.get).asJava)
            .build())
      } else {
        None
      }
    }

    val matched = op.matchedInstructions.map(convertInstruction)
    val notMatched = op.notMatchedInstructions.map(convertInstruction)
    val notMatchedBySource = op.notMatchedBySourceInstructions.map(convertInstruction)

    val isSourcePresent = exprToProto(op.isSourceRowPresent, input)
    val isTargetPresent = exprToProto(op.isTargetRowPresent, input)

    val outputTypes = op.output.map(a => serializeDataType(a.dataType))

    // Only wired when Spark asked for a cardinality check; if `checkCardinality` is false this
    // must stay unset rather than default to 0. `row_id_ordinal` is `optional int32` (not a plain
    // `int32`) precisely because 0 is a legitimate ordinal -- the row-id column could be the
    // child's first column -- so presence must be distinguishable from a real value.
    val rowIdOrd: Option[Int] = if (op.checkCardinality) rowIdOrdinal(op) else None

    // `childOp` is empty when the child did not itself convert to a native operator --
    // `CometExecRule.convertToComet` still calls `convert` in that case. Without this guard a
    // childless `MergeRows` reaches the planner, where a missing child would otherwise be an
    // invalid native plan rather than a clean JVM fallback.
    if (childOp.nonEmpty && matched.forall(_.isDefined) && notMatched.forall(_.isDefined) &&
      notMatchedBySource.forall(_.isDefined) && isSourcePresent.isDefined &&
      isTargetPresent.isDefined && outputTypes.forall(_.isDefined) &&
      cardinalityCheckSatisfied(op) && instructionShapesSatisfied(op)) {
      val mergeBuilder = OperatorOuterClass.MergeRows
        .newBuilder()
        .setIsSourceRowPresent(isSourcePresent.get)
        .setIsTargetRowPresent(isTargetPresent.get)
        .addAllMatchedInstructions(matched.map(_.get).asJava)
        .addAllNotMatchedInstructions(notMatched.map(_.get).asJava)
        .addAllNotMatchedBySourceInstructions(notMatchedBySource.map(_.get).asJava)
        .addAllOutputTypes(outputTypes.map(_.get).asJava)
      rowIdOrd.foreach(mergeBuilder.setRowIdOrdinal)
      Some(builder.setMergeRows(mergeBuilder).build())
    } else if (childOp.isEmpty) {
      withFallbackReason(op, "No child operator")
      None
    } else if (!cardinalityCheckSatisfied(op)) {
      withFallbackReason(op, cardinalityCheckFallbackReason)
      None
    } else if (!instructionShapesSatisfied(op)) {
      withFallbackReason(op, instructionShapeFallbackReason)
      None
    } else {
      withFallbackReason(op, "Unsupported expression in MERGE instructions")
      None
    }
  }

  override def createExec(nativeOp: Operator, op: MergeRowsExec): CometMergeRowsExec = {
    // Carry each instruction group and predicate across as its own field. `MergeRows.Instruction`
    // is an `Expression`, so `Seq[Instruction]` widens to `Seq[Expression]` and Catalyst's
    // expression machinery (subquery registration in particular) still sees every instruction --
    // see `CometMergeRowsExec`'s scaladoc for why the groups must not be flattened together.
    CometMergeRowsExec(
      nativeOp,
      op,
      op.output,
      op.isSourceRowPresent,
      op.isTargetRowPresent,
      op.matchedInstructions.map(i => i: Expression),
      op.notMatchedInstructions.map(i => i: Expression),
      op.notMatchedBySourceInstructions.map(i => i: Expression),
      op.checkCardinality,
      if (op.checkCardinality) rowIdOrdinal(op) else None,
      op.child,
      SerializedPlan(None))
  }

  private val compatibilityNote: String =
    "Native MERGE preserves row values but may differ from Spark in physical output ordering " +
      "and in which error is reported first when multiple rows fail in one input batch"

  private val cardinalityCheckFallbackReason: String =
    s"MERGE cardinality check requires a resolvable, Long-typed '${MergeRows.ROW_ID}' column"

  private val instructionShapeFallbackReason: String =
    "MERGE instruction must be Discard, Keep, or Split with output rows compatible with the plan schema"

  /**
   * True iff cardinality checking is off, or the target row-id column resolves to a usable
   * ordinal. Shared by `getSupportLevel` (the planning-time gate) and `convert` (which must
   * re-derive the same condition to pick its own fallback branch) so the two checks cannot desync
   * -- `convert` is only reached after `getSupportLevel` already passed, so its branch for this
   * case is otherwise unreachable and would silently stop guarding anything if the two drifted
   * apart.
   */
  private def cardinalityCheckSatisfied(op: MergeRowsExec): Boolean =
    !op.checkCardinality || rowIdOrdinal(op).isDefined

  /**
   * Preserve the `MergeRows.Instruction` wire contract at the JVM/native boundary. Spark
   * currently constructs only Discard (0 rows), Keep (1 row), and Split (2 rows), with every
   * projected row matching `MergeRowsExec.output`. Validate that shape explicitly so a future
   * Spark change or a malformed internal plan falls back during planning instead of becoming an
   * invalid native protobuf that the Rust executor would otherwise accept generically.
   */
  private def instructionShapesSatisfied(op: MergeRowsExec): Boolean = {
    val outputTypes = op.output.map(_.dataType)
    op.matchedInstructions.forall(instructionShapeSatisfied(_, outputTypes)) &&
    op.notMatchedInstructions.forall(instructionShapeSatisfied(_, outputTypes)) &&
    op.notMatchedBySourceInstructions.forall(instructionShapeSatisfied(_, outputTypes))
  }

  /**
   * Validates both the Discard/Keep/Split row count and every projected leaf type. Nested
   * nullability may widen from an instruction expression to Spark's declared output schema, but
   * it may not narrow: stamping a nullable nested value as non-nullable would hide a real schema
   * incompatibility.
   */
  private def instructionShapeSatisfied(
      instruction: MergeRows.Instruction,
      outputTypes: Seq[DataType]): Boolean = {
    instruction.outputs.size <= 2 && instruction.outputs.forall { row =>
      row.size == outputTypes.size && row.zip(outputTypes).forall { case (expr, expected) =>
        dataTypeCompatible(expr.dataType, expected)
      }
    }
  }

  /** Spark-compatible type comparison that permits only safe nested-nullability widening. */
  private def dataTypeCompatible(actual: DataType, expected: DataType): Boolean =
    (actual, expected) match {
      case (ArrayType(actualElement, actualNulls), ArrayType(expectedElement, expectedNulls)) =>
        (expectedNulls || !actualNulls) && dataTypeCompatible(actualElement, expectedElement)
      case (
            MapType(actualKey, actualValue, actualNulls),
            MapType(expectedKey, expectedValue, expectedNulls)) =>
        (expectedNulls || !actualNulls) &&
        dataTypeCompatible(actualKey, expectedKey) &&
        dataTypeCompatible(actualValue, expectedValue)
      case (StructType(actualFields), StructType(expectedFields)) =>
        actualFields.length == expectedFields.length &&
        actualFields.zip(expectedFields).forall { case (actualField, expectedField) =>
          actualField.name == expectedField.name &&
          (expectedField.nullable || !actualField.nullable) &&
          dataTypeCompatible(actualField.dataType, expectedField.dataType)
        }
      case _ => actual == expected
    }

  /**
   * Locates the ordinal of Spark's target row-id column (`MergeRows.ROW_ID`) in the child output,
   * using the same SQLConf resolver as Spark's `MergeRowsExec.references`. Requiring `LongType`
   * keeps the native `Int64Array` cardinality validator type-safe; an unexpected schema falls
   * back during planning instead of failing inside native execution.
   */
  private def rowIdOrdinal(op: MergeRowsExec): Option[Int] = {
    val idx = op.child.output.indexWhere { attr =>
      op.conf.resolver(attr.name, MergeRows.ROW_ID) && attr.dataType == LongType
    }
    if (idx >= 0) Some(idx) else None
  }
}
