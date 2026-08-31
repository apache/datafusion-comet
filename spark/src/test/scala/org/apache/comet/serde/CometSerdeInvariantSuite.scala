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

package org.apache.comet.serde

import scala.collection.mutable.ArrayBuffer

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, Logger => Log4jLogger}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, IsNull, Literal, Unevaluable}
import org.apache.spark.sql.types.{DataType, IntegerType}

import org.apache.comet.CometExplainInfo
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason

/**
 * Expression with no entry in `QueryPlanSerde.exprSerdeMap`, so `exprToProtoInternal` always
 * declines it and tags it with a fallback reason. Used as the unconvertible child in the
 * inherited-failure cases below.
 */
case class TestUnregisteredExpression(child: Expression) extends Expression with Unevaluable {
  override def children: Seq[Expression] = Seq(child)
  override def nullable: Boolean = child.nullable
  override def dataType: DataType = IntegerType
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(child = newChildren.head)
}

/** Stand-in handler; only its class name reaches the warning message. */
private object TestSerde extends CometExpressionSerde[Expression] {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = None
}

/**
 * A serde reporting `Compatible` from `getSupportLevel` and then returning `None` from `convert`
 * breaks an invariant, and costs the user a Spark fallback that the JVM codegen dispatcher would
 * otherwise have absorbed. See https://github.com/apache/datafusion-comet/issues/5574.
 */
class CometSerdeInvariantSuite extends CometTestBase {

  private val loggerName = "org.apache.comet.serde.QueryPlanSerde"

  /** Collect the WARN messages `QueryPlanSerde` emits while running `f`. */
  private def captureWarnings(f: => Unit): Seq[String] = {
    val captured = ArrayBuffer.empty[String]
    val appender = new AbstractAppender("capture", null, null, true, Property.EMPTY_ARRAY) {
      override def append(event: LogEvent): Unit = {
        if (event.getLevel == Level.WARN) {
          captured.synchronized(captured += event.getMessage.getFormattedMessage)
        }
      }
    }
    appender.start()
    val logger = LogManager.getLogger(loggerName).asInstanceOf[Log4jLogger]
    logger.addAppender(appender)
    try {
      f
    } finally {
      logger.removeAppender(appender)
      appender.stop()
    }
    captured.synchronized(captured.toSeq)
  }

  private def invariantWarnings(f: => Unit): Seq[String] =
    captureWarnings(f).filter(_.contains("serde invariant violation"))

  test("warns when a Compatible serde declines in convert") {
    // CometAttributeReference reports Compatible (IntegerType serializes fine) and then declines
    // inside convert, because binding against an empty input list cannot resolve the attribute.
    val attr = AttributeReference("x", IntegerType)()
    val warnings = invariantWarnings {
      assert(QueryPlanSerde.exprToProtoInternal(attr, Seq.empty, binding = true).isEmpty)
    }
    assert(warnings.size === 1, s"expected exactly one warning, got: $warnings")
    assert(warnings.head.contains("CometAttributeReference"))
    assert(warnings.head.contains("cannot resolve"))
  }

  test("stays quiet when convert declines only because a child could not be converted") {
    // IsNull is Compatible and does not tag itself; the decline propagates up from the child,
    // which is not an invariant violation and must not warn once per ancestor.
    val expr = IsNull(IsNull(TestUnregisteredExpression(Literal(1))))
    val warnings = invariantWarnings {
      assert(QueryPlanSerde.exprToProtoInternal(expr, Seq.empty, binding = false).isEmpty)
    }
    assert(warnings.isEmpty, s"expected no warnings, got: $warnings")
  }

  test("stays quiet when the parent tags itself but a child also failed") {
    // Several serdes record their own reason when a child fails (CometCreateArray,
    // CometCreateNamedStruct, CometArraysZip). A tag on the node itself must not defeat the
    // child-failure check, otherwise those serdes warn on every unsupported leaf beneath them.
    val child = TestUnregisteredExpression(Literal(1))
    val parent = IsNull(child)
    withFallbackReason(child, "child is not supported")
    withFallbackReason(parent, "unsupported arguments for parent")
    val warnings = captureWarnings {
      QueryPlanSerde.warnCompatibleButDeclined(parent, TestSerde)
    }
    assert(warnings.isEmpty, s"expected no warnings, got: $warnings")
  }

  test("warns when the node is the only tagged expression in the tree") {
    val expr = IsNull(Literal(1))
    withFallbackReason(expr, "declined for a reason getSupportLevel should have caught")
    val warnings = invariantWarnings {
      QueryPlanSerde.warnCompatibleButDeclined(expr, TestSerde)
    }
    assert(warnings.size === 1, s"expected exactly one warning, got: $warnings")
    assert(warnings.head.contains("TestSerde"))
    assert(warnings.head.contains("declined for a reason getSupportLevel should have caught"))
  }

  test("warns with a placeholder when the serde recorded no reason at all") {
    val expr = IsNull(Literal(1))
    assert(expr.getTagValue(CometExplainInfo.FALLBACK_REASONS).isEmpty)
    val warnings = invariantWarnings {
      QueryPlanSerde.warnCompatibleButDeclined(expr, TestSerde)
    }
    assert(warnings.size === 1, s"expected exactly one warning, got: $warnings")
    assert(warnings.head.contains("no reason recorded"))
  }
}
