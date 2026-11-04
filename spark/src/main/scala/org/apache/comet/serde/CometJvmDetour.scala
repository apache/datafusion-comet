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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression}

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{withFallbackReason, withInfo}
import org.apache.comet.serde.ExprOuterClass.Expr

/**
 * The JVM expression detour (partial project fallback), gated by
 * [[CometConf.COMET_EXEC_JVM_DETOUR_ENABLED]].
 *
 * When an item of a projection list, or a filter condition, has no Comet translation, the whole
 * item is sent to the codegen dispatcher (Spark's own generated code, run over the Arrow batch)
 * instead of falling the operator back to Spark.
 *
 * The detour applies only at the root of an item: the child of a projection item's `Alias` (or
 * the item itself), or the whole filter condition. It never replaces a node below that root. A
 * node that declines conversion can be protecting its ancestors rather than itself: an ancestor's
 * Rust implementation can be Spark-compatible only for the inputs its Rust-implemented children
 * produce, and nothing in the serde API records that dependency (see the discussion on
 * apache/datafusion-comet#4827 and #5574). Detouring a declined node while its ancestors convert
 * to Rust would remove that protection without anything noticing. At the root there are no
 * ancestors in the expression: the projection only outputs the value, and the filter only keeps
 * the rows where it is true. Every node of the tree, conditionals included, runs in Spark's code,
 * so `CASE`, `IF`, `AND`, `OR` and `COALESCE` keep Spark's per-row short-circuit evaluation.
 *
 * The detour is not attempted when the tree contains an expression the user disabled with
 * `spark.comet.expression.<Name>.enabled=false`, because that switch is documented to make the
 * expression fall back to Spark.
 */
object CometJvmDetour {

  /**
   * Convert one root expression: a projection list item or a filter condition. Tries the usual
   * conversion first and, if that fails, the detour.
   */
  def exprToProtoOrDetour(
      root: Expression,
      inputs: Seq[Attribute],
      binding: Boolean = true): Option[Expr] =
    CometExplainInfo.withExpressionFallbackReasonScope {
      QueryPlanSerde
        .exprToProto(root, inputs, binding)
        .orElse(detour(root, inputs, binding))
    }

  /**
   * Run `root` through the codegen dispatcher, after its conversion failed. Returns `None` when
   * the detour is off, when `root` holds a disabled expression, or when the dispatcher cannot
   * take the tree; the reasons the conversion recorded are kept in that case, so the operator
   * falls back to Spark as it would without the detour.
   */
  private[comet] def detour(
      root: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    if (!CometConf.COMET_EXEC_JVM_DETOUR_ENABLED.get()) {
      return None
    }
    // The alias only names the output, so the dispatcher takes its child. The proto of a
    // projection item carries no name: `CometAlias` converts to its child's proto too.
    val target = root match {
      case a: Alias => a.child
      case other => other
    }
    disabledExpression(target) match {
      case Some(name) =>
        withFallbackReason(
          root,
          s"JVM detour not attempted: the expression contains $name, which is disabled by " +
            s"${CometConf.getExprEnabledConfigKey(name)}=false")
        None
      case None =>
        CometScalaUDF.emitJvmCodegenDispatch(target, inputs, binding).map { proto =>
          clearConversionTags(root, target)
          withInfo(
            target,
            "Expression evaluated in the JVM by the codegen dispatcher (JVM detour)")
          QueryPlanSerde.attachExprIdAndContext(target, proto)
        }
    }
  }

  /**
   * The config name of the first expression in `tree` that the user disabled with
   * `spark.comet.expression.<Name>.enabled=false`. An expression with a serde uses the serde's
   * config name, which is what its conversion checks. An expression without one uses its class
   * name, which is also the name the conversion of a serde would use by default, so a user can
   * keep any expression out of the detour the same way.
   */
  private[comet] def disabledExpression(tree: Expression): Option[String] =
    tree.collectFirst(Function.unlift { (e: Expression) =>
      val name = QueryPlanSerde.exprSerdeMap.get(e.getClass) match {
        case Some(serde) =>
          serde.asInstanceOf[CometExpressionSerde[Expression]].getExprConfigName(e)
        case None => e.getClass.getSimpleName
      }
      if (CometConf.isExprEnabled(name)) None else Some(name)
    })

  /**
   * Clear what the failed conversion recorded in the tree under `root`, now that the whole tree
   * runs in the dispatcher. Its fallback reasons no longer apply, and an operator that falls back
   * for another reason, such as another projection item, must not report them. Its coverage tags
   * would count a node that converted to Rust before an ancestor failed as Rust-implemented,
   * although it now runs in the kernel. `emitJvmCodegenDispatch` has already tagged `target` with
   * the names of everything in the kernel, so those tags on `target` are kept.
   */
  private def clearConversionTags(root: Expression, target: Expression): Unit =
    root.foreach { e =>
      CometExplainInfo.clearFallbackReasons(e)
      e.unsetTagValue(CometExplainInfo.NATIVE_EXPRS)
      if (!(e eq target)) {
        e.unsetTagValue(CometExplainInfo.CODEGEN_DISPATCH_EXPRS)
        e.unsetTagValue(CometExplainInfo.DISPATCHED_SELF)
      }
    }
}
