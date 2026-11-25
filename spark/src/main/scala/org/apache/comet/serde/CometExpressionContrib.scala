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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression

import org.apache.comet.ContribServices

/**
 * Hook that lets an optional, out-of-tree contrib register serdes for expression classes core
 * does not know about. Core names no contrib: implementations are discovered at runtime via the
 * JDK [[java.util.ServiceLoader]], so default builds (which ship no service file) see an empty
 * registry.
 *
 * A contrib ships an implementation of this trait and a
 * `META-INF/services/org.apache.comet.serde.CometExpressionContrib` resource naming it.
 *
 * Core serdes always win: [[QueryPlanSerde.serdeFor]] consults core first, and a contrib entry
 * for a class core registers is ignored. A class that two contribs both claim is dropped from
 * both, because ServiceLoader order is not specified and so neither can be preferred.
 *
 * `expressionSerdes` runs while the registry is being built, so it must not call
 * [[QueryPlanSerde.serdeFor]] or otherwise convert expressions.
 */
trait CometExpressionContrib {

  /** Serdes this contrib provides, keyed by the expression class each one converts. */
  def expressionSerdes: Map[Class[_ <: Expression], CometExpressionSerde[_]]
}

object CometExpressionContrib extends Logging {

  /**
   * Contrib serdes for classes core does not register, discovered through the thread context
   * ClassLoader (see `ContribServices.load`). Empty on a default build.
   */
  private[comet] lazy val registered: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    merge(
      QueryPlanSerde.exprSerdeMap.keySet.toSet[Class[_]],
      ContribServices.load(classOf[CometExpressionContrib], getClass.getClassLoader))

  /** Discover implementations visible to `loader`, without touching `registered`. */
  private[comet] def loadContribs(loader: ClassLoader): Seq[CometExpressionContrib] =
    ContribServices.loadFrom(classOf[CometExpressionContrib], loader)

  /**
   * Combine the serdes of `contribs`, dropping classes in `core` and classes more than one
   * contrib claims. A contrib whose `expressionSerdes` throws is skipped with a warning, so one
   * broken contrib jar neither fails planning nor hides the serdes of the others. As in
   * `CometScanContrib`, a `LinkageError` (a contrib built against other Comet internals) is
   * contained the same way, while fatal errors propagate.
   */
  private[comet] def merge(core: Set[Class[_]], contribs: Seq[CometExpressionContrib])
      : Map[Class[_ <: Expression], CometExpressionSerde[_]] = {
    val claims = contribs.flatMap { contrib =>
      serdesOf(contrib).map { case (cls, serde) => Claim(cls, contrib.getClass.getName, serde) }
    }
    val kept = claims.groupBy(_.cls).values.toList.flatMap { entries =>
      val cls = entries.head.cls
      val owners = entries.map(_.owner).mkString(", ")
      if (core.contains(cls)) {
        logWarning(
          s"Ignoring the ${cls.getName} serde from $owners: Comet already has a serde for " +
            "this expression.")
        None
      } else if (entries.size > 1) {
        logWarning(
          s"Ignoring every serde for ${cls.getName}: more than one contrib registers one " +
            s"($owners), and their discovery order is not specified.")
        None
      } else {
        entries.headOption
      }
    }
    kept.map(claim => claim.cls -> claim.serde).toMap
  }

  private case class Claim(
      cls: Class[_ <: Expression],
      owner: String,
      serde: CometExpressionSerde[_])

  private def serdesOf(
      contrib: CometExpressionContrib): Seq[(Class[_ <: Expression], CometExpressionSerde[_])] =
    try contrib.expressionSerdes.toList
    catch {
      case NonFatal(e) =>
        logWarning(
          s"Skipping contrib ${contrib.getClass.getName}: its expression serdes failed",
          e)
        Nil
      case e: LinkageError =>
        logWarning(
          s"Skipping contrib ${contrib.getClass.getName}: its expression serdes failed with " +
            s"${e.getClass.getName}, so it was likely built against a different Comet version",
          e)
        Nil
    }
}
