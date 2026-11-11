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

import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, Expression}

import org.apache.comet.ContribServiceFiles

/**
 * Unit coverage for [[CometExpressionContrib]]: how contrib serdes merge with core's, how they
 * are discovered, and how [[QueryPlanSerde.serdeFor]] consults them.
 */
class CometExpressionContribSuite extends AnyFunSuite {

  private val core: Set[Class[_]] = Set(classOf[Add])

  private def contrib(
      serdes: (Class[_ <: Expression], CometExpressionSerde[_])*): CometExpressionContrib =
    new CometExpressionContrib {
      override def expressionSerdes: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
        serdes.toMap
    }

  test("empty when no contribs") {
    assert(CometExpressionContrib.merge(core, Nil).isEmpty)

    // A default build ships no service file, so the production registry is empty too. Raw
    // discovery is checked first because the registry also comes out empty when a listed
    // provider fails to load.
    val discovered =
      ServiceLoader.load(classOf[CometExpressionContrib], getClass.getClassLoader).asScala.toSeq
    assert(discovered.isEmpty, s"unexpected providers: ${discovered.map(_.getClass.getName)}")
    assert(CometExpressionContrib.registered.isEmpty)
  }

  test("contrib serde is used for an unclaimed class") {
    val serde = new StubSerde
    val merged =
      CometExpressionContrib.merge(core, Seq(contrib(classOf[ContribOnlyExpression] -> serde)))
    assert(merged.keySet == Set(classOf[ContribOnlyExpression]))
    assert(merged(classOf[ContribOnlyExpression]) eq serde)
  }

  test("core wins over a contrib") {
    val kept = new StubSerde
    val merged = CometExpressionContrib.merge(
      core,
      Seq(contrib(classOf[Add] -> new StubSerde, classOf[ContribOnlyExpression] -> kept)))
    assert(!merged.contains(classOf[Add]))
    assert(merged.get(classOf[ContribOnlyExpression]).exists(_ eq kept))
  }

  test("two contribs claiming one class both lose") {
    val kept = new StubSerde
    val merged = CometExpressionContrib.merge(
      core,
      Seq(
        contrib(classOf[ContribOnlyExpression] -> new StubSerde),
        contrib(
          classOf[ContribOnlyExpression] -> new StubSerde,
          classOf[OtherContribOnlyExpression] -> kept)))
    assert(!merged.contains(classOf[ContribOnlyExpression]))
    assert(merged.keySet == Set(classOf[OtherContribOnlyExpression]))
    assert(merged(classOf[OtherContribOnlyExpression]) eq kept)
  }

  test("a throwing contrib is skipped and others kept") {
    val kept = new StubSerde
    val merged = CometExpressionContrib.merge(
      core,
      Seq(
        new ThrowingExpressionContrib,
        new VersionSkewedExpressionContrib,
        contrib(classOf[ContribOnlyExpression] -> kept)))
    assert(merged.keySet == Set(classOf[ContribOnlyExpression]))
    assert(merged(classOf[ContribOnlyExpression]) eq kept)
  }

  test("discovery via loadFrom") {
    val impls = Seq(classOf[DiscoverableExpressionContrib].getName)
    ContribServiceFiles.withServiceFile(classOf[CometExpressionContrib], impls) { loader =>
      val discovered = CometExpressionContrib.loadContribs(loader)
      assert(discovered.map(_.getClass) == Seq(classOf[DiscoverableExpressionContrib]))
      val merged = CometExpressionContrib.merge(core, discovered)
      assert(merged.get(classOf[ContribOnlyExpression]).exists(_.isInstanceOf[StubSerde]))
    }
  }

  test("serdeFor falls back to the contrib registry") {
    val contribSerde = new StubSerde
    val coreSerde = QueryPlanSerde.exprSerdeMap(classOf[Add])
    val contribSerdes: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
      Map(classOf[ContribOnlyExpression] -> contribSerde, classOf[Add] -> new StubSerde)

    assert(
      QueryPlanSerde
        .serdeFor(classOf[ContribOnlyExpression], contribSerdes)
        .exists(_ eq contribSerde))
    assert(QueryPlanSerde.serdeFor(classOf[Add], contribSerdes).exists(_ eq coreSerde))
    assert(QueryPlanSerde.serdeFor(classOf[OtherContribOnlyExpression], contribSerdes).isEmpty)
    // The production lookup on a default build sees core serdes only.
    assert(QueryPlanSerde.serdeFor(classOf[Add]).exists(_ eq coreSerde))
    assert(QueryPlanSerde.serdeFor(classOf[ContribOnlyExpression]).isEmpty)
  }
}

/** Expression classes core never registers; only their `Class` objects are used. */
abstract class ContribOnlyExpression extends Expression
abstract class OtherContribOnlyExpression extends Expression

/** Serde stand-in; tests compare instances by identity. */
class StubSerde extends CometExpressionSerde[Expression] {
  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = None
}

/** Public no-arg contrib for ServiceLoader discovery. */
class DiscoverableExpressionContrib extends CometExpressionContrib {
  override def expressionSerdes: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map(classOf[ContribOnlyExpression] -> new StubSerde)
}

class ThrowingExpressionContrib extends CometExpressionContrib {
  override def expressionSerdes: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    throw new IllegalStateException("contrib failed to build its serde map")
}

/** A contrib jar built against Comet internals that have since moved. */
class VersionSkewedExpressionContrib extends CometExpressionContrib {
  override def expressionSerdes: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    throw new NoSuchMethodError("org.apache.comet.serde.Moved.method()V")
}
