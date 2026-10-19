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

package org.apache.comet.rules

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.CometSparkToColumnarExec
import org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer
import org.apache.spark.sql.execution.{RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.internal.StaticSQLConf

import org.apache.comet.CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED
import org.apache.comet.CometSparkSessionExtensions.isCometLoaded

/**
 * Builds a cached adaptive plan that ends in a Spark row operator straight into Arrow.
 *
 * From Spark 4.0, when the cache serializer accepts columnar input, as Comet's does, Spark marks
 * a cached `AdaptiveSparkPlanExec` columnar and AQE plans its final stage for columnar output. A
 * final stage that ends in a row operator then gets a `RowToColumnarExec` on top, which fills
 * Spark's own column vectors, and Comet's serializer copies those into Arrow before it writes
 * them. `CometSparkToColumnarExec` writes Arrow straight from the rows, so the serializer stores
 * its batches as they are, one copy instead of two.
 *
 * That `RowToColumnarExec` is the root of the plan these rules are handed. Spark plans no other
 * stage for columnar output with a row operator at its root: every other query is planned for row
 * output, an exchange stage is rooted at its exchange, and Comet's dynamic pruning subquery is
 * rooted at a broadcast exchange. A transition below the root is left alone.
 *
 * Runs only when Comet's cache is enabled and its serializer is installed, so another serializer
 * gets the batches Spark planned for it, and for schemas `CometSparkToColumnarExec` can convert.
 *
 * @param preview
 *   true in the plan-only preview, which shows the plan Comet would execute. Otherwise the rule
 *   leaves plans alone in plan-only mode, where Spark executes each query unchanged.
 */
case class CometCacheBuildRule(preview: Boolean = false) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case transition @ RowToColumnarExec(child) if appliesTo(transition) =>
      val conversion = CometSparkToColumnarExec(child)
      transition.logicalLink.foreach(conversion.setLogicalLink)
      conversion
    case _ => plan
  }

  private def appliesTo(transition: RowToColumnarExec): Boolean =
    isCometLoaded(conf) &&
      COMET_EXEC_IN_MEMORY_CACHE_ENABLED.get(conf) &&
      (preview || !CometRule.planOnlyApplies(conf, transition)) &&
      conf.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER) ==
      classOf[ArrowCachedBatchSerializer].getName &&
      CometSparkToColumnarExec.isSchemaSupported(transition.schema, ListBuffer.empty[String])
}
