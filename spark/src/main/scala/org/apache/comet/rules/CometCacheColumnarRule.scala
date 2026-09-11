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

import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer
import org.apache.spark.sql.execution.{CodegenSupport, ColumnarToRowExec, ColumnarToRowTransition, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec

/**
 * Lets Spark's generated consumers read cached Arrow vectors without an intermediate UnsafeRow.
 */
object CometCacheColumnarRule extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.wholeStageEnabled) return plan

    plan.transformUp {
      case parent: CodegenSupport
          if parent.supportCodegen && !parent.supportsColumnar &&
            !parent.isInstanceOf[ColumnarToRowTransition] &&
            !WholeStageCodegenExec.isTooManyFields(conf, parent.schema) &&
            !parent.children.exists(p => WholeStageCodegenExec.isTooManyFields(conf, p.schema)) &&
            !parent.expressions.exists(_.exists {
              case _: LeafExpression => false
              case _: CodegenFallback => true
              case _ => false
            }) =>
        // Match the consuming edge rather than every scan: an existing columnar consumer (or a
        // cache stage being materialized by AQE) must keep receiving batches. Spark inserts an
        // InputAdapter around the scan later, while this transition fuses with the row consumer.
        parent.withNewChildren(parent.children.map {
          case child if isColumnarCometCache(child) => ColumnarToRowExec(child)
          case child => child
        })
    }
  }

  private def isColumnarCometCache(plan: SparkPlan): Boolean = {
    plan.supportsColumnar && (plan match {
      case scan: InMemoryTableScanExec =>
        // The materialized format is fixed even when Comet execution is later disabled. The
        // serializer delegates unsupported schemas to Spark, whose cache keeps its own reader.
        scan.relation.cacheBuilder.serializer.isInstanceOf[ArrowCachedBatchSerializer] &&
        ArrowCachedBatchSerializer.supportsSchema(scan.relation.output)
      case stage: QueryStageExec => isColumnarCometCache(stage.plan)
      case _ => false
    })
  }
}
