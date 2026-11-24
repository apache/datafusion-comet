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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import org.apache.comet.CometConf
import org.apache.comet.serde.QueryPlanSerde

/**
 * `canAggregateBeConverted` must reject the same grouping keys as `CometBaseAggregate.doConvert`.
 * End-to-end SQL cannot reach this today: Spark sorts collated grouping keys and Comet declines
 * that sort, so the aggregate never gets a native child. Plan the query with Comet disabled and
 * call the check directly instead. See https://github.com/apache/datafusion-comet/issues/6906.
 */
class CometAggregateCollationSuite extends CometTestBase {

  private def partialAggregate(keyType: StringType): BaseAggregateExec = {
    val schema = StructType(Seq(StructField("k", keyType)))
    val rows = Seq(Row("a"), Row("A"), Row("b")).asJava
    var aggregate: BaseAggregateExec = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      withTempView("collation_agg") {
        spark.createDataFrame(rows, schema).createOrReplaceTempView("collation_agg")
        val plan =
          spark
            .sql("SELECT k, count(*) FROM collation_agg GROUP BY k")
            .queryExecution
            .executedPlan
        aggregate = stripAQEPlan(plan).collectFirst {
          case agg: BaseAggregateExec
              if agg.aggregateExpressions.nonEmpty &&
                agg.aggregateExpressions.forall(_.mode == Partial) =>
            agg
        }.get
      }
    }
    assert(aggregate.groupingExpressions.map(_.dataType) == Seq(keyType), aggregate.toString)
    aggregate
  }

  test("canAggregateBeConverted rejects a non-default collated grouping key (issue #6906)") {
    val aggregate = partialAggregate(StringType("UTF8_LCASE"))
    assert(QueryPlanSerde.isStringCollationType(aggregate.groupingExpressions.head.dataType))
    assert(!CometExecRule(spark).canAggregateBeConverted(aggregate, Partial))
  }

  test("canAggregateBeConverted still accepts a default UTF8_BINARY grouping key") {
    val aggregate = partialAggregate(StringType)
    assert(CometExecRule(spark).canAggregateBeConverted(aggregate, Partial))
  }
}
