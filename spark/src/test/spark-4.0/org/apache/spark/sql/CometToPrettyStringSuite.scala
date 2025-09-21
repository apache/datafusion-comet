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

package org.apache.spark.sql

import org.apache.comet.CometFuzzTestBase
import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.Compatible
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, ToPrettyString}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.BinaryOutputStyle
import org.apache.spark.sql.types.DataTypes

class CometToPrettyStringSuite extends CometFuzzTestBase {

  test("ToPrettyString") {
    val style = List(
      BinaryOutputStyle.UTF8,
      BinaryOutputStyle.BASIC,
      BinaryOutputStyle.BASE64,
      BinaryOutputStyle.HEX,
      BinaryOutputStyle.HEX_DISCRETE
    )
    style.foreach(s =>
      withSQLConf(SQLConf.BINARY_OUTPUT_STYLE.key -> s.toString) {
        val df = spark.read.parquet(filename)
        df.createOrReplaceTempView("t1")
        val table = spark.sessionState.catalog.lookupRelation(TableIdentifier("t1"))

        for (field <- df.schema.fields) {
          val col = field.name
          val prettyExpr = Alias(ToPrettyString(UnresolvedAttribute(col)), s"pretty_$col")()
          val plan = Project(Seq(prettyExpr), table)
          val analyzed = spark.sessionState.analyzer.execute(plan)
          val result: DataFrame = Dataset.ofRows(spark, analyzed)
          CometCast.isSupported(field.dataType, DataTypes.StringType, Some(spark.sessionState.conf.sessionLocalTimeZone), CometEvalMode.TRY) match {
            case _: Compatible => checkSparkAnswerAndOperator(result)
            case _ => checkSparkAnswer(result)
          }
        }
      }
    )
  }
}
