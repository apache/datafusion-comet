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

package org.apache.comet

import scala.collection.mutable

import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.CometNativeExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

import org.apache.comet.serde.{ExprOuterClass, OperatorOuterClass}

class QueryContextInternerSuite extends CometTestBase {

  private val sqlText =
    """select k1,
      |       sum(d1) as sum_d1,
      |       max(case when i1 > 100 then d1 + 1 else d1 * 2 end) as m1
      |from t1
      |where i1 between 1 and 1000 and d1 > 1.0
      |group by k1
      |order by sum_d1 desc""".stripMargin

  /** Every `Expr`/`AggExpr` QueryContext in an operator tree, at any nesting depth. */
  private def collectContexts(
      root: OperatorOuterClass.Operator): Seq[ExprOuterClass.QueryContext] = {
    val found = mutable.ArrayBuffer.empty[ExprOuterClass.QueryContext]

    def walk(m: com.google.protobuf.Message): Unit = {
      m match {
        case e: ExprOuterClass.Expr if e.hasQueryContext => found += e.getQueryContext
        case a: ExprOuterClass.AggExpr if a.hasQueryContext => found += a.getQueryContext
        case _ =>
      }
      m.getAllFields.forEach { (_, v) =>
        v match {
          case child: com.google.protobuf.Message => walk(child)
          case list: java.util.List[_] =>
            list.forEach {
              case child: com.google.protobuf.Message => walk(child)
              case _ =>
            }
          case _ =>
        }
      }
    }

    walk(root)
    found.toSeq
  }

  private def nativeBlocks(df: DataFrame): Seq[Array[Byte]] = {
    val plan = df.queryExecution.executedPlan match {
      case a: AdaptiveSparkPlanExec => a.executedPlan
      case p => p
    }
    plan.collect {
      case n: CometNativeExec if n.serializedPlanOpt.isDefined => n.serializedPlanOpt.plan.get
    }
  }

  private def withTestTable(f: => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      withTempPath { dir =>
        spark
          .range(0, 100)
          .selectExpr(
            "cast(id % 17 as int) as k1",
            "cast(id as decimal(20,4)) as d1",
            "cast(id % 1000 as int) as i1")
          .write
          .mode("overwrite")
          .parquet(dir.getAbsolutePath)
        withTempView("t1") {
          spark.read.parquet(dir.getAbsolutePath).createOrReplaceTempView("t1")
          f
        }
      }
    }
  }

  test("SQL text is interned into the root operator pool") {
    withTestTable {
      val blocks = nativeBlocks(spark.sql(sqlText))
      assert(blocks.nonEmpty, "expected at least one serialized native block")

      var sawContext = false
      blocks.foreach { bytes =>
        val root = OperatorOuterClass.Operator.parseFrom(bytes)
        val contexts = collectContexts(root)
        if (contexts.nonEmpty) {
          sawContext = true
          contexts.foreach { ctx =>
            assert(
              ctx.hasSqlTextIdx,
              s"QueryContext was not interned; carries inline sql_text: ${ctx.getSqlText}")
            assert(ctx.getSqlText.isEmpty, "interned QueryContext should not repeat sql_text")
            assert(ctx.getSqlTextIdx >= 0 && ctx.getSqlTextIdx < root.getSqlTextPoolCount)
          }
          // The query text is stored once for the block, not once per expression.
          assert(root.getSqlTextPoolCount == 1, s"pool: ${root.getSqlTextPoolList}")
          assert(root.getSqlTextPool(0).contains("sum(d1)"))
        }
      }
      assert(sawContext, "expected at least one expression to carry a QueryContext")
    }
  }

  test("interning shrinks the serialized plan") {
    withTestTable {
      val blocks = nativeBlocks(spark.sql(sqlText))
      val interned = blocks.map(_.length).sum

      // What the same plan would serialize to with a copy of the text on every context, which is
      // what was emitted before interning: the UTF-8 text plus its tag/length prefix per context.
      val expanded = blocks.map { bytes =>
        val root = OperatorOuterClass.Operator.parseFrom(bytes)
        val pool = root.getSqlTextPoolList
        bytes.length + collectContexts(root).map { ctx =>
          pool.get(ctx.getSqlTextIdx).getBytes("UTF-8").length + 3
        }.sum
      }.sum

      assert(
        expanded > interned * 4,
        s"expected interning to shrink the plan substantially: $expanded -> $interned")
    }
  }

  test("ANSI error still reports full SQL context after interning") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      "spark.sql.ansi.enabled" -> "true") {
      withTempPath { dir =>
        spark
          .range(1, 10)
          .selectExpr("cast(id as int) as a", "cast(0 as int) as b")
          .write
          .mode("overwrite")
          .parquet(dir.getAbsolutePath)
        withTempView("t_div") {
          spark.read.parquet(dir.getAbsolutePath).createOrReplaceTempView("t_div")

          val query = "select a / b as ratio from t_div"
          val err = intercept[Exception](spark.sql(query).collect())
          val msg = Option(err.getMessage).getOrElse("")
          assert(msg.contains("DIVIDE_BY_ZERO"), msg)
          // The pooled text must round-trip through native: the summary echoes the full query.
          assert(msg.contains(query), s"error message lost the SQL context:\n$msg")
        }
      }
    }
  }
}
